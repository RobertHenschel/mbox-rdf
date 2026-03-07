#!/usr/bin/env python3
"""IMAP IDLE sync daemon for mbox-rdf.

Monitors IMAP folders via IDLE, converts new messages to RDF using the
mbox-rdf binary, and inserts them into QLever via SPARQL UPDATE.
Performs full UID-diff sync: new messages are inserted, deleted messages
are removed from the RDF store.
"""

import configparser
import hashlib
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

try:
    from imapclient import IMAPClient
except ImportError:
    print("ERROR: imapclient not installed. Run: pip install imapclient", file=sys.stderr)
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests", file=sys.stderr)
    sys.exit(1)


shutdown_event = threading.Event()


class EventBus:
    """Unix domain socket server that broadcasts JSON events to connected clients.

    Events are newline-delimited JSON. Each line is a complete JSON object.
    Clients connect, receive events as they happen, and can disconnect at any time.
    """

    def __init__(self, socket_path):
        self._socket_path = socket_path
        self._clients = []
        self._clients_lock = threading.Lock()
        self._server = None
        self._thread = None

    def start(self):
        if os.path.exists(self._socket_path):
            os.unlink(self._socket_path)
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(self._socket_path)
        self._server.listen(16)
        self._server.settimeout(1.0)
        self._thread = threading.Thread(target=self._accept_loop, name="ipc-server", daemon=True)
        self._thread.start()
        log("ipc", "INFO", f"Event socket listening on {self._socket_path}")

    def _accept_loop(self):
        while not shutdown_event.is_set():
            try:
                conn, _ = self._server.accept()
                conn.settimeout(5.0)
                with self._clients_lock:
                    self._clients.append(conn)
                log("ipc", "INFO", f"Client connected ({len(self._clients)} active)")
            except socket.timeout:
                continue
            except OSError:
                break

    def broadcast(self, event: dict):
        """Send an event dict to all connected clients as a JSON line."""
        line = json.dumps(event, separators=(",", ":")) + "\n"
        data = line.encode("utf-8")
        dead = []
        with self._clients_lock:
            for conn in self._clients:
                try:
                    conn.sendall(data)
                except (BrokenPipeError, OSError):
                    dead.append(conn)
            for conn in dead:
                self._clients.remove(conn)
                try:
                    conn.close()
                except OSError:
                    pass
            if dead:
                log("ipc", "INFO", f"Removed {len(dead)} disconnected client(s) ({len(self._clients)} active)")

    def stop(self):
        if self._server:
            self._server.close()
        with self._clients_lock:
            for conn in self._clients:
                try:
                    conn.close()
                except OSError:
                    pass
            self._clients.clear()
        if os.path.exists(self._socket_path):
            os.unlink(self._socket_path)
        log("ipc", "INFO", "Event socket closed")


_event_bus: EventBus | None = None


def log(folder, level, msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] [{folder}] {msg}", flush=True)


def extract_message_id(raw_bytes):
    """Extract Message-ID from raw RFC 822 bytes without full parsing."""
    for line in raw_bytes.split(b"\n"):
        line = line.strip()
        if line.lower().startswith(b"message-id:"):
            mid = line.split(b":", 1)[1].strip()
            return mid.decode("utf-8", errors="replace").strip("<>")
        if line == b"" and not line.startswith(b" "):
            break
    return None


def check_duplicate(endpoint, access_token, graph_iri, message_id):
    """ASK QLever whether this message-id already exists in the graph."""
    query = (
        f'ASK {{ GRAPH <{graph_iri}> {{ '
        f'?msg <https://mail.described.at/messageId> "{escape_sparql(message_id)}" '
        f'}} }}'
    )
    return _ask_query(endpoint, access_token, query)


def check_duplicate_by_headers(endpoint, access_token, graph_iri, raw_bytes):
    """Fallback dedup for messages without a Message-ID: match on date + sender + subject."""
    date_val = _extract_header(raw_bytes, b"date")
    from_val = _extract_header(raw_bytes, b"from")
    subject_val = _extract_header(raw_bytes, b"subject")
    if not date_val or not from_val:
        return False
    clauses = [
        f'?msg <http://schema.org/dateCreated> ?d . FILTER(STR(?d) = "{escape_sparql(date_val)}")',
        f'?msg <https://mail.described.at/from> ?sender . ?sender <http://schema.org/email> ?email . FILTER(CONTAINS(LCASE("{escape_sparql(from_val)}"), LCASE(?email)))',
    ]
    if subject_val:
        clauses.append(
            f'?msg <http://schema.org/name> "{escape_sparql(subject_val)}"'
        )
    body = " .\n".join(clauses)
    query = f'ASK {{ GRAPH <{graph_iri}> {{ {body} }} }}'
    return _ask_query(endpoint, access_token, query)


def _extract_header(raw_bytes, header_name):
    """Extract a single header value from raw RFC 822 bytes."""
    prefix = header_name + b":"
    for line in raw_bytes.split(b"\n"):
        stripped = line.strip()
        if stripped.lower().startswith(prefix):
            return stripped.split(b":", 1)[1].strip().decode("utf-8", errors="replace")
        if stripped == b"":
            break
    return None


def _ask_query(endpoint, access_token, query):
    try:
        resp = requests.get(
            endpoint,
            params={"query": query, "access-token": access_token},
            headers={"Accept": "application/sparql-results+json"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("boolean", False)
    except Exception as e:
        log("dedup", "WARN", f"Dedup check failed: {e}")
        return False


def escape_sparql(s):
    """Escape a string for use inside SPARQL double quotes."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")


def compute_message_iri(data_iri, message_id, raw_bytes):
    """Compute the message IRI the same way the Rust binary does.

    With a Message-ID: data_iri/msg/<url-encoded-id>
    Without: data_iri/msg/sha256/<hex-digest>
    """
    base = data_iri.rstrip("/") + "/"
    if message_id:
        return f"{base}msg/{quote(message_id, safe='')}"
    digest = hashlib.sha256(raw_bytes).hexdigest()
    return f"{base}msg/sha256/{digest}"


def delete_message_triples(endpoint, access_token, graph_iri, msg_iri):
    """Remove all triples for a message and its attachments from QLever.

    Issues two DELETE WHERE operations:
    1. Delete triples where attachment sub-resources are the subject
       (found via schema:associatedMedia).
    2. Delete all triples where the message IRI is the subject.
    """
    delete_attachments = (
        f"DELETE WHERE {{\n"
        f"  GRAPH <{graph_iri}> {{\n"
        f"    <{msg_iri}> <http://schema.org/associatedMedia> ?att .\n"
        f"    ?att ?p ?o .\n"
        f"  }}\n"
        f"}}"
    )
    delete_msg = (
        f"DELETE WHERE {{\n"
        f"  GRAPH <{graph_iri}> {{\n"
        f"    <{msg_iri}> ?p ?o .\n"
        f"  }}\n"
        f"}}"
    )
    post_update(endpoint, access_token, delete_attachments)
    post_update(endpoint, access_token, delete_msg)


def convert_message(binary_path, raw_bytes, folder_name, graph_iri, data_iri, include_body, include_attachments):
    """Pipe raw RFC 822 bytes through mbox-rdf --stdin and return N-Quads lines."""
    cmd = [
        binary_path, "--stdin",
        "--folder-name", folder_name,
        "--graph-iri", graph_iri,
        "--data-iri", data_iri,
    ]
    if include_body:
        cmd.append("--include-body")
    if include_attachments:
        cmd.append("--include-attachments")

    result = subprocess.run(cmd, input=raw_bytes, capture_output=True, timeout=60)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"mbox-rdf exited with code {result.returncode}: {stderr}")

    return result.stdout.decode("utf-8", errors="replace")


def nquads_to_insert_data(nquads_text, graph_iri):
    """Convert N-Quads output to an INSERT DATA SPARQL update."""
    graph_suffix = f" <{graph_iri}> ."
    triples = []
    for line in nquads_text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        triple = line.replace(graph_suffix, " .")
        triples.append(f"    {triple}")

    if not triples:
        return None

    body = "\n".join(triples)
    return f"INSERT DATA {{\n  GRAPH <{graph_iri}> {{\n{body}\n  }}\n}}"


def post_update(endpoint, access_token, sparql_update):
    """POST a SPARQL UPDATE to QLever."""
    resp = requests.post(
        endpoint,
        data=sparql_update.encode("utf-8"),
        headers={
            "Content-Type": "application/sparql-update",
            "Authorization": f"Bearer {access_token}",
        },
        timeout=30,
    )
    resp.raise_for_status()


def load_state(state_file):
    """Load sync state from file.

    New format per folder:
      { "uids": { "<uid>": { "msg_iri": "...", "message_id": "..." } },
        "uidvalidity": <int> }
    Automatically migrates old watermark format (folder -> int).
    """
    if os.path.exists(state_file):
        with open(state_file) as f:
            raw = json.load(f)
        migrated = False
        for key, val in list(raw.items()):
            if isinstance(val, int):
                raw[key] = {"uids": {}, "uidvalidity": None}
                migrated = True
        if migrated:
            log("state", "INFO", "Migrated state file from watermark format (old UIDs forgotten)")
        return raw
    return {}


def parse_folder_config(raw_value):
    """Parse 'graph_iri | canonical_name' from a [folders] config value.

    Returns (graph_iri, canonical_name). If no '|' is present, the IMAP
    folder key itself should be used as canonical_name by the caller.
    """
    if "|" in raw_value:
        graph_iri, canonical = raw_value.split("|", 1)
        return graph_iri.strip(), canonical.strip()
    return raw_value.strip(), None


def save_state(state_file, state):
    """Persist last-seen UIDs to state file."""
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)


def monitor_folder(config, folder_name, graph_iri, canonical_name, state, state_file, state_lock):
    """Main loop for one IMAP folder. Runs in its own thread.

    Performs full UID-diff sync on each cycle: fetches the complete set of
    UIDs from the server, compares against the stored set, inserts new
    messages, and deletes triples for messages that have been removed.
    """
    host = config.get("imap", "host")
    port = config.getint("imap", "port", fallback=993)
    username = config.get("imap", "username")
    password = config.get("imap", "password", fallback="") or os.environ.get("IMAP_PASSWORD", "")
    use_starttls = config.getboolean("imap", "starttls", fallback=False)
    endpoint = config.get("qlever", "endpoint")
    access_token = config.get("qlever", "access_token")
    binary_path = config.get("rdf", "binary")
    data_iri = config.get("rdf", "data_iri")
    include_body = config.getboolean("rdf", "include_body", fallback=True)
    include_attachments = config.getboolean("rdf", "include_attachments", fallback=True)
    poll_interval = config.getint("imap", "poll_interval", fallback=300)

    backoff = 5

    while not shutdown_event.is_set():
        client = None
        try:
            log(folder_name, "INFO", f"Connecting to {host}:{port} ({'STARTTLS' if use_starttls else 'TLS'})...")
            client = IMAPClient(host, port=port, ssl=(not use_starttls))
            if use_starttls:
                client.starttls()
            client.login(username, password)
            log(folder_name, "INFO", f"Connected as {username}")

            select_info = client.select_folder(folder_name, readonly=True)
            server_uidvalidity = select_info.get(b"UIDVALIDITY")
            backoff = 5

            with state_lock:
                folder_state = state.setdefault(folder_name, {"uids": {}, "uidvalidity": None})
                stored_uidvalidity = folder_state.get("uidvalidity")

            if server_uidvalidity and stored_uidvalidity and server_uidvalidity != stored_uidvalidity:
                log(folder_name, "WARN",
                    f"UIDVALIDITY changed ({stored_uidvalidity} -> {server_uidvalidity}), resetting state")
                with state_lock:
                    folder_state["uids"] = {}
                    folder_state["uidvalidity"] = server_uidvalidity
                    save_state(state_file, state)

            if server_uidvalidity:
                with state_lock:
                    folder_state["uidvalidity"] = server_uidvalidity

            while not shutdown_event.is_set():
                server_uids = set(client.search(["ALL"]))

                with state_lock:
                    known_uids = {int(u) for u in folder_state.get("uids", {})}

                new_uids = sorted(server_uids - known_uids)
                deleted_uids = sorted(known_uids - server_uids)

                log(folder_name, "INFO",
                    f"Server has {len(server_uids)} messages, "
                    f"we know {len(known_uids)}: "
                    f"{len(new_uids)} new, {len(deleted_uids)} deleted")

                # --- Handle deletions ---
                for uid in deleted_uids:
                    if shutdown_event.is_set():
                        break

                    with state_lock:
                        uid_info = folder_state["uids"].get(str(uid), {})
                    msg_iri = uid_info.get("msg_iri")
                    if not msg_iri:
                        log(folder_name, "WARN", f"UID {uid}: deleted on server but no IRI stored, removing from state only")
                        with state_lock:
                            folder_state["uids"].pop(str(uid), None)
                            save_state(state_file, state)
                        continue

                    subject = uid_info.get("subject", "(unknown)")
                    log(folder_name, "INFO", f'UID {uid}: deleted on server, removing triples for "{subject}"')

                    try:
                        delete_message_triples(endpoint, access_token, graph_iri, msg_iri)
                        log(folder_name, "INFO", f"UID {uid}: triples deleted")
                        if _event_bus:
                            _event_bus.broadcast({
                                "event": "delete_mail",
                                "folder": folder_name,
                                "graph": graph_iri,
                                "uid": uid,
                                "message_id": uid_info.get("message_id"),
                                "msg_iri": msg_iri,
                                "subject": subject,
                            })
                    except Exception as e:
                        log(folder_name, "ERROR", f"UID {uid}: DELETE failed: {e}")

                    with state_lock:
                        folder_state["uids"].pop(str(uid), None)
                        save_state(state_file, state)

                # --- Handle new messages ---
                for uid in new_uids:
                    if shutdown_event.is_set():
                        break

                    fetch_resp = client.fetch([uid], ["RFC822"])
                    if uid not in fetch_resp:
                        log(folder_name, "WARN", f"UID {uid}: FETCH returned no data, skipping")
                        continue

                    raw_bytes = fetch_resp[uid][b"RFC822"]
                    message_id = extract_message_id(raw_bytes)
                    subject_match = re.search(rb"^Subject:\s*(.+)", raw_bytes, re.IGNORECASE | re.MULTILINE)
                    subject = subject_match.group(1).decode("utf-8", errors="replace").strip()[:60] if subject_match else "(no subject)"

                    log(folder_name, "INFO", f'UID {uid}: <{message_id or "?"}> "{subject}"')

                    msg_iri = compute_message_iri(data_iri, message_id, raw_bytes)

                    if message_id and check_duplicate(endpoint, access_token, graph_iri, message_id):
                        log(folder_name, "INFO", f"UID {uid}: already in store, recording UID only")
                        with state_lock:
                            folder_state["uids"][str(uid)] = {
                                "msg_iri": msg_iri, "message_id": message_id, "subject": subject,
                            }
                            save_state(state_file, state)
                        continue

                    if not message_id and check_duplicate_by_headers(endpoint, access_token, graph_iri, raw_bytes):
                        log(folder_name, "INFO", f"UID {uid}: already in store (header match), recording UID only")
                        with state_lock:
                            folder_state["uids"][str(uid)] = {
                                "msg_iri": msg_iri, "message_id": message_id, "subject": subject,
                            }
                            save_state(state_file, state)
                        continue

                    try:
                        nquads = convert_message(binary_path, raw_bytes, canonical_name, graph_iri, data_iri, include_body, include_attachments)
                    except Exception as e:
                        log(folder_name, "ERROR", f"UID {uid}: conversion failed: {e}")
                        continue

                    sparql = nquads_to_insert_data(nquads, graph_iri)
                    if sparql is None:
                        log(folder_name, "WARN", f"UID {uid}: no triples produced, skipping")
                        with state_lock:
                            folder_state["uids"][str(uid)] = {
                                "msg_iri": msg_iri, "message_id": message_id, "subject": subject,
                            }
                            save_state(state_file, state)
                        continue

                    triple_count = nquads.strip().count("\n") + 1
                    try:
                        post_update(endpoint, access_token, sparql)
                        log(folder_name, "INFO", f"UID {uid}: inserted {triple_count} triples")
                        if _event_bus:
                            _event_bus.broadcast({
                                "event": "new_mail",
                                "folder": folder_name,
                                "graph": graph_iri,
                                "uid": uid,
                                "message_id": message_id,
                                "subject": subject,
                                "triples": triple_count,
                            })
                    except Exception as e:
                        log(folder_name, "ERROR", f"UID {uid}: INSERT DATA failed: {e}")
                        continue

                    with state_lock:
                        folder_state["uids"][str(uid)] = {
                            "msg_iri": msg_iri, "message_id": message_id, "subject": subject,
                        }
                        save_state(state_file, state)

                if shutdown_event.is_set():
                    break

                log(folder_name, "INFO", f"Entering IDLE (timeout {poll_interval}s)...")
                try:
                    client.idle()
                    all_responses = []
                    elapsed = 0
                    while elapsed < poll_interval and not shutdown_event.is_set():
                        chunk = min(2, poll_interval - elapsed)
                        responses = client.idle_check(timeout=chunk)
                        if responses:
                            all_responses.extend(responses)
                            break
                        elapsed += chunk
                    client.idle_done()
                    if all_responses:
                        log(folder_name, "INFO", f"IDLE notification received: {all_responses}")
                    elif not shutdown_event.is_set():
                        log(folder_name, "INFO", "IDLE timeout, rechecking...")
                except Exception as e:
                    if shutdown_event.is_set():
                        break
                    log(folder_name, "WARN", f"IDLE error: {e}, reconnecting...")
                    break

        except Exception as e:
            if shutdown_event.is_set():
                break
            log(folder_name, "ERROR", f"Connection error: {e}")
            log(folder_name, "INFO", f"Reconnecting in {backoff}s...")
            shutdown_event.wait(backoff)
            backoff = min(backoff * 2, 300)
        finally:
            if client:
                try:
                    client.logout()
                except Exception:
                    pass

    log(folder_name, "INFO", "Thread stopped")


def _run_init(config, folders, state_file):
    """Connect to each folder, fetch all UIDs and their Message-IDs, save state.

    After init, the daemon knows every existing message and will only act on
    changes (new arrivals or deletions).
    """
    host = config.get("imap", "host")
    port = config.getint("imap", "port", fallback=993)
    username = config.get("imap", "username")
    password = config.get("imap", "password", fallback="") or os.environ.get("IMAP_PASSWORD", "")
    use_starttls = config.getboolean("imap", "starttls", fallback=False)
    data_iri = config.get("rdf", "data_iri")

    log("init", "INFO", f"Connecting to {host}:{port} ({'STARTTLS' if use_starttls else 'TLS'})...")
    client = IMAPClient(host, port=port, ssl=(not use_starttls))
    if use_starttls:
        client.starttls()
    client.login(username, password)
    log("init", "INFO", f"Connected as {username}")

    state = load_state(state_file)
    for folder_name in folders:
        select_info = client.select_folder(folder_name, readonly=True)
        uidvalidity = select_info.get(b"UIDVALIDITY")
        uids = client.search(["ALL"])
        log("init", "INFO", f"{folder_name}: {len(uids)} messages, fetching Message-IDs...")

        uid_map = {}
        batch_size = 200
        for i in range(0, len(uids), batch_size):
            batch = uids[i:i + batch_size]
            fetch_resp = client.fetch(batch, ["BODY.PEEK[HEADER.FIELDS (MESSAGE-ID SUBJECT)]"])
            for uid in batch:
                if uid not in fetch_resp:
                    continue
                header_key = b"BODY[HEADER.FIELDS (MESSAGE-ID SUBJECT)]"
                header_bytes = fetch_resp[uid].get(header_key, b"")
                message_id = extract_message_id(header_bytes)
                subject_match = re.search(rb"^Subject:\s*(.+)", header_bytes, re.IGNORECASE | re.MULTILINE)
                subject = subject_match.group(1).decode("utf-8", errors="replace").strip()[:60] if subject_match else ""
                msg_iri = compute_message_iri(data_iri, message_id, b"")
                uid_map[str(uid)] = {
                    "msg_iri": msg_iri, "message_id": message_id, "subject": subject,
                }

        state[folder_name] = {"uids": uid_map, "uidvalidity": uidvalidity}
        log("init", "INFO", f"{folder_name}: recorded {len(uid_map)} UIDs (UIDVALIDITY={uidvalidity})")

    client.logout()
    save_state(state_file, state)
    log("init", "INFO", f"State saved to {state_file} -- daemon will track changes from here")


def main():
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / "imap-sync.conf"

    init_mode = "--init" in sys.argv

    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        print("Copy imap-sync.conf.example to imap-sync.conf and fill in your settings.", file=sys.stderr)
        sys.exit(1)

    config = configparser.ConfigParser()
    config.optionxform = str  # preserve case for folder names
    config.read(config_path)

    state_file = config.get("state", "file", fallback="imap-sync.state.json")
    if not os.path.isabs(state_file):
        state_file = str(script_dir / state_file)

    raw_folders = dict(config.items("folders"))
    if not raw_folders:
        print("ERROR: No folders configured in [folders] section", file=sys.stderr)
        sys.exit(1)

    folders = {}
    for imap_name, raw_value in raw_folders.items():
        graph_iri, canonical = parse_folder_config(raw_value)
        folders[imap_name] = (graph_iri, canonical or imap_name)

    if init_mode:
        _run_init(config, folders, state_file)
        return

    socket_path = config.get("ipc", "socket_path", fallback="")
    if socket_path and not os.path.isabs(socket_path):
        socket_path = str(script_dir / socket_path)

    state = load_state(state_file)
    state_lock = threading.Lock()

    binary_path = config.get("rdf", "binary")
    if not os.path.isabs(binary_path):
        binary_path = str(script_dir / binary_path)
        config.set("rdf", "binary", binary_path)

    if not os.path.isfile(binary_path):
        print(f"ERROR: mbox-rdf binary not found at {binary_path}", file=sys.stderr)
        print("Build it first: cargo build --release", file=sys.stderr)
        sys.exit(1)

    def handle_signal(signum, frame):
        if shutdown_event.is_set():
            return
        sig_name = signal.Signals(signum).name
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{ts} [INFO] [main] Received {sig_name}, shutting down...", flush=True)
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    global _event_bus
    if socket_path:
        _event_bus = EventBus(socket_path)
        _event_bus.start()

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [INFO] [main] Starting imap-sync daemon", flush=True)
    print(f"{ts} [INFO] [main] Monitoring folders: {', '.join(f'{f} -> {g} (as {c})' for f, (g, c) in folders.items())}", flush=True)
    print(f"{ts} [INFO] [main] QLever endpoint: {config.get('qlever', 'endpoint')}", flush=True)
    print(f"{ts} [INFO] [main] State file: {state_file}", flush=True)

    threads = []
    for folder_name, (graph_iri, canonical_name) in folders.items():
        t = threading.Thread(
            target=monitor_folder,
            args=(config, folder_name, graph_iri, canonical_name, state, state_file, state_lock),
            name=f"imap-{folder_name}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        while t.is_alive():
            t.join(timeout=1.0)
            if shutdown_event.is_set():
                break

    if shutdown_event.is_set():
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{ts} [INFO] [main] Waiting for threads to finish...", flush=True)
        for t in threads:
            t.join(timeout=10)

    with state_lock:
        save_state(state_file, state)

    if _event_bus:
        _event_bus.stop()

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [INFO] [main] Shutdown complete", flush=True)


if __name__ == "__main__":
    main()
