#!/usr/bin/env python3
"""IMAP IDLE sync daemon for mbox-rdf.

Monitors IMAP folders via IDLE, converts new messages to RDF using the
mbox-rdf binary, and inserts them into QLever via SPARQL UPDATE.
"""

import configparser
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
    """Load last-seen UIDs from state file."""
    if os.path.exists(state_file):
        with open(state_file) as f:
            return json.load(f)
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
    """Main loop for one IMAP folder. Runs in its own thread."""
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

            client.select_folder(folder_name, readonly=True)
            backoff = 5

            while not shutdown_event.is_set():
                with state_lock:
                    last_uid = state.get(folder_name, 0)

                log(folder_name, "INFO", f"Last seen UID: {last_uid}, searching for new messages...")

                if last_uid == 0:
                    uids = client.search(["ALL"])
                else:
                    uids = client.search(["UID", f"{last_uid + 1}:*"])
                    uids = [u for u in uids if u > last_uid]

                if uids:
                    log(folder_name, "INFO", f"Found {len(uids)} new message(s) (UIDs {uids[0]}-{uids[-1]})")
                else:
                    log(folder_name, "INFO", "No new messages")

                for uid in uids:
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

                    if message_id and check_duplicate(endpoint, access_token, graph_iri, message_id):
                        log(folder_name, "INFO", f"UID {uid}: already in store, skipping")
                        with state_lock:
                            state[folder_name] = uid
                            save_state(state_file, state)
                        continue

                    if not message_id and check_duplicate_by_headers(endpoint, access_token, graph_iri, raw_bytes):
                        log(folder_name, "INFO", f"UID {uid}: already in store (header match), skipping")
                        with state_lock:
                            state[folder_name] = uid
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
                            state[folder_name] = uid
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
                        state[folder_name] = uid
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
    """Connect to each folder, record the highest UID, save state, and exit."""
    host = config.get("imap", "host")
    port = config.getint("imap", "port", fallback=993)
    username = config.get("imap", "username")
    password = config.get("imap", "password", fallback="") or os.environ.get("IMAP_PASSWORD", "")
    use_starttls = config.getboolean("imap", "starttls", fallback=False)

    log("init", "INFO", f"Connecting to {host}:{port} ({'STARTTLS' if use_starttls else 'TLS'})...")
    client = IMAPClient(host, port=port, ssl=(not use_starttls))
    if use_starttls:
        client.starttls()
    client.login(username, password)
    log("init", "INFO", f"Connected as {username}")

    state = load_state(state_file)
    for folder_name in folders:
        client.select_folder(folder_name, readonly=True)
        uids = client.search(["ALL"])
        max_uid = max(uids) if uids else 0
        state[folder_name] = max_uid
        log("init", "INFO", f"{folder_name}: {len(uids)} messages, max UID = {max_uid}")

    client.logout()
    save_state(state_file, state)
    log("init", "INFO", f"State saved to {state_file} -- daemon will only process new messages")


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
