#!/usr/bin/env python3
"""Local HTTP receiver for CendioSlackExtension that ingests messages into QLever.

Listens on http://localhost:19876/tag and accepts POSTs from the Chrome
extension. Each Slack message is inserted into the named graph
``urn:slack:robhe@cendio.com:messages`` in the same QLever instance that
imap-sync.py writes to.

Bootstraps the Slack graph with an ontology header on first run so the
graph is self-describing and always contains at least one triple.

Run with:
    source ~/my/venv/bin/activate
    python ~/my/mbox-rdf/qlever/slack-receiver.py
"""

from __future__ import annotations

import configparser
import hashlib
import json
import os
import sys
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests", file=sys.stderr)
    sys.exit(1)


HOST = "localhost"
PORT = 19876

DEFAULT_ENDPOINT = "http://localhost:7029"
DEFAULT_ACCESS_TOKEN = "mbox_access_token"
DEFAULT_GRAPH_IRI = "urn:slack:robhe@cendio.com:messages"
DEFAULT_DATA_IRI = "https://data.cendio.com/slack/"

SLACK_NS = "https://slack.described.at/"
SCHEMA_NS = "http://schema.org/"
RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"
VOID_NS = "http://rdfs.org/ns/void#"

MESSAGE_FIELDS = (
    "author",
    "channelName",
    "channelId",
    "teamId",
    "slackMessageId",
    "permalink",
    "content",
)

PER_MESSAGE_FIELDS = ("author", "slackMessageId", "permalink", "content")

BATCH_HEADER_FIELDS = (
    "teamId",
    "channelId",
    "channelName",
    "messageCount",
    "clickedMessageId",
    "capturedAt",
)


def log(level: str, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] [slack-receiver] {msg}"
    if level == "ERROR":
        print(f"\033[31m{line}\033[0m", file=sys.stderr, flush=True)
    elif level == "WARN":
        print(f"\033[33m{line}\033[0m", file=sys.stderr, flush=True)
    else:
        print(line, flush=True)


# --- Configuration -----------------------------------------------------------


class Config:
    def __init__(
        self,
        endpoint: str,
        access_token: str,
        graph_iri: str,
        data_iri: str,
    ) -> None:
        self.endpoint = endpoint
        self.access_token = access_token
        self.graph_iri = graph_iri
        self.data_iri = data_iri if data_iri.endswith("/") else data_iri + "/"


def load_config() -> Config:
    """Load settings from imap-sync.conf (alongside this script), with env overrides."""
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / "imap-sync.conf"

    endpoint = DEFAULT_ENDPOINT
    access_token = DEFAULT_ACCESS_TOKEN
    graph_iri = DEFAULT_GRAPH_IRI
    data_iri = DEFAULT_DATA_IRI

    if config_path.exists():
        cp = configparser.ConfigParser()
        cp.optionxform = str
        try:
            cp.read(config_path)
            if cp.has_section("qlever"):
                endpoint = cp.get("qlever", "endpoint", fallback=endpoint)
                access_token = cp.get("qlever", "access_token", fallback=access_token)
            if cp.has_section("slack"):
                graph_iri = cp.get("slack", "graph_iri", fallback=graph_iri)
                data_iri = cp.get("slack", "data_iri", fallback=data_iri)
        except Exception as exc:
            log("WARN", f"Could not parse {config_path}: {exc}; using defaults")

    endpoint = os.environ.get("QLEVER_ENDPOINT", endpoint)
    access_token = os.environ.get("QLEVER_ACCESS_TOKEN", access_token)
    graph_iri = os.environ.get("SLACK_GRAPH_IRI", graph_iri)
    data_iri = os.environ.get("SLACK_DATA_IRI", data_iri)

    return Config(endpoint, access_token, graph_iri, data_iri)


# --- SPARQL helpers ----------------------------------------------------------


def escape_sparql(value: str) -> str:
    """Escape a string for use inside a SPARQL double-quoted literal."""
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def ask_query(cfg: Config, query: str, timeout: float = 10.0) -> bool:
    resp = requests.get(
        cfg.endpoint,
        params={"query": query, "access-token": cfg.access_token},
        headers={"Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    resp.raise_for_status()
    return bool(resp.json().get("boolean", False))


def post_update(cfg: Config, sparql_update: str, timeout: float = 30.0) -> None:
    resp = requests.post(
        cfg.endpoint,
        data=sparql_update.encode("utf-8"),
        headers={
            "Content-Type": "application/sparql-update",
            "Authorization": f"Bearer {cfg.access_token}",
        },
        timeout=timeout,
    )
    resp.raise_for_status()


def count_triples(cfg: Config) -> int | None:
    query = f"SELECT (COUNT(*) AS ?c) WHERE {{ GRAPH <{cfg.graph_iri}> {{ ?s ?p ?o }} }}"
    try:
        resp = requests.get(
            cfg.endpoint,
            params={"query": query, "access-token": cfg.access_token},
            headers={"Accept": "application/sparql-results+json"},
            timeout=10,
        )
        resp.raise_for_status()
        bindings = resp.json().get("results", {}).get("bindings", [])
        if not bindings:
            return 0
        return int(bindings[0].get("c", {}).get("value", "0"))
    except Exception:
        return None


# --- Graph bootstrap ---------------------------------------------------------


def bootstrap_graph(cfg: Config) -> None:
    """Ensure the Slack graph exists; seed ontology header on first run."""
    try:
        has_any = ask_query(
            cfg,
            f"ASK {{ GRAPH <{cfg.graph_iri}> {{ ?s ?p ?o }} }}",
        )
    except Exception as exc:
        log("ERROR", f"Cannot reach QLever at {cfg.endpoint}: {exc}")
        log("ERROR", "Refusing to start HTTP listener against an unreachable backend")
        sys.exit(1)

    if has_any:
        n = count_triples(cfg)
        if n is None:
            log("INFO", f"Slack graph <{cfg.graph_iri}> already present")
        else:
            log("INFO", f"Slack graph <{cfg.graph_iri}> already present ({n} triples)")
        return

    dataset_iri = cfg.data_iri.rstrip("/")
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    triples = [
        f'<{dataset_iri}> <{RDF_NS}type> <{VOID_NS}Dataset> .',
        f'<{dataset_iri}> <{SCHEMA_NS}name> "Cendio Slack messages" .',
        f'<{dataset_iri}> <{SCHEMA_NS}dateCreated> "{now_iso}"^^<{XSD_NS}dateTime> .',
        f'<{dataset_iri}> <{SCHEMA_NS}creator> "slack-receiver.py" .',
        f'<{SLACK_NS}Channel> <{RDF_NS}type> <{RDFS_NS}Class> .',
        f'<{SLACK_NS}Channel> <{RDFS_NS}label> "Slack channel" .',
        f'<{SLACK_NS}Message> <{RDFS_NS}subClassOf> <{SCHEMA_NS}Message> .',
        f'<{SLACK_NS}Message> <{RDFS_NS}label> "Slack message" .',
        f'<{SLACK_NS}slackMessageId> <{RDFS_NS}label> "Slack message id (ts)" .',
        f'<{SLACK_NS}teamId> <{RDFS_NS}label> "Slack team id" .',
        f'<{SLACK_NS}channelId> <{RDFS_NS}label> "Slack channel id" .',
        f'<{SLACK_NS}capturedAt> <{RDFS_NS}label> "Timestamp when the message was captured" .',
        f'<{SLACK_NS}rawPayload> <{RDFS_NS}label> "Raw JSON of fields not otherwise modeled" .',
        f'<{SLACK_NS}tag> <{RDFS_NS}label> "User-assigned tag" .',
    ]
    body = "\n    ".join(triples)
    sparql = f"INSERT DATA {{\n  GRAPH <{cfg.graph_iri}> {{\n    {body}\n  }}\n}}"

    try:
        post_update(cfg, sparql)
    except Exception as exc:
        log("ERROR", f"Failed to bootstrap Slack graph: {exc}")
        sys.exit(1)

    log("INFO", f"Slack graph <{cfg.graph_iri}> bootstrapped ({len(triples)} header triples)")


# --- Per-message conversion --------------------------------------------------


def _slack_ts_to_iso(slack_message_id: str) -> str | None:
    """Slack ts is '<seconds>.<microseconds>' (epoch). Convert to UTC ISO-8601."""
    if not slack_message_id:
        return None
    try:
        seconds = float(slack_message_id)
    except ValueError:
        return None
    try:
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _author_iri(channel_iri: str, author: str) -> str:
    return f"{channel_iri}/author/{quote(author, safe='')}"


def _channel_iri(cfg: Config, team_id: str, channel_id: str) -> str:
    return (
        f"{cfg.data_iri}team/{quote(team_id, safe='')}"
        f"/channel/{quote(channel_id, safe='')}"
    )


def _message_iri(cfg: Config, team_id: str, channel_id: str, slack_message_id: str,
                 author: str, content: str) -> str:
    if team_id and channel_id and slack_message_id:
        channel_iri = _channel_iri(cfg, team_id, channel_id)
        return f"{channel_iri}/msg/{quote(slack_message_id, safe='')}"
    payload = f"{team_id}|{channel_id}|{slack_message_id}|{author}|{content}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"{cfg.data_iri}msg/sha256/{digest}"


def _extras_json(payload: dict, known_keys) -> str | None:
    extras = {k: v for k, v in payload.items() if k not in known_keys}
    if not extras:
        return None
    try:
        return json.dumps(extras, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return None


def build_insert(cfg: Config, msg: dict, batch_captured_at: str | None) -> tuple[str, str | None, str]:
    """Build the INSERT DATA SPARQL for a single Slack message.

    Returns (sparql, slack_message_id, msg_iri). slack_message_id may be None
    if the payload didn't include one.
    """
    slack_message_id = str(msg.get("slackMessageId") or "").strip()
    author = str(msg.get("author") or "").strip()
    channel_name = str(msg.get("channelName") or "").strip()
    channel_id = str(msg.get("channelId") or "").strip()
    team_id = str(msg.get("teamId") or "").strip()
    permalink = str(msg.get("permalink") or "").strip()
    content = str(msg.get("content") or "")

    msg_iri = _message_iri(cfg, team_id, channel_id, slack_message_id, author, content)
    channel_iri = _channel_iri(cfg, team_id, channel_id) if (team_id and channel_id) else None
    author_iri = _author_iri(channel_iri, author) if (channel_iri and author) else None

    captured_at = batch_captured_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    date_created = _slack_ts_to_iso(slack_message_id)

    triples: list[str] = [
        f'<{msg_iri}> <{RDF_NS}type> <{SCHEMA_NS}Message> .',
        f'<{msg_iri}> <{RDF_NS}type> <{SLACK_NS}Message> .',
        f'<{msg_iri}> <{SCHEMA_NS}text> "{escape_sparql(content)}" .',
        f'<{msg_iri}> <{SLACK_NS}capturedAt> "{escape_sparql(captured_at)}"^^<{XSD_NS}dateTime> .',
    ]
    if slack_message_id:
        triples.append(
            f'<{msg_iri}> <{SCHEMA_NS}identifier> "{escape_sparql(slack_message_id)}" .'
        )
        triples.append(
            f'<{msg_iri}> <{SLACK_NS}slackMessageId> "{escape_sparql(slack_message_id)}" .'
        )
    if date_created:
        triples.append(
            f'<{msg_iri}> <{SCHEMA_NS}dateCreated> "{date_created}"^^<{XSD_NS}dateTime> .'
        )
    if permalink:
        triples.append(f'<{msg_iri}> <{SCHEMA_NS}url> <{permalink}> .')
    if team_id:
        triples.append(
            f'<{msg_iri}> <{SLACK_NS}teamId> "{escape_sparql(team_id)}" .'
        )
    if channel_id:
        triples.append(
            f'<{msg_iri}> <{SLACK_NS}channelId> "{escape_sparql(channel_id)}" .'
        )
    if channel_iri:
        triples.append(f'<{msg_iri}> <{SCHEMA_NS}isPartOf> <{channel_iri}> .')
        triples.append(f'<{channel_iri}> <{RDF_NS}type> <{SLACK_NS}Channel> .')
        if channel_name:
            triples.append(
                f'<{channel_iri}> <{SCHEMA_NS}name> "{escape_sparql(channel_name)}" .'
            )
        if channel_id:
            triples.append(
                f'<{channel_iri}> <{SLACK_NS}channelId> "{escape_sparql(channel_id)}" .'
            )
        if team_id:
            triples.append(
                f'<{channel_iri}> <{SLACK_NS}teamId> "{escape_sparql(team_id)}" .'
            )
    if author_iri:
        triples.append(f'<{msg_iri}> <{SCHEMA_NS}author> <{author_iri}> .')
        triples.append(f'<{author_iri}> <{RDF_NS}type> <{SCHEMA_NS}Person> .')
        triples.append(
            f'<{author_iri}> <{SCHEMA_NS}name> "{escape_sparql(author)}" .'
        )
    elif author:
        triples.append(
            f'<{msg_iri}> <{SCHEMA_NS}author> "{escape_sparql(author)}" .'
        )

    extras = _extras_json(msg, PER_MESSAGE_FIELDS + MESSAGE_FIELDS)
    if extras:
        triples.append(
            f'<{msg_iri}> <{SLACK_NS}rawPayload> "{escape_sparql(extras)}" .'
        )

    body = "\n    ".join(triples)
    sparql = f"INSERT DATA {{\n  GRAPH <{cfg.graph_iri}> {{\n    {body}\n  }}\n}}"
    return sparql, (slack_message_id or None), msg_iri


# --- Dedup + insert with per-message-id lock --------------------------------


class MessageLockRegistry:
    """Per-slackMessageId locks to serialize ASK+INSERT for the same message."""

    def __init__(self) -> None:
        self._dict_lock = threading.Lock()
        self._locks: dict[str, tuple[threading.Lock, int]] = {}
        self._fallback_lock = threading.Lock()

    def acquire(self, key: str | None) -> threading.Lock:
        if not key:
            self._fallback_lock.acquire()
            return self._fallback_lock
        with self._dict_lock:
            entry = self._locks.get(key)
            if entry is None:
                lock = threading.Lock()
                self._locks[key] = (lock, 1)
            else:
                lock, count = entry
                self._locks[key] = (lock, count + 1)
        lock.acquire()
        return lock

    def release(self, key: str | None, lock: threading.Lock) -> None:
        lock.release()
        if not key:
            return
        with self._dict_lock:
            entry = self._locks.get(key)
            if entry is None:
                return
            _, count = entry
            if count <= 1:
                self._locks.pop(key, None)
            else:
                self._locks[key] = (entry[0], count - 1)


_locks = MessageLockRegistry()


def ingest_message(cfg: Config, msg: dict, batch_captured_at: str | None) -> dict:
    """Dedup + insert one Slack message. Returns a small status dict."""
    sparql, slack_message_id, msg_iri = build_insert(cfg, msg, batch_captured_at)

    lock = _locks.acquire(slack_message_id)
    try:
        if slack_message_id:
            try:
                exists = ask_query(
                    cfg,
                    f'ASK {{ GRAPH <{cfg.graph_iri}> {{ '
                    f'?m <{SLACK_NS}slackMessageId> "{escape_sparql(slack_message_id)}" '
                    f'}} }}',
                )
            except Exception as exc:
                log("ERROR", f"dedup ASK failed for {slack_message_id}: {exc}")
                return {"ok": False, "error": f"dedup check failed: {exc}"}
            if exists:
                log("INFO", f"duplicate: {slack_message_id} ({msg_iri})")
                return {"ok": True, "duplicate": True, "msgIRI": msg_iri}
        else:
            try:
                exists = ask_query(
                    cfg,
                    f"ASK {{ GRAPH <{cfg.graph_iri}> {{ <{msg_iri}> ?p ?o }} }}",
                )
            except Exception as exc:
                log("ERROR", f"dedup ASK failed for {msg_iri}: {exc}")
                return {"ok": False, "error": f"dedup check failed: {exc}"}
            if exists:
                log("INFO", f"duplicate (no slackMessageId): {msg_iri}")
                return {"ok": True, "duplicate": True, "msgIRI": msg_iri}

        try:
            post_update(cfg, sparql)
        except Exception as exc:
            log("ERROR", f"INSERT failed for {msg_iri}: {exc}")
            return {"ok": False, "error": f"insert failed: {exc}"}

        log(
            "INFO",
            f"inserted {msg_iri}"
            + (f" (id={slack_message_id})" if slack_message_id else ""),
        )
        return {"ok": True, "duplicate": False, "msgIRI": msg_iri}
    finally:
        _locks.release(slack_message_id, lock)


# --- HTTP server -------------------------------------------------------------


def _is_batch_payload(payload: dict) -> bool:
    return isinstance(payload.get("messages"), list)


def _write_cors_headers(handler: BaseHTTPRequestHandler) -> None:
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")


def handle_payload(cfg: Config, payload: dict) -> dict:
    if _is_batch_payload(payload):
        captured_at = str(payload.get("capturedAt") or "").strip() or None
        messages = payload.get("messages") or []
        results = []
        inserted = 0
        duplicates = 0
        errors = 0
        log(
            "INFO",
            f"batch: team={payload.get('teamId','')} channel={payload.get('channelName','')} "
            f"count={len(messages)} clicked={payload.get('clickedMessageId','')}",
        )
        for item in messages:
            if not isinstance(item, dict):
                errors += 1
                results.append({"ok": False, "error": "non-dict entry"})
                continue
            merged = {
                "teamId": payload.get("teamId", ""),
                "channelId": payload.get("channelId", ""),
                "channelName": payload.get("channelName", ""),
                **item,
            }
            result = ingest_message(cfg, merged, captured_at)
            results.append(result)
            if not result.get("ok"):
                errors += 1
            elif result.get("duplicate"):
                duplicates += 1
            else:
                inserted += 1
        return {
            "ok": errors == 0,
            "batch": True,
            "inserted": inserted,
            "duplicates": duplicates,
            "errors": errors,
            "total": len(messages),
            "results": results,
        }

    log(
        "INFO",
        f"single: author={payload.get('author','')} id={payload.get('slackMessageId','')} "
        f"channel={payload.get('channelName','')}",
    )
    return ingest_message(cfg, payload, None)


class TagHandler(BaseHTTPRequestHandler):
    server_version = "CendioSlackReceiver/2.0"
    cfg: Config  # assigned via subclassing below

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        sys.stderr.write(
            "[%s] %s - %s\n"
            % (self.log_date_time_string(), self.address_string(), format % args)
        )

    def do_OPTIONS(self) -> None:  # noqa: N802
        if self.path != "/tag":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self.send_response(HTTPStatus.NO_CONTENT)
        _write_cors_headers(self)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/tag":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b""

        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
            if not isinstance(payload, dict):
                raise ValueError("payload must be a JSON object")
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        try:
            result = handle_payload(self.cfg, payload)
        except Exception as exc:
            log("ERROR", f"unhandled error: {exc}")
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": f"internal error: {exc}"},
            )
            return

        status = HTTPStatus.OK if result.get("ok") else HTTPStatus.INTERNAL_SERVER_ERROR
        self._send_json(status, result)

    def do_GET(self) -> None:  # noqa: N802
        self.send_error(HTTPStatus.NOT_FOUND)

    def _send_json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        _write_cors_headers(self)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def make_handler(cfg: Config):
    class BoundHandler(TagHandler):
        pass

    BoundHandler.cfg = cfg
    return BoundHandler


def main() -> int:
    cfg = load_config()
    log("INFO", f"QLever endpoint: {cfg.endpoint}")
    log("INFO", f"Slack graph:     {cfg.graph_iri}")
    log("INFO", f"Data IRI base:   {cfg.data_iri}")

    bootstrap_graph(cfg)

    server = ThreadingHTTPServer((HOST, PORT), make_handler(cfg))
    log("INFO", f"Listening on http://{HOST}:{PORT}/tag (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("INFO", "Shutting down")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
