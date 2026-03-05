# imap-sync IPC Protocol

The `imap-sync.py` daemon exposes a Unix domain socket that applications can connect to for **real-time notifications** when new email is inserted into QLever.

## Configuration

In `imap-sync.conf`, the `[ipc]` section controls the socket:

```ini
[ipc]
socket_path = /tmp/imap-sync.sock
```

Set `socket_path` to empty to disable IPC. The daemon logs when the socket starts and when clients connect/disconnect.

## Protocol

- **Transport:** Unix domain socket (`AF_UNIX`, `SOCK_STREAM`)
- **Format:** Newline-delimited JSON (NDJSON). Each line is a complete JSON object terminated by `\n`.
- **Direction:** Server (daemon) -> Client (your app). The daemon only writes; it never reads from the client.
- **Connection:** Connect and keep the connection open. Events stream as they occur. Reconnect if the connection drops.

## Event Types

### `new_mail`

Emitted immediately after a new message is successfully inserted into QLever.

```json
{
  "event": "new_mail",
  "folder": "INBOX",
  "graph": "urn:email:robhe@cendio.com:INBOX",
  "uid": 7341,
  "message_id": "abc123@example.com",
  "subject": "Meeting tomorrow",
  "triples": 42
}
```

| Field | Type | Description |
|---|---|---|
| `event` | `string` | Always `"new_mail"` |
| `folder` | `string` | IMAP folder name (e.g. `"INBOX"`, `"INBOX.Sent"`) |
| `graph` | `string` | QLever named graph IRI the triples were inserted into |
| `uid` | `int` | IMAP UID of the message |
| `message_id` | `string\|null` | RFC 822 Message-ID (without angle brackets), or `null` if the message had no Message-ID header |
| `subject` | `string` | Subject line (truncated to 60 chars) |
| `triples` | `int` | Number of RDF triples inserted |

## Client Examples

### Python

```python
import socket
import json

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect("/tmp/imap-sync.sock")

buf = b""
while True:
    data = sock.recv(4096)
    if not data:
        break  # daemon disconnected
    buf += data
    while b"\n" in buf:
        line, buf = buf.split(b"\n", 1)
        event = json.loads(line)
        print(f"New mail in {event['folder']}: {event['subject']}")
        # Re-run your SPARQL queries here, scoped to event["graph"]
```

### JavaScript / Node.js

```javascript
const net = require("net");

const client = net.createConnection("/tmp/imap-sync.sock");
let buf = "";

client.on("data", (chunk) => {
  buf += chunk.toString();
  let idx;
  while ((idx = buf.indexOf("\n")) !== -1) {
    const event = JSON.parse(buf.slice(0, idx));
    buf = buf.slice(idx + 1);
    console.log(`New mail in ${event.folder}: ${event.subject}`);
    // Re-run your SPARQL queries here, scoped to event.graph
  }
});

client.on("end", () => console.log("Daemon disconnected"));
client.on("error", (err) => console.error("IPC error:", err.message));
```

### Bash (for testing)

```bash
socat - UNIX-CONNECT:/tmp/imap-sync.sock
```

This prints each JSON event as it arrives. Useful for verifying the daemon is emitting events.

## Integration Pattern

A typical app integration looks like:

1. On startup, run your initial SPARQL queries against QLever.
2. Connect to the IPC socket in a background thread/task.
3. When a `new_mail` event arrives:
   - Check `event.graph` to see which graph changed.
   - Re-run any queries that depend on that graph.
   - Update the UI or notify the user.
4. If the socket connection drops (daemon restarted), retry with backoff.

## Notes

- Multiple clients can connect simultaneously.
- The daemon cleans up the socket file on normal shutdown.
- If the daemon crashes, a stale socket file may remain. The daemon removes it on next start.
- Events are only emitted for newly inserted messages, not for skipped duplicates.
- The socket is local-only (Unix domain socket), so there are no network security concerns.
