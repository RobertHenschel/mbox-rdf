# Multi-Device Sync Protocol

The `imap-sync.py` daemon supports replicating app-specific data (tags, follow-ups, address book) across multiple devices using a hidden IMAP folder as the transport layer.

Each device runs its own QLever instance and `imap-sync.py` daemon. Email data syncs naturally because every device talks to the same IMAP server. App-specific data is replicated through sync events stored in a dedicated IMAP folder.

## Architecture

```
Device A                        IMAP Server                     Device B
┌──────────┐                 ┌──────────────┐                ┌──────────┐
│ App      │──SPARQL UPDATE──│              │                │          │
│ QLever   │                 │ INBOX        │                │ QLever   │
│ Daemon   │──IMAP APPEND──▶│ INBOX.Sent   │                │ Daemon   │
│          │                 │ INBOX.mbox-  │◀──IMAP IDLE──▶│          │
│          │◀──IMAP IDLE────│  rdf-sync    │──IMAP FETCH──▶│          │
└──────────┘                 └──────────────┘                └──────────┘
```

When the app writes data to QLever (e.g. tags a message), it also writes a sync event to the IMAP sync folder. The daemon on every other device picks up the event via IDLE and replays it into their local QLever.

## Configuration

In `imap-sync.conf`:

```ini
[sync]
enabled = true
folder = INBOX.mbox-rdf-sync
device_id = mac-a
```

| Key | Description |
|-----|-------------|
| `enabled` | Set to `true` to enable multi-device sync |
| `folder` | IMAP folder name for sync events (created automatically if missing) |
| `device_id` | Unique identifier for this device (e.g. hostname). Used to skip self-originated events. |

## Sync Event Format

Sync events are RFC 822 messages stored in the sync IMAP folder. Each message wraps a JSON array of operations.

### RFC 822 Envelope

| Header | Value |
|--------|-------|
| `From` | `mbox-rdf-sync@<device_id>` |
| `Subject` | `mbox-rdf sync (N ops)` |
| `Message-ID` | `<sync-<uuid>@<device_id>>` |
| `X-Mbox-RDF-Sync` | `v1` (protocol version) |
| `X-Sync-Device-ID` | The originating device's `device_id` |
| `Content-Type` | `text/plain; charset=utf-8` |

The body is a JSON array of operation objects.

### Operation Types

#### `insert` -- Add triples

```json
{
  "op": "insert",
  "graph": "urn:email:robhe@cendio.com:tags",
  "triples": [
    "<https://data.cendio.com/mbox/user/msg/abc123%40example.com> <https://mail.described.at/tag> \"important\" ."
  ],
  "timestamp": "2026-03-06T17:45:00+00:00"
}
```

#### `delete` -- Remove specific triples

```json
{
  "op": "delete",
  "graph": "urn:email:robhe@cendio.com:tags",
  "triples": [
    "<https://data.cendio.com/mbox/user/msg/abc123%40example.com> <https://mail.described.at/tag> \"important\" ."
  ],
  "timestamp": "2026-03-06T18:00:00+00:00"
}
```

#### `clear_subject` -- Remove all triples for a subject

```json
{
  "op": "clear_subject",
  "graph": "urn:email:robhe@cendio.com:followup",
  "subject_iri": "https://data.cendio.com/mbox/user/msg/abc123%40example.com",
  "triples": [],
  "timestamp": "2026-03-06T18:10:00+00:00"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `op` | `string` | `"insert"`, `"delete"`, or `"clear_subject"` |
| `graph` | `string` | Named graph IRI to operate on |
| `triples` | `string[]` | N-Triples lines (subject predicate object .) |
| `subject_iri` | `string` | (only for `clear_subject`) The subject IRI to remove |
| `timestamp` | `string` | ISO 8601 timestamp of when the operation was performed |

## App-Specific Graphs

These are the graphs that carry app-specific data and are replicated via sync:

| Graph | Purpose | Predicates |
|-------|---------|------------|
| `urn:email:robhe@cendio.com:tags` | User-applied tags on messages | `<https://mail.described.at/tag>` |
| `urn:email:robhe@cendio.com:sheet-tags` | Tags imported from spreadsheets | `<https://mail.described.at/tag>` |
| `urn:email:robhe@cendio.com:followup` | Follow-up flags on messages | `<https://mail.described.at/needsFollowUp>` (xsd:boolean) |
| `urn:email:robhe@cendio.com:addressbook` | Contact/address book entries | Various schema.org predicates |

The email graphs (`urn:email:robhe@cendio.com:INBOX`, `urn:email:robhe@cendio.com:SENT`) are **not** synced this way -- they sync via IMAP itself.

## IPC Event: `sync_update`

When the daemon replays a sync event from another device, it broadcasts a `sync_update` event on the IPC socket (see [IPC_PROTOCOL.md](IPC_PROTOCOL.md)):

```json
{
  "event": "sync_update",
  "op": "insert",
  "graph": "urn:email:robhe@cendio.com:tags",
  "subject_iri": "https://data.cendio.com/mbox/user/msg/abc123%40example.com",
  "device_id": "mac-b"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `event` | `string` | Always `"sync_update"` |
| `op` | `string` | The operation that was replayed (`"insert"`, `"delete"`, `"clear_subject"`) |
| `graph` | `string` | The graph that was modified |
| `subject_iri` | `string\|null` | The primary subject IRI affected (if available) |
| `device_id` | `string` | Which device originated the change |

**App action:** When you receive `sync_update`, re-run any queries that depend on `event.graph` and refresh the UI.

## What the App Must Implement

### 1. Listen for `sync_update` on the IPC socket

The app already listens for `new_mail` and `delete_mail`. Add handling for `sync_update`:

```javascript
client.on("data", (chunk) => {
  // ... existing NDJSON parsing ...
  const event = JSON.parse(line);
  switch (event.event) {
    case "new_mail":
    case "delete_mail":
      // existing handling
      break;
    case "sync_update":
      // A remote device changed app data -- refresh affected queries
      refreshGraph(event.graph);
      break;
  }
});
```

### 2. Write sync events when modifying app data

When the app writes to QLever (e.g. adding a tag), it must **also** write a sync event to the IMAP sync folder. There are two approaches:

**Option A (recommended): Direct IMAP APPEND from the app**

The app opens its own IMAP connection (using the same credentials from `imap-sync.conf`) and APPENDs a sync message. The JSON body format is documented above.

```javascript
// Pseudocode
async function tagMessage(msgIri, tag) {
  const triple = `<${msgIri}> <https://mail.described.at/tag> "${tag}" .`;
  const graph = "urn:email:robhe@cendio.com:tags";

  // 1. Apply locally
  await sparqlUpdate(`INSERT DATA { GRAPH <${graph}> { ${triple} } }`);

  // 2. Write sync event to IMAP
  const event = {
    op: "insert",
    graph: graph,
    triples: [triple],
    timestamp: new Date().toISOString()
  };
  await imapAppendSyncMessage(deviceId, [event]);
}
```

**Option B: Let the daemon handle it (future)**

A future enhancement could make the IPC socket bidirectional -- the app sends a write request to the daemon, and the daemon handles both the local SPARQL UPDATE and the IMAP APPEND. This is not implemented yet.

### 3. Idempotency

- `INSERT DATA` is idempotent in QLever for the same triple (inserting a triple that already exists is a no-op).
- `DELETE DATA` is also idempotent (deleting a triple that doesn't exist is a no-op).
- `clear_subject` (DELETE WHERE) is idempotent by nature.

This means sync events can be safely replayed without side effects. The daemon does not deduplicate -- it relies on SPARQL idempotency.

### 4. Device ID

Each device must have a unique, stable `device_id`. The daemon uses this to skip events it originated. Recommendations:

- Use the machine's hostname: `mac-a`, `mac-b`, `work-laptop`
- Set it once in `imap-sync.conf` and don't change it
- The app must use the **same** `device_id` when writing sync events

## CLI Commands

| Command | Description |
|---------|-------------|
| `./imap-sync.py` | Normal daemon mode (monitors mail + sync folders) |
| `./imap-sync.py --init` | Initialize state for all folders including sync (marks existing UIDs as known) |
| `./imap-sync.py --seed-sync` | Export all app-specific graphs from local QLever to the IMAP sync folder |
| `./imap-sync.py --sync-replay` | Replay all sync events from IMAP into local QLever (for bootstrapping a new device) |

## Migration Guide: Two Existing Devices

Scenario: Mac-A has the authoritative app data, Mac-B needs to catch up.

### On Mac-A (primary):

```bash
# 1. Update imap-sync.conf: add [sync] section with device_id = mac-a
# 2. Seed the sync folder with current app data
./imap-sync.py --seed-sync

# 3. Re-init so the daemon knows about the sync folder
./imap-sync.py --init

# 4. Start the daemon
./imap-sync.py
```

### On Mac-B (secondary):

```bash
# 1. Update imap-sync.conf: add [sync] section with device_id = mac-b
# 2. Init mail folders
./imap-sync.py --init

# 3. Replay all sync events into local QLever
./imap-sync.py --sync-replay

# 4. Start the daemon
./imap-sync.py
```

After this, both devices are in sync and will stay in sync via the IMAP sync folder.

### Adding a New Device Later

Same as Mac-B above: configure `[sync]`, run `--init`, run `--sync-replay`, start the daemon.

## Notes

- The sync folder (`INBOX.mbox-rdf-sync`) is created automatically if it doesn't exist.
- Sync events are never deleted from the IMAP folder (they serve as an append-only log).
- The daemon skips events from its own `device_id` (they were already applied locally).
- Multiple devices can connect simultaneously -- each monitors the folder via IDLE.
- If a device is offline, events accumulate on the IMAP server and are processed on reconnect.
