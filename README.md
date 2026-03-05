# mbox-rdf

`mbox-rdf` is a Proof-of-Concept (PoC) tool that converts local email archives (mbox files) into RDF. This allows users to leverage the power of SPARQL to analyze and query their email history in ways that are typically impossible with standard desktop email clients.

## Motivation

For those familiar with RDF and Graph databases, the search capabilities in tools like Thunderbird or Outlook feel extremely limiting. Email is naturally a graph—subjects, senders, recipients, and threads create a complex web of information. By converting this data to RDF, you can answer complex questions about your communication patterns, social connectivity, and archive consistency using standard Semantic Web technologies.

Currently, the tool operates on local mbox files (such as those used by Thunderbird). In the future, it might be extended to fetch data directly via IMAP or JMAP to act as a live bridge between your mailbox and a triple store.

## Features

- **Extremely Fast**: Capable of converting multi-GB mailboxes in a matter of seconds or minutes.
- **Streaming Architecture**: Designed to handle archives of any size with minimal memory footprint.
- **Gzip & N-Quads**: Native support for compressed output and named graphs for multi-account analysis.
- **Timezone Aware**: Accurate `xsd:dateTime` literals preserving original sender offsets.
- **Schema.org Aligned**: Uses `schema:CreativeWork`, `schema:MediaObject`, `schema:dateCreated`, and `schema:name` for interoperability.
- **SHACL Shapes**: Includes `mail-shapes.ttl` for validation and documentation.

## Usage

### Basic conversion

```bash
cargo run --release -- path/to/INBOX --output mail.nt
```

### Advanced usage (N-Quads + Gzip)

```bash
cargo run --release -- path/to/INBOX \
  --data-iri https://example.org/data/ \
  --graph-iri urn:email:user@example.com \
  --include-body \
  --output mail.nq.gz
```

### Options

```
-o, --output       Output path (.nt or .nq, optionally .gz) [default: mail.nt]
    --data-iri     Base IRI for instance data [default: https://example.org/data/]
    --graph-iri    Optional Graph IRI (enables N-Quads)
    --gzip         Force Gzip compression
    --include-body Include body text
    --folder-name  Folder name override (default: derived from filename)
    --limit        Limit number of messages to process
```

## Vocabulary

The schema namespace is `https://mail.described.at/`. Key concepts:

- **`mail:Message`** (also `schema:CreativeWork`): Individual email messages.
- **`mail:Account`**: Email accounts, identified by `mailto:` URIs (e.g., `<mailto:alice@example.com>`).
- **`mail:Thread`**: Conversation threads, derived from the `References` header chain.
- **`mail:MailingList`**: Mailing lists with `mail:listId`.
- **`schema:MediaObject`**: File attachments with `schema:sha256`, `schema:encodingFormat`, `schema:name`.

Dates use `schema:dateCreated` and `schema:dateReceived`. Display names use `schema:name`.

Schema files:
- `mail-schema.ttl` — RDFS class and property definitions
- `mail-shapes.ttl` — SHACL validation shapes with cardinality constraints
- `SPARQL_INSTRUCTIONS.md` — Complete reference for LLMs generating SPARQL queries

## LLM Integration

Point your LLM agent at [`SPARQL_INSTRUCTIONS.md`](SPARQL_INSTRUCTIONS.md) — it contains the complete schema reference with all classes, properties, cardinalities, and example query patterns. Enough for an LLM to generate correct SPARQL queries without needing to read the source code.

## SPARQL Queries

Queries live as standalone `.rq` files in `sparql/`. The `QUERIES.sparqlbook` references them for use with the [SPARQL Notebook](https://marketplace.visualstudio.com/items?itemName=Zazuko.sparql-notebook) VS Code extension.

Run the smoke test to verify all queries return results:

```bash
SPARQL_ENDPOINT=http://localhost:7029 ./sparql/smoke-test.sh
```

## Indexing and Querying with QLever

For large archives, we recommend using [QLever](https://github.com/ad-freiburg/qlever). It provides a high-performance SPARQL engine that can handle millions of triples with ease.

Detailed instructions and a pre-configured `Qleverfile` can be found in the [qlever/](qlever/) directory.

### Quick Start with QLever
```bash
cd qlever
qlever index  # Indexes the generated .nq.gz files
qlever start  # Starts the SPARQL endpoint
```

## Live IMAP Sync

The `imap-sync.py` daemon monitors IMAP folders in real-time via IDLE and inserts new messages into QLever as they arrive.

### Setup

```bash
cd qlever
pip install -r requirements-sync.txt
cp imap-sync.conf.example imap-sync.conf
# Edit imap-sync.conf with your IMAP credentials and settings
```

### Running

```bash
cd qlever
python3 imap-sync.py
```

The daemon logs all operations to stdout. Press Ctrl+C to stop. It persists its state (last-seen UID per folder) to `imap-sync.state.json` and resumes from where it left off on restart.

### stdin mode

The `mbox-rdf` binary also supports reading a single RFC 822 message from stdin:

```bash
cat message.eml | mbox-rdf --stdin \
  --folder-name INBOX \
  --graph-iri urn:email:user@example.com:INBOX \
  --include-body
```

### Interaction with benchmark.sh

Stop the daemon before running `benchmark.sh` (it checks and refuses to run if the daemon is active). After `benchmark.sh` completes, restart the daemon manually. The deduplication check ensures no messages are inserted twice.

## Roadmap

- [x] **Live IMAP sync** — real-time monitoring via IMAP IDLE with INSERT DATA to QLever.
- [ ] **Text search** — implement QLever materialized views with `ql:has-word` for ranked keyword search over subjects (weight 5) and body text (weight 1). Waiting on [QLever PR #2579](https://github.com/ad-freiburg/qlever/pull/2579).
- [ ] **URL extraction** — re-enable `mail:linksTo` by parsing `<a href>` from HTML parts instead of regex on plain text.

## License

This project is licensed under the MIT License.

---
*Developed with the assistance of Google Antigravity*
