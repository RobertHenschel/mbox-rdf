# Mail RDF — SPARQL Query Guide for LLMs

You are querying an RDF dataset of email archives. This document describes the complete schema so you can generate correct SPARQL queries.

## Prefixes

Always declare these prefixes:

```sparql
PREFIX mail: <https://mail.described.at/>
PREFIX schema: <http://schema.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
```

## Named Graphs

Each email account is stored in its own named graph (N-Quads). Example graph IRIs:

- `urn:email:user@domain.org`
- `urn:email:someone@gmail.com`

Use `FROM <urn:email:...>` to scope queries to one account, or omit it to query across all.

## Classes and Properties

### mail:Message (also a schema:CreativeWork)

An email message. Every message also has type `schema:CreativeWork`.

| Property | Type | Card. | Description |
|----------|------|-------|-------------|
| `mail:folder` | `mail:Mailbox` (IRI) | 1 | Folder containing the message |
| `mail:sourcePath` | `xsd:string` | 1 | Local filesystem path to source mbox |
| `mail:size` | `xsd:integer` | 1 | Raw message size in bytes |
| `mail:messageId` | `xsd:string` | 0..1 | RFC 822 Message-ID |
| `mail:mid` | IRI (`mid:...`) | 0..1 | Thunderbird-compatible mid: link |
| `mail:subject` | `xsd:string` | 0..1 | Subject line |
| `mail:normalizedSubject` | `xsd:string` | 0..1 | Subject with Re:/Fwd:/Aw: stripped |
| `schema:dateCreated` | `xsd:dateTime` | 0..1 | Date sent |
| `schema:dateReceived` | `xsd:dateTime` | 0..1 | Date received (from Received header) |
| `mail:userAgent` | `xsd:string` | 0..1 | Mail client (User-Agent/X-Mailer) |
| `mail:bodyText` | `xsd:string` | 0..1 | Plain text body (only if enabled) |
| `mail:bodyHtml` | `xsd:string` | 0..1 | HTML body (only if message has HTML part, requires --include-body) |
| `mail:thread` | `mail:Thread` (IRI) | 0..1 | Conversation thread |
| `mail:belongsToList` | `mail:MailingList` (IRI) | 0..1 | Mailing list |
| `mail:attachmentCount` | `xsd:integer` | 0..1 | Number of attachments (only if > 0) |
| `mail:from` | `mail:Account` (IRI) | 0..n | Sender(s) |
| `mail:to` | `mail:Account` (IRI) | 0..n | Primary recipients |
| `mail:cc` | `mail:Account` (IRI) | 0..n | CC recipients |
| `mail:bcc` | `mail:Account` (IRI) | 0..n | BCC recipients |
| `mail:replyTo` | `mail:Account` (IRI) | 0..n | Reply-To address(es) |
| `mail:inReplyTo` | `mail:Message` (IRI) | 0..n | Parent message(s) |
| `mail:references` | `mail:Message` (IRI) | 0..n | Ancestor messages in thread |
| `schema:associatedMedia` | `schema:MediaObject` (IRI) | 0..n | Attachments |
| `mail:linksTo` | IRI | 0..n | URLs mentioned in body (currently disabled) |

### mail:Account

An email account, **identified by its `mailto:` URI**. Example: `<mailto:alice@example.com>`.

The email address is the URI itself — there is no separate address property.

| Property | Type | Card. | Description |
|----------|------|-------|-------------|
| `schema:name` | `xsd:string` | 0..1 | Display name |

**To extract the email address as a string, use `STR(?account)` and strip the `mailto:` prefix:**

```sparql
BIND(REPLACE(STR(?account), "^mailto:", "") AS ?email)
```

### mail:Thread

A conversation thread. Messages link to threads via `mail:thread`. The thread IRI is derived from the first message in the `References` header chain (the conversation root).

No properties on the thread itself — query via messages:

```sparql
SELECT ?thread (COUNT(?msg) AS ?count) WHERE {
  ?msg mail:thread ?thread .
} GROUP BY ?thread
```

### mail:MailingList

| Property | Type | Card. | Description |
|----------|------|-------|-------------|
| `mail:listId` | `xsd:string` | 1 | List-Id header value |
| `mail:listArchive` | IRI | 0..1 | Archive URL |

### schema:MediaObject (attachments)

Attachment IRI is content-addressable: `<data:.../attachment/sha256/{hash}>`.

| Property | Type | Card. | Description |
|----------|------|-------|-------------|
| `schema:sha256` | `xsd:string` | 1 | SHA-256 hash of content |
| `schema:encodingFormat` | `xsd:string` | 1 | MIME type |
| `schema:name` | `xsd:string` | 0..1 | Filename |
| `schema:contentSize` | `xsd:integer` | 0..1 | Size in bytes |
| `schema:text` | `xsd:string` | 0..1 | Text attachment body (only if `--include-attachments` enabled, `text/*` MIME types) |
| `schema:contentUrl` | `xsd:string` | 0..1 | Binary attachment body as base64 data URI (only if `--include-attachments` enabled, non-text MIME types) |

## Common Query Patterns

### Find messages by sender email

```sparql
SELECT ?msg ?subject WHERE {
  ?msg mail:from <mailto:alice@example.com> ;
       mail:subject ?subject .
}
```

### Find all messages in a thread, ordered by date

```sparql
SELECT ?msg ?subject ?date WHERE {
  ?msg mail:thread ?thread ;
       mail:subject ?subject ;
       schema:dateCreated ?date .
  # Pick a thread by knowing a message in it
  <mailto:known-msg-id> mail:thread ?thread .
}
ORDER BY ?date
```

### Count messages per sender with display name

```sparql
SELECT ?sender ?name (COUNT(?msg) AS ?count) WHERE {
  ?msg mail:from ?sender .
  OPTIONAL { ?sender schema:name ?name }
}
GROUP BY ?sender ?name
ORDER BY DESC(?count)
```

### Find messages with large attachments

```sparql
SELECT ?msg ?subject ?filename ?size WHERE {
  ?msg schema:associatedMedia ?att ;
       mail:subject ?subject .
  ?att schema:name ?filename ;
       schema:contentSize ?size .
  FILTER(?size > 1000000)
}
ORDER BY DESC(?size)
```

### Text search (QLever-specific)

> **TODO**: QLever is replacing `ql:contains-word` with `ql:has-word` + materialized views
> (see [QLever PR #2579](https://github.com/ad-freiburg/qlever/pull/2579)).
> Once merged, create a materialized view over `mail:subject` (weight 5) and
> `mail:bodyText` (weight 1) for ranked keyword search across messages.

## Tips

- **Dates** are `xsd:dateTime`. Use `FILTER(?date >= "2024-01-01"^^xsd:dateTime)` for date ranges.
- **Addresses are IRIs**, not strings. Use `STR(?account)` with `CONTAINS` for substring matching.
- **OPTIONAL** is needed for most properties since they are 0..1 or 0..n.
- **mail:normalizedSubject** is useful for grouping conversations without Re:/Fwd: noise.
- **mail:thread** is the simplest way to group related messages.
