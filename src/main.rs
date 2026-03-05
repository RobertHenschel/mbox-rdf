mod vocab;
mod mbox;

use anyhow::{Context, Result, bail};
use clap::Parser;
use mbox::MboxFile;
use mail_parser::{MessageParser, Address, Addr, MimeHeaders, HeaderValue};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Sha256, Digest};
use base64::Engine;
use vocab::{Vocab, DEFAULT_DATA_IRI, DEFAULT_SCHEMA_IRI};
use regex::Regex;
use std::sync::OnceLock;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input mbox file(s)
    inputs: Vec<String>,

    /// Read a single RFC 822 message from stdin instead of mbox files
    #[arg(long, default_value_t = false)]
    stdin: bool,

    /// Output path for RDF data (.nt or .nq, optionally .gz)
    #[arg(short, long, default_value = "mail.nt")]
    output: String,

    /// Base IRI for instance data
    #[arg(long, default_value = DEFAULT_DATA_IRI)]
    data_iri: String,

    /// Optional Graph IRI (enables N-Quads)
    #[arg(long)]
    graph_iri: Option<String>,

    /// Force Gzip compression (enabled automatically if output ends in .gz)
    #[arg(long)]
    gzip: bool,

    /// Include body text
    #[arg(long, default_value_t = false)]
    include_body: bool,

    /// Include attachment body content
    #[arg(long, default_value_t = false)]
    include_attachments: bool,

    /// Max attachment size (bytes) to include body for (requires --include-attachments)
    #[arg(long)]
    max_attachment_size: Option<usize>,

    /// Folder name override (default: derived from filename)
    #[arg(long)]
    folder_name: Option<String>,

    /// Limit number of messages to process
    #[arg(long)]
    limit: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if !args.stdin && args.inputs.is_empty() {
        bail!("Either provide input mbox file(s) or use --stdin");
    }

    let vocab = Vocab::new(DEFAULT_SCHEMA_IRI.to_string(), args.data_iri.clone());

    if args.stdin {
        let folder_name = args.folder_name.clone()
            .unwrap_or_else(|| "unknown".to_string());
        let folder_iri_str = vocab.folder_iri(&folder_name);

        let mut raw_message = Vec::new();
        std::io::stdin().read_to_end(&mut raw_message)
            .context("Failed to read message from stdin")?;

        let mut writer = BufWriter::new(std::io::stdout().lock());
        process_message(&vocab, &raw_message, &folder_iri_str, "imap", args.include_body, args.include_attachments, args.max_attachment_size, args.graph_iri.as_deref(), &mut writer)?;
        writer.flush()?;
        return Ok(());
    }

    let out_file = File::create(&args.output)
        .with_context(|| format!("Failed to create output file {}", args.output))?;
    
    let is_gzip = args.gzip || args.output.ends_with(".gz");
    
    let writer: Box<dyn Write> = if is_gzip {
        println!("Encryption enabled (gzip)");
        Box::new(GzEncoder::new(out_file, Compression::default()))
    } else {
        Box::new(out_file)
    };
    
    let mut writer = BufWriter::new(writer);

    let mut total_processed = 0;
    let mut total_failed = 0;

    for input_path in &args.inputs {
        let path = Path::new(input_path);
        let folder_name = args.folder_name.clone().unwrap_or_else(|| {
            path.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string()
        });
        let folder_iri_str = vocab.folder_iri(&folder_name);
        
        let absolute_input_path = path.canonicalize()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| input_path.clone());

        println!("Processing folder: {} ({})", folder_name, input_path);

        let mbox = MboxFile::from_file(Path::new(input_path))
            .with_context(|| format!("Failed to open mbox file {}", input_path))?;

        for entry in mbox.iter() {
            if let Some(limit) = args.limit {
                if total_processed >= limit {
                    break;
                }
            }

            let raw_message = entry.message();
            match process_message(&vocab, raw_message, &folder_iri_str, &absolute_input_path, args.include_body, args.include_attachments, args.max_attachment_size, args.graph_iri.as_deref(), &mut writer) {
                Ok(_) => {
                    total_processed += 1;
                    if total_processed % 100 == 0 {
                        println!("Processed {} messages...", total_processed);
                    }
                }
                Err(e) => {
                    eprintln!("Error processing message {} in {}: {}", total_processed + total_failed, input_path, e);
                    total_failed += 1;
                }
            }
        }
    }

    writer.flush()?;
    println!("Done! Total processed: {}, Total failed: {}", total_processed, total_failed);

    Ok(())
}

fn process_message<W: Write>(
    vocab: &Vocab,
    raw_message: &[u8],
    folder_iri_str: &str,
    source_path: &str,
    include_body: bool,
    include_attachments: bool,
    max_attachment_size: Option<usize>,
    graph_iri: Option<&str>,
    writer: &mut W,
) -> Result<()> {
    let msg = MessageParser::default().parse(raw_message)
        .context("Failed to parse email message")?;

    let msg_id = msg.message_id();
    let msg_iri_str = vocab.message_iri(msg_id, raw_message);
    
    let mut triples = Vec::new();
    let s = msg_iri_str.clone();

    // Type: Message + schema:CreativeWork
    add_iri_triple(&mut triples, &s, vocab::RDF_TYPE, &vocab.term("Message"), graph_iri);
    add_iri_triple(&mut triples, &s, vocab::RDF_TYPE, &vocab.schema_term("CreativeWork"), graph_iri);

    // Folder
    add_iri_triple(&mut triples, &s, &vocab.term("folder"), folder_iri_str, graph_iri);

    // Source Path
    add_literal_triple(&mut triples, &s, &vocab.term("sourcePath"), source_path, graph_iri);

    // Message size (raw RFC822 bytes)
    add_typed_literal_triple(&mut triples, &s, &vocab.term("size"), &raw_message.len().to_string(), vocab::XSD_INTEGER, graph_iri);

    // Subject
    if let Some(subject) = msg.subject() {
        add_literal_triple(&mut triples, &s, &vocab.term("subject"), subject, graph_iri);
        add_literal_triple(&mut triples, &s, &vocab.term("normalizedSubject"), &normalize_subject(subject), graph_iri);
    }

    // Message-ID
    if let Some(id) = msg_id {
        add_literal_triple(&mut triples, &s, &vocab.term("messageId"), id, graph_iri);
        add_iri_triple(&mut triples, &s, &vocab.term("mid"), &vocab.mid_iri(id), graph_iri);
    }

    // In-Reply-To
    match msg.in_reply_to() {
        HeaderValue::Text(parent_id) => {
            let parent_iri = vocab.message_iri(Some(parent_id.as_ref()), &[]);
            add_iri_triple(&mut triples, &s, &vocab.term("inReplyTo"), &parent_iri, graph_iri);
        }
        HeaderValue::TextList(ids) => {
            for id in ids {
                let parent_iri = vocab.message_iri(Some(id.as_ref()), &[]);
                add_iri_triple(&mut triples, &s, &vocab.term("inReplyTo"), &parent_iri, graph_iri);
            }
        }
        _ => {}
    }

    // References + Thread computation
    let thread_root_id: Option<String>;
    match msg.references() {
        HeaderValue::Text(id) => {
            let ref_iri = vocab.message_iri(Some(id.as_ref()), &[]);
            add_iri_triple(&mut triples, &s, &vocab.term("references"), &ref_iri, graph_iri);
            thread_root_id = Some(id.to_string());
        }
        HeaderValue::TextList(ids) => {
            for id in ids {
                let ref_iri = vocab.message_iri(Some(id.as_ref()), &[]);
                add_iri_triple(&mut triples, &s, &vocab.term("references"), &ref_iri, graph_iri);
            }
            // First entry in References is the thread root
            thread_root_id = ids.first().map(|id| id.to_string());
        }
        _ => {
            thread_root_id = None;
        }
    }

    // Thread: use first References entry as root, otherwise own message-id
    let effective_thread_root = thread_root_id
        .as_deref()
        .or(msg_id)
        .unwrap_or("");
    if !effective_thread_root.is_empty() {
        let thread_iri = vocab.thread_iri(effective_thread_root);
        add_iri_triple(&mut triples, &s, &vocab.term("thread"), &thread_iri, graph_iri);
        add_iri_triple(&mut triples, &thread_iri, vocab::RDF_TYPE, &vocab.term("Thread"), graph_iri);
    }

    // Mailing List Awareness
    if let HeaderValue::Text(list_id) = msg.list_id() {
        let list_iri = vocab.list_iri(list_id.as_ref());
        add_iri_triple(&mut triples, &s, &vocab.term("belongsToList"), &list_iri, graph_iri);
        add_iri_triple(&mut triples, &list_iri, vocab::RDF_TYPE, &vocab.term("MailingList"), graph_iri);
        add_literal_triple(&mut triples, &list_iri, &vocab.term("listId"), list_id.as_ref(), graph_iri);
        
        if let HeaderValue::Text(archive) = msg.list_archive() {
            add_literal_triple(&mut triples, &list_iri, &vocab.term("listArchive"), archive.as_ref(), graph_iri);
        }
    }

    // Date → schema:dateCreated
    if let Some(date) = msg.date() {
        let tz = if date.tz_hour == 0 && date.tz_minute == 0 {
            "Z".to_string()
        } else {
            let sign = if date.tz_before_gmt { "-" } else { "+" };
            format!("{}{:02}:{:02}", sign, date.tz_hour, date.tz_minute)
        };
        let date_str = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}", 
            date.year, date.month, date.day, date.hour, date.minute, date.second, tz);
        add_typed_literal_triple(&mut triples, &s, &vocab.schema_term("dateCreated"), &date_str, vocab::XSD_DATETIME, graph_iri);
    }

    // schema:dateReceived (best-effort from Received header)
    if let Some(HeaderValue::Received(received)) = msg.header_values("Received").next() {
        if let Some(recv_date) = &received.date {
            let tz = if recv_date.tz_hour == 0 && recv_date.tz_minute == 0 {
                "Z".to_string()
            } else {
                let sign = if recv_date.tz_before_gmt { "-" } else { "+" };
                format!("{}{:02}:{:02}", sign, recv_date.tz_hour, recv_date.tz_minute)
            };
            let recv_date_str = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}",
                recv_date.year, recv_date.month, recv_date.day, recv_date.hour, recv_date.minute, recv_date.second, tz);
            add_typed_literal_triple(&mut triples, &s, &vocab.schema_term("dateReceived"), &recv_date_str, vocab::XSD_DATETIME, graph_iri);
        }
    }

    // Reply-To
    if let Some(HeaderValue::Address(reply_to_addr)) = msg.header_values("Reply-To").next() {
        match reply_to_addr {
            Address::List(list) => {
                for a in list {
                    if let Some(email) = &a.address {
                        let account_iri = vocab.account_iri(email);
                        add_iri_triple(&mut triples, &s, &vocab.term("replyTo"), &account_iri, graph_iri);
                    }
                }
            }
            Address::Group(groups) => {
                for g in groups {
                    for a in &g.addresses {
                        if let Some(email) = &a.address {
                            let account_iri = vocab.account_iri(email);
                            add_iri_triple(&mut triples, &s, &vocab.term("replyTo"), &account_iri, graph_iri);
                        }
                    }
                }
            }
        }
    }

    // User-Agent / X-Mailer
    if let Some(HeaderValue::Text(ua)) = msg.header_values("User-Agent").next() {
        add_literal_triple(&mut triples, &s, &vocab.term("userAgent"), ua.as_ref(), graph_iri);
    } else if let Some(HeaderValue::Text(ua)) = msg.header_values("X-Mailer").next() {
        add_literal_triple(&mut triples, &s, &vocab.term("userAgent"), ua.as_ref(), graph_iri);
    }

    // Addresses (mail:Account with mailto: URIs)
    process_addresses(&mut triples, &s, &vocab.term("from"), msg.from(), vocab, graph_iri);
    process_addresses(&mut triples, &s, &vocab.term("to"), msg.to(), vocab, graph_iri);
    process_addresses(&mut triples, &s, &vocab.term("cc"), msg.cc(), vocab, graph_iri);
    process_addresses(&mut triples, &s, &vocab.term("bcc"), msg.bcc(), vocab, graph_iri);

    // Body & Link Extraction
    if include_body {
        if let Some(text) = msg.body_text(0) {
            add_literal_triple(&mut triples, &s, &vocab.term("bodyText"), &text, graph_iri);
        }
        if let Some(part) = msg.html_part(0) {
            if matches!(&part.body, mail_parser::PartType::Html(_)) {
                if let Some(html) = msg.body_html(0) {
                    add_literal_triple(&mut triples, &s, &vocab.term("bodyHtml"), &html, graph_iri);
                }
            }
        }
    }

    // Attachments (schema:MediaObject)
    let mut attachment_count = 0u32;
    for att in msg.attachments() {
        let mut hasher = Sha256::new();
        let content = match &att.body {
            mail_parser::PartType::Binary(b) => b.as_ref(),
            mail_parser::PartType::InlineBinary(b) => b.as_ref(),
            mail_parser::PartType::Text(t) => t.as_bytes(),
            mail_parser::PartType::Html(h) => h.as_bytes(),
            _ => &[],
        };
        hasher.update(content);
        let hash = format!("{:x}", hasher.finalize());
        let content_size = content.len();
        
        let att_iri = vocab.attachment_iri(&hash);
        add_iri_triple(&mut triples, &s, &vocab.schema_term("associatedMedia"), &att_iri, graph_iri);
        add_iri_triple(&mut triples, &att_iri, vocab::RDF_TYPE, &vocab.schema_term("MediaObject"), graph_iri);
        add_literal_triple(&mut triples, &att_iri, &vocab.schema_term("sha256"), &hash, graph_iri);
        
        if let Some(filename) = att.attachment_name() {
             add_literal_triple(&mut triples, &att_iri, &vocab.schema_term("name"), filename, graph_iri);
        }
        
        let ctype = att.content_type().map(|c| c.ctype()).unwrap_or("application/octet-stream");
        add_literal_triple(&mut triples, &att_iri, &vocab.schema_term("encodingFormat"), ctype, graph_iri);
        add_typed_literal_triple(&mut triples, &att_iri, &vocab.schema_term("contentSize"), &content_size.to_string(), vocab::XSD_INTEGER, graph_iri);

        if include_attachments {
            let under_cap = max_attachment_size.map_or(true, |max| content_size <= max);
            if under_cap {
                if ctype.starts_with("text/") {
                    if let Ok(text) = std::str::from_utf8(content) {
                        add_literal_triple(&mut triples, &att_iri, &vocab.schema_term("text"), text, graph_iri);
                    }
                } else {
                    let b64 = base64::engine::general_purpose::STANDARD.encode(content);
                    let data_uri = format!("data:{};base64,{}", ctype, b64);
                    add_literal_triple(&mut triples, &att_iri, &vocab.schema_term("contentUrl"), &data_uri, graph_iri);
                }
            }
        }

        attachment_count += 1;
    }

    // Attachment count
    if attachment_count > 0 {
        add_typed_literal_triple(&mut triples, &s, &vocab.term("attachmentCount"), &attachment_count.to_string(), vocab::XSD_INTEGER, graph_iri);
    }

    // Serialize all triples/quads for this message
    for t in triples {
        writeln!(writer, "{}", t)?;
    }

    Ok(())
}

fn add_iri_triple(triples: &mut Vec<String>, s: &str, p: &str, o: &str, graph_iri: Option<&str>) {
    if let Some(g) = graph_iri {
        triples.push(format!("<{}> <{}> <{}> <{}> .", s, p, o, g));
    } else {
        triples.push(format!("<{}> <{}> <{}> .", s, p, o));
    }
}

fn add_literal_triple(triples: &mut Vec<String>, s: &str, p: &str, o: &str, graph_iri: Option<&str>) {
    if let Some(g) = graph_iri {
        triples.push(format!("<{}> <{}> \"{}\" <{}> .", s, p, escape_nt(o), g));
    } else {
        triples.push(format!("<{}> <{}> \"{}\" .", s, p, escape_nt(o)));
    }
}

fn add_typed_literal_triple(triples: &mut Vec<String>, s: &str, p: &str, o: &str, t: &str, graph_iri: Option<&str>) {
    if let Some(g) = graph_iri {
        triples.push(format!("<{}> <{}> \"{}\"^^<{}> <{}> .", s, p, escape_nt(o), t, g));
    } else {
        triples.push(format!("<{}> <{}> \"{}\"^^<{}> .", s, p, escape_nt(o), t));
    }
}

fn escape_nt(s: &str) -> String {
    s.replace('\\', "\\\\")
     .replace('\"', "\\\"")
     .replace('\n', "\\n")
     .replace('\r', "\\r")
     .replace('\t', "\\t")
}

#[allow(dead_code)]
fn sanitize_iri(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            ' ' | '<' | '>' | '"' | '{' | '}' | '|' | '^' | '\\' | '`' | '@' | '[' | ']' => {
                result.push_str(&format!("%{:02X}", c as u32));
            }
            _ if c.is_control() => {
                result.push_str(&format!("%{:02X}", c as u32));
            }
            _ => result.push(c),
        }
    }
    result
}

fn process_addresses(
    triples: &mut Vec<String>,
    msg_s: &str,
    predicate: &str,
    addr_value: Option<&Address>,
    vocab: &Vocab,
    graph_iri: Option<&str>,
) {
    if let Some(addr) = addr_value {
        match addr {
            Address::List(list) => {
                for a in list {
                    add_single_addr(triples, msg_s, predicate, a, vocab, graph_iri);
                }
            }
            Address::Group(groups) => {
                for g in groups {
                    for a in &g.addresses {
                        add_single_addr(triples, msg_s, predicate, a, vocab, graph_iri);
                    }
                }
            }
        }
    }
}

fn add_single_addr(
    triples: &mut Vec<String>,
    msg_s: &str,
    predicate: &str,
    addr: &Addr,
    vocab: &Vocab,
    graph_iri: Option<&str>,
) {
    if let Some(email) = &addr.address {
        let account_iri = vocab.account_iri(email);
        
        add_iri_triple(triples, msg_s, predicate, &account_iri, graph_iri);
        add_iri_triple(triples, &account_iri, vocab::RDF_TYPE, &vocab.term("Account"), graph_iri);
        
        if let Some(name) = &addr.name {
            add_literal_triple(triples, &account_iri, &vocab.schema_term("name"), name, graph_iri);
        }
    }
}

fn normalize_subject(subject: &str) -> String {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"(?i)^((Re|Fwd|Aw|Antw|Vs):[ \t]*)+").unwrap());
    re.replace(subject, "").trim().to_string()
}
