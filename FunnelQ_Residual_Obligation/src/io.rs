use crate::model::{CanonicalEvent, Scalar};
use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

const COMPACT_RECORD_BYTES: usize = 37;

fn u32_at(raw: &[u8]) -> u32 {
    u32::from_le_bytes(raw.try_into().expect("fixed u32 field"))
}

fn u64_at(raw: &[u8]) -> u64 {
    u64::from_le_bytes(raw.try_into().expect("fixed u64 field"))
}

pub fn load_compact_artifact(path: &Path) -> Result<Vec<CanonicalEvent>> {
    let mut raw = Vec::new();
    BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?)
        .read_to_end(&mut raw)?;
    if raw.is_empty() || raw.len() % COMPACT_RECORD_BYTES != 0 {
        bail!("canonical artifact length must be a positive multiple of {COMPACT_RECORD_BYTES}")
    }
    let mut events = Vec::with_capacity(raw.len() / COMPACT_RECORD_BYTES);
    for row in raw.chunks_exact(COMPACT_RECORD_BYTES) {
        let order = u64_at(&row[0..8]);
        let timestamp = u64_at(&row[8..16]);
        let src = u32_at(&row[16..20]);
        let dst = u32_at(&row[20..24]);
        let nft = u32_at(&row[24..28]);
        let collection = u32_at(&row[28..32]);
        let tx = u32_at(&row[32..36]);
        let event_type = match row[36] {
            1 => "NFTTrade",
            2 => "NFTTransfer",
            other => bail!("unknown compact NFT event type id {other}"),
        };
        let mut fields = BTreeMap::new();
        fields.insert("move_from".to_owned(), Scalar::Int(src as i64));
        fields.insert("move_to".to_owned(), Scalar::Int(dst as i64));
        fields.insert("nft".to_owned(), Scalar::Int(nft as i64));
        fields.insert(
            "collection_contract".to_owned(),
            Scalar::Int(collection as i64),
        );
        events.push(CanonicalEvent::new(
            format!("order:{order}"),
            event_type,
            i64::try_from(order).context("event order exceeds i64")?,
            i64::try_from(timestamp).context("event timestamp exceeds i64")?,
            format!("tx:{tx}"),
            fields,
        ));
    }
    Ok(events)
}

pub fn load_events_json(path: &Path) -> Result<Vec<CanonicalEvent>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let first = reader
        .fill_buf()?
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace());
    if first == Some(b'[') {
        let values: Vec<Value> = serde_json::from_reader(reader)?;
        values.iter().map(CanonicalEvent::from_json).collect()
    } else {
        reader
            .lines()
            .filter_map(|line| match line {
                Ok(line) if line.trim().is_empty() => None,
                other => Some(other),
            })
            .map(|line| {
                let value: Value = serde_json::from_str(&line?)?;
                CanonicalEvent::from_json(&value)
            })
            .collect()
    }
}
