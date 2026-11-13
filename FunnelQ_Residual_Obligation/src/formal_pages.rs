                                                                                
   
                                                                                
                                                                          
                                                                                
                                                                            

use crate::evaluator::{evaluate_query, EvaluationOutput, EvaluationRequest};
use crate::index::QueryIndependentIndex;
use crate::model::{CanonicalEvent, Scalar};
use crate::query::{QuerySpec, ScopeSpec};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

pub const FORMAL_RECORD_BYTES: usize = 37;
pub const FORMAL_PAGE_ROWS: usize = 16;
pub const FORMAL_NFT_PAGE_ROOT: &str =
    "0x47271ed382f67dd56c0b414625cfb7bdb0bdd3d899eac3f4b313c2730dca6518";

const FORMAL_PROTOCOL: &str = "funnelq-csr-formal-v3";
const MANIFEST_DOMAIN: &[u8] = b"funnelq-csr-formal-v3-manifest\0";
const PAGE_LEAF_DOMAIN: &[u8] = b"funnelq-csr-v2-event-page\0";
const PAGE_PARENT_DOMAIN: &[u8] = b"funnelq-csr-v2-page-parent\0";

type Hash32 = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompactEventRecord {
    order: u64,
    timestamp: u64,
    src: u32,
    dst: u32,
    nft: u32,
    collection: u32,
    tx: u32,
    event_type: u8,
}

impl CompactEventRecord {
    fn decode(raw: &[u8]) -> Result<Self> {
        if raw.len() != FORMAL_RECORD_BYTES {
            bail!("formal canonical event record must contain 37 bytes")
        }
        let event_type = raw[36];
        if !matches!(event_type, 1 | 2) {
            bail!("unknown compact NFT event type id {event_type}")
        }
        Ok(Self {
            order: u64::from_le_bytes(raw[0..8].try_into().expect("fixed u64 field")),
            timestamp: u64::from_le_bytes(raw[8..16].try_into().expect("fixed u64 field")),
            src: u32::from_le_bytes(raw[16..20].try_into().expect("fixed u32 field")),
            dst: u32::from_le_bytes(raw[20..24].try_into().expect("fixed u32 field")),
            nft: u32::from_le_bytes(raw[24..28].try_into().expect("fixed u32 field")),
            collection: u32::from_le_bytes(raw[28..32].try_into().expect("fixed u32 field")),
            tx: u32::from_le_bytes(raw[32..36].try_into().expect("fixed u32 field")),
            event_type,
        })
    }

    fn to_canonical(&self) -> Result<CanonicalEvent> {
        let mut fields = BTreeMap::new();
        fields.insert("move_from".to_owned(), Scalar::Int(i64::from(self.src)));
        fields.insert("move_to".to_owned(), Scalar::Int(i64::from(self.dst)));
        fields.insert("nft".to_owned(), Scalar::Int(i64::from(self.nft)));
        fields.insert(
            "collection_contract".to_owned(),
            Scalar::Int(i64::from(self.collection)),
        );
        Ok(CanonicalEvent::new(
            format!("order:{}", self.order),
            if self.event_type == 1 {
                "NFTTrade"
            } else {
                "NFTTransfer"
            },
            i64::try_from(self.order).context("event order exceeds i64")?,
            i64::try_from(self.timestamp).context("event timestamp exceeds i64")?,
            format!("tx:{}", self.tx),
            fields,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormalEventPageManifest {
    pub manifest_root: String,
    pub artifact_sha256: String,
    pub record_size_bytes: usize,
    pub record_count: usize,
    pub page_rows: usize,
    pub page_count: usize,
    pub page_root: String,
    pub artifact_path: PathBuf,
    pub tree_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormalMerkleProofNode {
    pub level: usize,
    pub index: usize,
    pub hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormalOpenedPage {
    pub page_id: usize,
    pub row_count: usize,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormalEpisodeOpening {
    pub version: u32,
    pub page_root: String,
    pub record_size_bytes: usize,
    pub record_count: usize,
    pub page_rows: usize,
    pub page_count: usize,
    pub anchor_event_id: String,
    pub anchor_record_index: usize,
    pub anchor_order: u64,
    pub anchor_time: u64,
    pub anchor_collection: u32,
    pub window_seconds: i64,
    pub semantic_record_start: usize,
    pub semantic_record_end: usize,
    pub opened_page_start: usize,
    pub opened_page_end: usize,
    pub pages: Vec<FormalOpenedPage>,
    pub proof_nodes: Vec<FormalMerkleProofNode>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedFormalEpisode {
    pub opened_events: Vec<CanonicalEvent>,
    pub semantic_events: Vec<CanonicalEvent>,
    pub anchor_event_id: String,
    pub page_root: String,
}

                                                                           
#[derive(Clone, Debug)]
pub struct FormalEventPageIndex {
    manifest: FormalEventPageManifest,
    raw: Vec<u8>,
    levels: Vec<Vec<Hash32>>,
                                                                           
                                                                             
    order_positions: Vec<(u64, u32)>,
}

fn normalize_root(raw: &str) -> Result<String> {
    let value = raw.strip_prefix("0x").unwrap_or(raw);
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("formal Merkle root must be a 32-byte hexadecimal digest")
    }
    Ok(format!("0x{}", value.to_ascii_lowercase()))
}

fn hash_to_hex(hash: &Hash32) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in hash {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hash_from_hex(raw: &str) -> Result<Hash32> {
    let value = raw.strip_prefix("0x").unwrap_or(raw);
    if value.len() != 64 {
        bail!("Merkle hash must contain exactly 64 hexadecimal characters")
    }
    fn nibble(value: u8) -> Option<u8> {
        match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            b'A'..=b'F' => Some(value - b'A' + 10),
            _ => None,
        }
    }
    let bytes = value.as_bytes();
    let mut output = [0u8; 32];
    for (index, slot) in output.iter_mut().enumerate() {
        let high = nibble(bytes[index * 2]).context("invalid Merkle hash")?;
        let low = nibble(bytes[index * 2 + 1]).context("invalid Merkle hash")?;
        *slot = (high << 4) | low;
    }
    Ok(output)
}

fn page_leaf_hash(page_id: usize, row_count: usize, payload: &[u8]) -> Result<Hash32> {
    let page_id = u32::try_from(page_id).context("page id exceeds formal u32 wire type")?;
    let row_count = u32::try_from(row_count).context("row count exceeds formal u32 wire type")?;
    let mut hasher = Sha256::new();
    hasher.update(PAGE_LEAF_DOMAIN);
    hasher.update(page_id.to_le_bytes());
    hasher.update(row_count.to_le_bytes());
    hasher.update(payload);
    Ok(hasher.finalize().into())
}

fn parent_hash(left: &Hash32, right: &Hash32) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(PAGE_PARENT_DOMAIN);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

#[cfg(test)]
fn build_levels(leaves: Vec<Hash32>) -> Result<Vec<Vec<Hash32>>> {
    if leaves.is_empty() {
        bail!("formal event page tree cannot be empty")
    }
    let mut levels = vec![leaves];
    while levels.last().is_some_and(|level| level.len() > 1) {
        let current = levels.last().expect("nonempty level");
        let next = current
            .chunks(2)
            .map(|pair| parent_hash(&pair[0], pair.get(1).unwrap_or(&pair[0])))
            .collect();
        levels.push(next);
    }
    Ok(levels)
}

fn parse_tree(path: &Path) -> Result<Vec<Vec<Hash32>>> {
    let document: Value = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("read formal page tree {}", path.display()))?,
    )?;
    if document.get("protocol").and_then(Value::as_str) != Some(FORMAL_PROTOCOL) {
        bail!("formal page tree protocol mismatch")
    }
    let raw_levels = document
        .get("levels")
        .and_then(Value::as_array)
        .context("formal page tree has no levels")?;
    let mut levels = Vec::with_capacity(raw_levels.len());
    for raw_level in raw_levels {
        let hashes = raw_level
            .as_array()
            .context("formal page tree level is not an array")?
            .iter()
            .map(|value| {
                hash_from_hex(
                    value
                        .as_str()
                        .context("formal page tree hash is not a string")?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if hashes.is_empty() {
            bail!("formal page tree contains an empty level")
        }
        levels.push(hashes);
    }
    if levels.last().map(Vec::len) != Some(1) {
        bail!("formal page tree has no singleton root")
    }
    for pair in levels.windows(2) {
        let expected_width = pair[0].len().div_ceil(2);
        if pair[1].len() != expected_width {
            bail!("formal page tree level width is inconsistent")
        }
        for (index, children) in pair[0].chunks(2).enumerate() {
            let expected = parent_hash(&children[0], children.get(1).unwrap_or(&children[0]));
            if pair[1][index] != expected {
                bail!("formal page tree has an inconsistent parent hash")
            }
        }
    }
    Ok(levels)
}

fn manifest_string<'a>(object: &'a Value, key: &str) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("formal Manifest field {key} is missing"))
}

fn manifest_usize(object: &Value, key: &str) -> Result<usize> {
    let value = object
        .get(key)
        .and_then(Value::as_u64)
        .with_context(|| format!("formal Manifest field {key} is missing"))?;
    usize::try_from(value).with_context(|| format!("formal Manifest field {key} exceeds usize"))
}

impl FormalEventPageManifest {
    pub fn load(manifest_path: &Path) -> Result<Self> {
        let document: Value = serde_json::from_slice(
            &fs::read(manifest_path)
                .with_context(|| format!("read formal Manifest {}", manifest_path.display()))?,
        )?;
        let core = document
            .get("manifest_core")
            .context("formal Manifest has no manifest_core")?;
        if core.get("protocol").and_then(Value::as_str) != Some(FORMAL_PROTOCOL) {
            bail!("formal Manifest protocol mismatch")
        }
        let mut hasher = Sha256::new();
        hasher.update(MANIFEST_DOMAIN);
        hasher.update(serde_json::to_vec(core)?);
        let calculated_manifest_root = format!("0x{:x}", hasher.finalize());
        let manifest_root = normalize_root(manifest_string(&document, "manifest_root")?)?;
        if calculated_manifest_root != manifest_root {
            bail!("formal Manifest wrapper digest mismatch")
        }
        let pages = core
            .get("canonical_event_pages")
            .context("formal Manifest has no canonical_event_pages")?;
        let record_size_bytes = manifest_usize(pages, "record_size_bytes")?;
        let record_count = manifest_usize(pages, "record_count")?;
        let page_rows = manifest_usize(pages, "page_rows")?;
        let page_count = manifest_usize(pages, "page_count")?;
        if record_size_bytes != FORMAL_RECORD_BYTES || page_rows != FORMAL_PAGE_ROWS {
            bail!("formal NFT event layout must be 37-byte records in 16-row pages")
        }
        if record_count == 0 || page_count != record_count.div_ceil(page_rows) {
            bail!("formal Manifest event page cardinality is inconsistent")
        }
        let sort_key = pages
            .get("sort_key")
            .and_then(Value::as_array)
            .context("formal Manifest has no canonical event sort key")?
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        if sort_key != ["collection", "order"] {
            bail!("formal event artifact is not ordered by collection and order")
        }
        let directory = manifest_path
            .parent()
            .context("formal Manifest path has no parent")?;
        let artifact_path = directory.join(manifest_string(pages, "artifact")?);
        let tree_name = document
            .get("build")
            .and_then(|value| value.get("page_tree_artifact"))
            .and_then(Value::as_str)
            .unwrap_or("canonical_event_page_tree_v3.json");
        Ok(Self {
            manifest_root,
            artifact_sha256: manifest_string(pages, "sha256")?.to_ascii_lowercase(),
            record_size_bytes,
            record_count,
            page_rows,
            page_count,
            page_root: normalize_root(manifest_string(pages, "page_root")?)?,
            artifact_path,
            tree_path: directory.join(tree_name),
        })
    }
}

impl FormalEventPageIndex {
                                                                               
                                                                           
                                                                 
    pub fn load(manifest_path: &Path) -> Result<Self> {
        let manifest = FormalEventPageManifest::load(manifest_path)?;
        let raw = fs::read(&manifest.artifact_path).with_context(|| {
            format!(
                "read formal canonical event artifact {}",
                manifest.artifact_path.display()
            )
        })?;
        let expected_bytes = manifest
            .record_count
            .checked_mul(manifest.record_size_bytes)
            .context("formal event artifact byte length overflows usize")?;
        if raw.len() != expected_bytes {
            bail!("formal canonical event artifact is truncated or has trailing bytes")
        }
        let artifact_digest = format!("{:x}", Sha256::digest(&raw));
        if artifact_digest != manifest.artifact_sha256 {
            bail!("formal canonical event artifact SHA-256 mismatch")
        }
        let levels = parse_tree(&manifest.tree_path)?;
        if levels[0].len() != manifest.page_count
            || normalize_root(&format!("0x{}", hash_to_hex(&levels.last().unwrap()[0])))?
                != manifest.page_root
        {
            bail!("formal page tree root/cardinality differs from the Manifest")
        }

        for page_id in 0..manifest.page_count {
            let row_start = page_id * manifest.page_rows;
            let row_count = manifest.page_rows.min(manifest.record_count - row_start);
            let byte_start = row_start * manifest.record_size_bytes;
            let byte_end = byte_start + row_count * manifest.record_size_bytes;
            if page_leaf_hash(page_id, row_count, &raw[byte_start..byte_end])? != levels[0][page_id]
            {
                bail!("formal event page {page_id} differs from its committed leaf")
            }
        }

        let mut order_positions = Vec::with_capacity(manifest.record_count);
        let mut previous: Option<CompactEventRecord> = None;
        for (position, row) in raw.chunks_exact(FORMAL_RECORD_BYTES).enumerate() {
            let record = CompactEventRecord::decode(row)?;
            if let Some(prior) = &previous {
                if (record.collection, record.order) <= (prior.collection, prior.order) {
                    bail!("formal canonical events are not strictly collection-order sorted")
                }
                if record.collection == prior.collection && record.timestamp < prior.timestamp {
                    bail!("formal event timestamps decrease inside a collection")
                }
            }
            order_positions.push((
                record.order,
                u32::try_from(position).context("formal event position exceeds u32")?,
            ));
            previous = Some(record);
        }
        order_positions.sort_unstable_by_key(|entry| entry.0);
        if order_positions
            .windows(2)
            .any(|pair| pair[0].0 >= pair[1].0)
        {
            bail!("formal canonical artifact contains duplicate event orders")
        }
        Ok(Self {
            manifest,
            raw,
            levels,
            order_positions,
        })
    }

    pub fn manifest(&self) -> &FormalEventPageManifest {
        &self.manifest
    }

    pub fn page_root(&self) -> &str {
        &self.manifest.page_root
    }

    fn record(&self, position: usize) -> Result<CompactEventRecord> {
        if position >= self.manifest.record_count {
            bail!("formal event position lies outside the epoch")
        }
        let start = position * FORMAL_RECORD_BYTES;
        CompactEventRecord::decode(&self.raw[start..start + FORMAL_RECORD_BYTES])
    }

    fn anchor_position(&self, event_id: &str) -> Result<usize> {
        let order = event_id
            .strip_prefix("order:")
            .context("formal NFT event id must use the order:<u64> form")?
            .parse::<u64>()
            .context("formal NFT event id has an invalid order")?;
        let offset = self
            .order_positions
            .binary_search_by_key(&order, |entry| entry.0)
            .map_err(|_| anyhow::anyhow!("unknown formal Episode anchor {event_id}"))?;
        Ok(self.order_positions[offset].1 as usize)
    }

    fn derive_multiproof(
        &self,
        opened_page_start: usize,
        opened_page_end: usize,
    ) -> Result<Vec<FormalMerkleProofNode>> {
        if opened_page_start >= opened_page_end || opened_page_end > self.manifest.page_count {
            bail!("invalid formal page opening")
        }
        let mut selected = (opened_page_start..opened_page_end).collect::<BTreeSet<_>>();
        let mut nodes = BTreeMap::new();
        for (level, values) in self.levels.iter().enumerate().take(self.levels.len() - 1) {
            let mut parents = BTreeSet::new();
            for index in &selected {
                let sibling = index ^ 1;
                if sibling < values.len() && !selected.contains(&sibling) {
                    nodes.insert(
                        (level, sibling),
                        FormalMerkleProofNode {
                            level,
                            index: sibling,
                            hash: hash_to_hex(&values[sibling]),
                        },
                    );
                }
                parents.insert(index / 2);
            }
            selected = parents;
        }
        Ok(nodes.into_values().collect())
    }

                                                                           
                                                                              
                                                                               
    pub fn open_episode(
        &self,
        anchor_event_id: &str,
        window_seconds: i64,
    ) -> Result<FormalEpisodeOpening> {
        if window_seconds <= 0 {
            bail!("Episode window must be positive")
        }
        let anchor_record_index = self.anchor_position(anchor_event_id)?;
        let anchor = self.record(anchor_record_index)?;
        let upper_time = anchor
            .timestamp
            .saturating_add(u64::try_from(window_seconds).expect("positive window"));

        let mut collection_end_low = anchor_record_index + 1;
        let mut collection_end_high = self.manifest.record_count;
        while collection_end_low < collection_end_high {
            let middle = collection_end_low + (collection_end_high - collection_end_low) / 2;
            if self.record(middle)?.collection <= anchor.collection {
                collection_end_low = middle + 1;
            } else {
                collection_end_high = middle;
            }
        }
        let collection_end = collection_end_low;
        let mut semantic_end_low = anchor_record_index + 1;
        let mut semantic_end_high = collection_end;
        while semantic_end_low < semantic_end_high {
            let middle = semantic_end_low + (semantic_end_high - semantic_end_low) / 2;
            if self.record(middle)?.timestamp <= upper_time {
                semantic_end_low = middle + 1;
            } else {
                semantic_end_high = middle;
            }
        }
        let semantic_record_start = anchor_record_index;
        let semantic_record_end = semantic_end_low;

        let left_boundary = semantic_record_start.saturating_sub(1);
        let right_boundary = if semantic_record_end < self.manifest.record_count {
            semantic_record_end
        } else {
            self.manifest.record_count - 1
        };
        let opened_page_start = left_boundary / self.manifest.page_rows;
        let opened_page_end = right_boundary / self.manifest.page_rows + 1;
        let mut pages = Vec::with_capacity(opened_page_end - opened_page_start);
        for page_id in opened_page_start..opened_page_end {
            let row_start = page_id * self.manifest.page_rows;
            let row_count = self
                .manifest
                .page_rows
                .min(self.manifest.record_count - row_start);
            let byte_start = row_start * self.manifest.record_size_bytes;
            let byte_end = byte_start + row_count * self.manifest.record_size_bytes;
            pages.push(FormalOpenedPage {
                page_id,
                row_count,
                payload: self.raw[byte_start..byte_end].to_vec(),
            });
        }
        Ok(FormalEpisodeOpening {
            version: 1,
            page_root: self.manifest.page_root.clone(),
            record_size_bytes: self.manifest.record_size_bytes,
            record_count: self.manifest.record_count,
            page_rows: self.manifest.page_rows,
            page_count: self.manifest.page_count,
            anchor_event_id: anchor_event_id.to_owned(),
            anchor_record_index,
            anchor_order: anchor.order,
            anchor_time: anchor.timestamp,
            anchor_collection: anchor.collection,
            window_seconds,
            semantic_record_start,
            semantic_record_end,
            opened_page_start,
            opened_page_end,
            pages,
            proof_nodes: self.derive_multiproof(opened_page_start, opened_page_end)?,
        })
    }
}

fn verify_multiproof(opening: &FormalEpisodeOpening) -> Result<Hash32> {
    let mut current = BTreeMap::new();
    for page in &opening.pages {
        if current
            .insert(
                page.page_id,
                page_leaf_hash(page.page_id, page.row_count, &page.payload)?,
            )
            .is_some()
        {
            bail!("formal Episode repeats an opened page")
        }
    }
    let mut proof = BTreeMap::new();
    for node in &opening.proof_nodes {
        if proof
            .insert((node.level, node.index), hash_from_hex(&node.hash)?)
            .is_some()
        {
            bail!("formal Episode multiproof repeats a node")
        }
    }
    let mut width = opening.page_count;
    let mut level = 0usize;
    while width > 1 {
        let parents = current
            .keys()
            .map(|index| index / 2)
            .collect::<BTreeSet<_>>();
        let mut next = BTreeMap::new();
        for parent in parents {
            let left_index = parent * 2;
            let right_index = left_index + 1;
            let left = if let Some(value) = current.get(&left_index) {
                *value
            } else {
                proof
                    .remove(&(level, left_index))
                    .context("formal Episode multiproof omits a left sibling")?
            };
            let right = if right_index >= width {
                left
            } else if let Some(value) = current.get(&right_index) {
                *value
            } else {
                proof
                    .remove(&(level, right_index))
                    .context("formal Episode multiproof omits a right sibling")?
            };
            next.insert(parent, parent_hash(&left, &right));
        }
        current = next;
        width = width.div_ceil(2);
        level += 1;
    }
    if !proof.is_empty() || current.len() != 1 || !current.contains_key(&0) {
        bail!("formal Episode multiproof is incomplete or contains unused nodes")
    }
    Ok(current[&0])
}

fn opened_records(opening: &FormalEpisodeOpening) -> Result<BTreeMap<usize, CompactEventRecord>> {
    let mut records = BTreeMap::new();
    for page in &opening.pages {
        let expected_rows = opening
            .page_rows
            .min(opening.record_count - page.page_id * opening.page_rows);
        if page.row_count != expected_rows
            || page.payload.len() != page.row_count * opening.record_size_bytes
        {
            bail!("formal Episode contains a truncated page")
        }
        for (offset, row) in page.payload.chunks_exact(FORMAL_RECORD_BYTES).enumerate() {
            let position = page.page_id * opening.page_rows + offset;
            if records
                .insert(position, CompactEventRecord::decode(row)?)
                .is_some()
            {
                bail!("formal Episode repeats an event position")
            }
        }
    }
    Ok(records)
}

                                                                                 
                                                                                
                                           
pub fn verify_formal_episode_opening(
    trusted_page_root: &str,
    expected_anchor_event_id: &str,
    expected_window_seconds: i64,
    opening: &FormalEpisodeOpening,
) -> Result<VerifiedFormalEpisode> {
    let trusted_page_root = normalize_root(trusted_page_root)?;
    if opening.version != 1
        || opening.record_size_bytes != FORMAL_RECORD_BYTES
        || opening.page_rows != FORMAL_PAGE_ROWS
        || opening.record_count == 0
        || opening.page_count != opening.record_count.div_ceil(opening.page_rows)
        || opening.opened_page_start >= opening.opened_page_end
        || opening.opened_page_end > opening.page_count
        || opening.pages.len() != opening.opened_page_end - opening.opened_page_start
    {
        bail!("malformed formal Episode metadata")
    }
    if normalize_root(&opening.page_root)? != trusted_page_root {
        bail!("formal Episode claims a different page root")
    }
    if opening.anchor_event_id != expected_anchor_event_id
        || opening.window_seconds != expected_window_seconds
        || expected_window_seconds <= 0
    {
        bail!("formal Episode is bound to a different anchor or query window")
    }
    for (offset, page) in opening.pages.iter().enumerate() {
        if page.page_id != opening.opened_page_start + offset {
            bail!("formal Episode opened pages are not a complete contiguous range")
        }
    }
    let reconstructed = verify_multiproof(opening)?;
    if format!("0x{}", hash_to_hex(&reconstructed)) != trusted_page_root {
        bail!("formal Episode multiproof does not reconstruct the trusted page root")
    }
    let records = opened_records(opening)?;
    if opening.semantic_record_start != opening.anchor_record_index
        || opening.semantic_record_start >= opening.semantic_record_end
        || opening.semantic_record_end > opening.record_count
    {
        bail!("formal Episode semantic interval is malformed")
    }
    let expected_left = opening.semantic_record_start.saturating_sub(1) / opening.page_rows;
    let right_record = if opening.semantic_record_end < opening.record_count {
        opening.semantic_record_end
    } else {
        opening.record_count - 1
    };
    let expected_right = right_record / opening.page_rows + 1;
    if opening.opened_page_start != expected_left || opening.opened_page_end != expected_right {
        bail!("formal Episode does not open both immediate boundary pages")
    }
    let anchor = records
        .get(&opening.anchor_record_index)
        .context("formal Episode omits its authenticated anchor")?;
    if format!("order:{}", anchor.order) != expected_anchor_event_id
        || anchor.order != opening.anchor_order
        || anchor.timestamp != opening.anchor_time
        || anchor.collection != opening.anchor_collection
    {
        bail!("formal Episode anchor metadata was replaced")
    }
    if opening.anchor_record_index > 0 {
        let predecessor = records
            .get(&(opening.anchor_record_index - 1))
            .context("formal Episode omits its left collection/order boundary")?;
        if (predecessor.collection, predecessor.order) >= (anchor.collection, anchor.order) {
            bail!("formal Episode has an invalid left collection/order boundary")
        }
    }
    let upper_time = anchor
        .timestamp
        .saturating_add(u64::try_from(expected_window_seconds).expect("positive window"));
    let mut semantic_records = Vec::new();
    for position in opening.semantic_record_start..opening.semantic_record_end {
        let record = records
            .get(&position)
            .context("formal Episode omits an event inside its semantic interval")?;
        if record.collection != anchor.collection
            || record.order < anchor.order
            || record.timestamp < anchor.timestamp
            || record.timestamp > upper_time
        {
            bail!("formal Episode semantic interval contains an out-of-range event")
        }
        semantic_records.push(record.clone());
    }
    if opening.semantic_record_end < opening.record_count {
        let successor = records
            .get(&opening.semantic_record_end)
            .context("formal Episode omits its right collection/time boundary")?;
        if successor.collection == anchor.collection && successor.timestamp <= upper_time {
            bail!("formal Episode was truncated before the collection/time bound")
        }
    }

    let opened_events = records
        .into_values()
        .map(|record| record.to_canonical())
        .collect::<Result<Vec<_>>>()?;
    let semantic_events = semantic_records
        .iter()
        .map(CompactEventRecord::to_canonical)
        .collect::<Result<Vec<_>>>()?;
    Ok(VerifiedFormalEpisode {
        opened_events,
        semantic_events,
        anchor_event_id: expected_anchor_event_id.to_owned(),
        page_root: trusted_page_root,
    })
}

                                                                        
                                                                              
                                                                                
                                      
pub fn evaluate_formal_episode_opening(
    trusted_page_root: &str,
    query: &QuerySpec,
    fixed_atoms: BTreeMap<String, String>,
    opening: &FormalEpisodeOpening,
) -> Result<EvaluationOutput> {
    let ScopeSpec::Episode {
        anchor_atom,
        window_seconds,
    } = &query.scope
    else {
        bail!("a formal Episode page opening cannot evaluate a non-Episode query")
    };
    let anchor_event_id = fixed_atoms
        .get(anchor_atom)
        .with_context(|| format!("formal Episode query has no fixed {anchor_atom} anchor"))?;
    let verified = verify_formal_episode_opening(
        trusted_page_root,
        anchor_event_id,
        *window_seconds,
        opening,
    )?;
    let allowed_event_ids = verified
        .semantic_events
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<BTreeSet<_>>();
    let replay_index = QueryIndependentIndex::build(verified.opened_events)?;
    evaluate_query(
        &replay_index,
        query,
        &EvaluationRequest {
            allowed_event_ids: Some(allowed_event_ids),
            fixed_atoms,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::compile_query_document;

    fn encode_record(record: &CompactEventRecord) -> Vec<u8> {
        let mut raw = Vec::with_capacity(FORMAL_RECORD_BYTES);
        raw.extend_from_slice(&record.order.to_le_bytes());
        raw.extend_from_slice(&record.timestamp.to_le_bytes());
        raw.extend_from_slice(&record.src.to_le_bytes());
        raw.extend_from_slice(&record.dst.to_le_bytes());
        raw.extend_from_slice(&record.nft.to_le_bytes());
        raw.extend_from_slice(&record.collection.to_le_bytes());
        raw.extend_from_slice(&record.tx.to_le_bytes());
        raw.push(record.event_type);
        raw
    }

    fn synthetic_index() -> FormalEventPageIndex {
        let records = (0..52u64)
            .map(|offset| CompactEventRecord {
                order: 100 + offset,
                timestamp: 1_000 + offset,
                src: offset as u32,
                dst: offset as u32 + 1,
                nft: 9,
                collection: if offset < 44 { 7 } else { 8 },
                tx: offset as u32,
                event_type: if offset % 2 == 0 { 1 } else { 2 },
            })
            .collect::<Vec<_>>();
        let raw = records.iter().flat_map(encode_record).collect::<Vec<_>>();
        let leaves = raw
            .chunks(FORMAL_PAGE_ROWS * FORMAL_RECORD_BYTES)
            .enumerate()
            .map(|(page_id, payload)| {
                page_leaf_hash(page_id, payload.len() / FORMAL_RECORD_BYTES, payload).unwrap()
            })
            .collect::<Vec<_>>();
        let levels = build_levels(leaves).unwrap();
        let page_root = format!("0x{}", hash_to_hex(&levels.last().unwrap()[0]));
        FormalEventPageIndex {
            manifest: FormalEventPageManifest {
                manifest_root: "0x00".to_owned(),
                artifact_sha256: format!("{:x}", Sha256::digest(&raw)),
                record_size_bytes: FORMAL_RECORD_BYTES,
                record_count: records.len(),
                page_rows: FORMAL_PAGE_ROWS,
                page_count: records.len().div_ceil(FORMAL_PAGE_ROWS),
                page_root,
                artifact_path: PathBuf::new(),
                tree_path: PathBuf::new(),
            },
            raw,
            levels,
            order_positions: records
                .iter()
                .enumerate()
                .map(|(position, record)| (record.order, position as u32))
                .collect(),
        }
    }

    #[test]
    fn formal_episode_rejects_truncation_tampering_shrink_and_anchor_replacement() {
        let index = synthetic_index();
        let opening = index.open_episode("order:110", 5).unwrap();
        let verified =
            verify_formal_episode_opening(index.page_root(), "order:110", 5, &opening).unwrap();
        assert_eq!(verified.semantic_events.len(), 6);

        let mut truncated = opening.clone();
        truncated.pages[0].payload.pop();
        assert!(
            verify_formal_episode_opening(index.page_root(), "order:110", 5, &truncated).is_err()
        );

        let mut tampered = opening.clone();
        tampered.pages[0].payload[0] ^= 1;
        assert!(
            verify_formal_episode_opening(index.page_root(), "order:110", 5, &tampered).is_err()
        );

        let mut shrunk = opening.clone();
        shrunk.semantic_record_end -= 1;
        assert!(verify_formal_episode_opening(index.page_root(), "order:110", 5, &shrunk).is_err());

        assert!(
            verify_formal_episode_opening(index.page_root(), "order:111", 5, &opening).is_err()
        );

        let second_query = index.open_episode("order:110", 9).unwrap();
        assert_eq!(opening.page_root, second_query.page_root);
        assert_eq!(opening.page_root, index.page_root());
    }

    #[test]
    fn verified_formal_pages_feed_the_existing_evaluator() {
        let index = synthetic_index();
        let opening = index.open_episode("order:110", 5).unwrap();
        let query = compile_query_document(&serde_json::json!({
            "query_name": "FORMAL_PAGE_SMOKE",
            "evidence_scope": {
                "kind": "episode",
                "anchor_position": "root",
                "delta_seconds": 5
            },
            "full_binding_plan": {
                "query_id": "FORMAL_PAGE_SMOKE",
                "result_semantics": "full_event_binding_v1",
                "branches": [{
                    "branch_id": "branch",
                    "event_patterns": [
                        {"id":"root", "event_type":"NFTTrade", "nft":"$n", "collection":"$c"},
                        {"id":"next", "event_type":"NFTTransfer", "nft":"$n", "collection":"$c"}
                    ],
                    "constraints": [
                        {"type":"order", "sequence":["root","next"]},
                        {"type":"within", "earlier":"root", "later":"next", "delta_seconds":5},
                        {"type":"distinct_event_ids", "events":["root","next"]}
                    ]
                }]
            }
        }))
        .unwrap();
        let output = evaluate_formal_episode_opening(
            index.page_root(),
            &query,
            BTreeMap::from([("root".to_owned(), "order:110".to_owned())]),
            &opening,
        )
        .unwrap();
        assert_eq!(output.matches.len(), 3);
        assert!(output
            .matches
            .iter()
            .all(|record| record.event_bindings["root"] == "order:110"));
    }

    #[test]
    fn different_queries_reuse_the_same_prebuilt_formal_page_root() {
        let index = synthetic_index();
        let before = index.page_root().to_owned();
        let first = index.open_episode("order:110", 2).unwrap();
        let second = index.open_episode("order:120", 11).unwrap();
        assert_eq!(first.page_root, before);
        assert_eq!(second.page_root, before);
        assert_eq!(index.page_root(), before);
    }

    #[test]
    fn full_mainnet_artifact_matches_the_formal_manifest_page_root() {
        let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let manifest = crate_dir
            .parent()
            .unwrap()
            .join("FunnelQ_CSR")
            .join("artifacts")
            .join("nft_csr_profit_screen_stage6_block_b16_t1800")
            .join("csr_nft_epoch_manifest_long_final.json");
        assert!(
            manifest.is_file(),
            "missing formal three-month NFT Manifest"
        );
        let index = FormalEventPageIndex::load(&manifest).unwrap();
        assert_eq!(index.page_root(), FORMAL_NFT_PAGE_ROOT);
        assert_eq!(index.manifest().record_count, 3_201_667);
        assert_eq!(index.manifest().page_count, 200_105);
        let opening = index.open_episode("order:2210", 1_800).unwrap();
        let verified =
            verify_formal_episode_opening(FORMAL_NFT_PAGE_ROOT, "order:2210", 1_800, &opening)
                .unwrap();
        eprintln!(
            "formal Q1 smoke: pages={}, proof_nodes={}, opened_events={}, semantic_events={}",
            opening.pages.len(),
            opening.proof_nodes.len(),
            verified.opened_events.len(),
            verified.semantic_events.len()
        );
        assert!(!verified.semantic_events.is_empty());
        assert!(verified
            .semantic_events
            .iter()
            .all(|event| event.fields["collection_contract"]
                == verified.semantic_events[0].fields["collection_contract"]));
        assert!(opening.pages.len() < index.manifest().page_count);
        let query_document: Value = serde_json::from_slice(
            &fs::read(crate_dir.join("examples").join("q1_episode_1800.json")).unwrap(),
        )
        .unwrap();
        let query = compile_query_document(&query_document).unwrap();
        let evaluated = evaluate_formal_episode_opening(
            FORMAL_NFT_PAGE_ROOT,
            &query,
            BTreeMap::from([("root".to_owned(), "order:2210".to_owned())]),
            &opening,
        )
        .unwrap();
        assert_eq!(evaluated.matches.len(), 1);
        assert_eq!(evaluated.matches[0].event_bindings["back"], "order:2417");
    }
}
