use crate::model::{CanonicalEvent, Scalar};
use crate::query::{AtomSpec, CompareOp, Term};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};

                                                                               
                                                                          
                                                     
pub const AUTH_BLOCK_EVENTS: usize = 256;

type Hash32 = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerkleProofNode {
    pub level: usize,
    pub index: usize,
    pub hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticatedEventRange {
    pub version: u32,
    pub block_size: usize,
    pub epoch_event_count: usize,
    pub epoch_block_count: usize,
    pub opened_block_start: usize,
    pub opened_block_end: usize,
    pub opened_event_start: usize,
    pub proof_nodes: Vec<MerkleProofNode>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenedEpisodeRange {
    pub events: Vec<CanonicalEvent>,
    pub proof: AuthenticatedEventRange,
    pub anchor_event_id: String,
    pub anchor_order: i64,
    pub anchor_time: i64,
    pub window_seconds: i64,
    pub semantic_event_start: usize,
    pub semantic_event_end: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenedSameTxRange {
    pub events: Vec<CanonicalEvent>,
    pub proof: AuthenticatedEventRange,
    pub anchor_event_id: String,
    pub tx_hash: String,
    pub semantic_event_start: usize,
    pub semantic_event_end: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochMetadata {
    pub epoch_root: String,
    pub event_count: usize,
    pub serialized_index_bytes: usize,
    pub query_specific_storage_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct QueryIndependentIndex {
    events: Vec<CanonicalEvent>,
    by_id: HashMap<String, usize>,
    postings: BTreeMap<String, BTreeMap<Scalar, Vec<usize>>>,
    ordered: Vec<usize>,
    merkle_levels: Vec<Vec<Hash32>>,
    metadata: EpochMetadata,
}

fn hash_bytes(domain: &[u8], parts: &[&[u8]]) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts {
        hasher.update((*part).len().to_le_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn hash_event_block(block_index: usize, events: &[CanonicalEvent]) -> Result<Hash32> {
    let mut hasher = Sha256::new();
    hasher.update(b"funnelq-auth-event-block-v1\0");
    hasher.update(block_index.to_le_bytes());
    hasher.update(events.len().to_le_bytes());
    for event in events {
        let encoded = serde_json::to_vec(event)?;
        hasher.update(encoded.len().to_le_bytes());
        hasher.update(encoded);
    }
    Ok(hasher.finalize().into())
}

fn hash_empty_block(block_index: usize) -> Hash32 {
    hash_bytes(
        b"funnelq-auth-empty-block-v1\0",
        &[&block_index.to_le_bytes()],
    )
}

fn hash_parent(left: &Hash32, right: &Hash32) -> Hash32 {
    hash_bytes(b"funnelq-auth-parent-v1\0", &[left, right])
}

fn epoch_commitment(event_count: usize, block_count: usize, root: &Hash32) -> String {
    let digest = hash_bytes(
        b"funnelq-query-independent-epoch-v2\0",
        &[
            &AUTH_BLOCK_EVENTS.to_le_bytes(),
            &event_count.to_le_bytes(),
            &block_count.to_le_bytes(),
            root,
        ],
    );
    hash_to_hex(&digest)
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
    if raw.len() != 64 {
        bail!("Merkle hash must contain exactly 64 hexadecimal characters")
    }
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let bytes = raw.as_bytes();
    let mut output = [0u8; 32];
    for (index, slot) in output.iter_mut().enumerate() {
        let high =
            nibble(bytes[index * 2]).ok_or_else(|| anyhow::anyhow!("invalid Merkle hash"))?;
        let low =
            nibble(bytes[index * 2 + 1]).ok_or_else(|| anyhow::anyhow!("invalid Merkle hash"))?;
        *slot = (high << 4) | low;
    }
    Ok(output)
}

fn build_merkle_levels(events: &[CanonicalEvent]) -> Result<Vec<Vec<Hash32>>> {
    let block_count = events.len().div_ceil(AUTH_BLOCK_EVENTS);
    let leaf_count = block_count.max(1).next_power_of_two();
    let mut leaves = Vec::with_capacity(leaf_count);
    for block_index in 0..leaf_count {
        if block_index < block_count {
            let start = block_index * AUTH_BLOCK_EVENTS;
            let end = (start + AUTH_BLOCK_EVENTS).min(events.len());
            leaves.push(hash_event_block(block_index, &events[start..end])?);
        } else {
            leaves.push(hash_empty_block(block_index));
        }
    }
    let mut levels = vec![leaves];
    while levels.last().is_some_and(|level| level.len() > 1) {
        let previous = levels.last().expect("Merkle tree has a leaf level");
        let next = previous
            .chunks_exact(2)
            .map(|pair| hash_parent(&pair[0], &pair[1]))
            .collect();
        levels.push(next);
    }
    Ok(levels)
}

fn compare(left: &Scalar, op: CompareOp, right: &Scalar) -> bool {
    match op {
        CompareOp::Eq => left == right,
        CompareOp::Ne => left != right,
        CompareOp::Lt => left.same_type_cmp(right).is_some_and(|order| order.is_lt()),
        CompareOp::Le => left.same_type_cmp(right).is_some_and(|order| order.is_le()),
        CompareOp::Gt => left.same_type_cmp(right).is_some_and(|order| order.is_gt()),
        CompareOp::Ge => left.same_type_cmp(right).is_some_and(|order| order.is_ge()),
    }
}

impl QueryIndependentIndex {
    pub fn build(mut events: Vec<CanonicalEvent>) -> Result<Self> {
        events.sort_by(|left, right| {
            (left.order_seq, &left.event_id).cmp(&(right.order_seq, &right.event_id))
        });
        let mut by_id = HashMap::with_capacity(events.len());
        let mut postings: BTreeMap<String, BTreeMap<Scalar, Vec<usize>>> = BTreeMap::new();
        for (index, event) in events.iter().enumerate() {
            if by_id.insert(event.event_id.clone(), index).is_some() {
                bail!("duplicate canonical event id {}", event.event_id)
            }
            for (field, value) in event.indexed_fields() {
                postings
                    .entry(field)
                    .or_default()
                    .entry(value)
                    .or_default()
                    .push(index);
            }
        }
        let ordered: Vec<_> = (0..events.len()).collect();
        let merkle_levels = build_merkle_levels(&events)?;
        let block_count = events.len().div_ceil(AUTH_BLOCK_EVENTS);
        let epoch_root = epoch_commitment(
            events.len(),
            block_count,
            &merkle_levels.last().expect("Merkle root level")[0],
        );
        let serialized_index_bytes = postings
            .iter()
            .map(|(field, values)| {
                field.len()
                    + values
                        .iter()
                        .map(|(value, ids)| {
                            serde_json::to_vec(value).map_or(0, |v| v.len()) + ids.len() * 4
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
            + merkle_levels
                .iter()
                .map(|level| level.len() * 32)
                .sum::<usize>();
        let event_count = ordered.len();
        Ok(Self {
            events,
            by_id,
            postings,
            ordered,
            merkle_levels,
            metadata: EpochMetadata {
                epoch_root,
                event_count,
                serialized_index_bytes,
                query_specific_storage_bytes: 0,
            },
        })
    }

    pub fn metadata(&self) -> &EpochMetadata {
        &self.metadata
    }

    pub fn events(&self) -> &[CanonicalEvent] {
        &self.events
    }

    pub fn event(&self, index: usize) -> &CanonicalEvent {
        &self.events[index]
    }

    pub fn event_index(&self, event_id: &str) -> Option<usize> {
        self.by_id.get(event_id).copied()
    }

    pub fn all_indices(&self) -> &[usize] {
        &self.ordered
    }

                                                                             
                                                                               
                                                                           
    pub fn open_episode_range(
        &self,
        anchor_event_id: &str,
        window_seconds: i64,
    ) -> Result<OpenedEpisodeRange> {
        if window_seconds <= 0 {
            bail!("Episode window must be positive")
        }
        if self.events.is_empty() {
            bail!("cannot open an Episode in an empty epoch")
        }
        if self
            .events
            .windows(2)
            .any(|pair| pair[0].block_ts > pair[1].block_ts)
        {
            bail!("Episode range opening requires canonical nondecreasing block timestamps")
        }
        let anchor_index = self
            .event_index(anchor_event_id)
            .ok_or_else(|| anyhow::anyhow!("unknown Episode anchor event {anchor_event_id}"))?;
        let anchor = &self.events[anchor_index];
        let semantic_event_start = self
            .events
            .partition_point(|event| event.order_seq < anchor.order_seq);
        let upper_time = anchor.block_ts.saturating_add(window_seconds);
        let semantic_event_end = self
            .events
            .partition_point(|event| event.block_ts <= upper_time)
            .max(semantic_event_start);

        let semantic_start_block = semantic_event_start / AUTH_BLOCK_EVENTS;
        let opened_block_start = semantic_start_block.saturating_sub(1);
                                                                             
                                                                     
        let last_opened_event = if semantic_event_end < self.events.len() {
            semantic_event_end
        } else {
            self.events.len() - 1
        };
        let opened_block_end = last_opened_event / AUTH_BLOCK_EVENTS + 1;
        let opened_event_start = opened_block_start * AUTH_BLOCK_EVENTS;
        let opened_event_end = (opened_block_end * AUTH_BLOCK_EVENTS).min(self.events.len());
        let proof = self.prove_block_range(opened_block_start, opened_block_end)?;
        Ok(OpenedEpisodeRange {
            events: self.events[opened_event_start..opened_event_end].to_vec(),
            proof,
            anchor_event_id: anchor.event_id.clone(),
            anchor_order: anchor.order_seq,
            anchor_time: anchor.block_ts,
            window_seconds,
            semantic_event_start,
            semantic_event_end,
        })
    }

                                                                            
                                                                              
                                                                               
    pub fn open_same_tx_range(&self, anchor_event_id: &str) -> Result<OpenedSameTxRange> {
        if self.events.is_empty() {
            bail!("cannot open a SameTx closure in an empty epoch")
        }
        let anchor_index = self
            .event_index(anchor_event_id)
            .ok_or_else(|| anyhow::anyhow!("unknown SameTx anchor event {anchor_event_id}"))?;
        let anchor = &self.events[anchor_index];
        if anchor.tx_hash.is_empty() {
            bail!("SameTx anchor has no transaction hash")
        }
        let tx_value = Scalar::from(anchor.tx_hash.clone());
        let matching = self
            .posting("tx_hash", &tx_value)
            .ok_or_else(|| anyhow::anyhow!("SameTx anchor transaction is absent from the index"))?;
        let semantic_event_start = *matching
            .first()
            .expect("anchor is a matching transaction event");
        let semantic_event_end = matching
            .last()
            .expect("anchor is a matching transaction event")
            + 1;
        if matching.len() != semantic_event_end - semantic_event_start {
            bail!("SameTx events are not contiguous in canonical event order")
        }

        let semantic_start_block = semantic_event_start / AUTH_BLOCK_EVENTS;
        let opened_block_start = semantic_start_block.saturating_sub(1);
        let last_opened_event = if semantic_event_end < self.events.len() {
            semantic_event_end
        } else {
            self.events.len() - 1
        };
        let opened_block_end = last_opened_event / AUTH_BLOCK_EVENTS + 1;
        let opened_event_start = opened_block_start * AUTH_BLOCK_EVENTS;
        let opened_event_end = (opened_block_end * AUTH_BLOCK_EVENTS).min(self.events.len());
        let proof = self.prove_block_range(opened_block_start, opened_block_end)?;
        Ok(OpenedSameTxRange {
            events: self.events[opened_event_start..opened_event_end].to_vec(),
            proof,
            anchor_event_id: anchor.event_id.clone(),
            tx_hash: anchor.tx_hash.clone(),
            semantic_event_start,
            semantic_event_end,
        })
    }

    fn prove_block_range(
        &self,
        opened_block_start: usize,
        opened_block_end: usize,
    ) -> Result<AuthenticatedEventRange> {
        let block_count = self.events.len().div_ceil(AUTH_BLOCK_EVENTS);
        if opened_block_start >= opened_block_end || opened_block_end > block_count {
            bail!("invalid authenticated block range")
        }
        let mut active: BTreeSet<usize> = (opened_block_start..opened_block_end).collect();
        let mut proof_nodes = Vec::new();
        for level in 0..self.merkle_levels.len().saturating_sub(1) {
            let hashes = &self.merkle_levels[level];
            for index in &active {
                let sibling = index ^ 1;
                if !active.contains(&sibling) {
                    proof_nodes.push(MerkleProofNode {
                        level,
                        index: sibling,
                        hash: hash_to_hex(&hashes[sibling]),
                    });
                }
            }
            active = active.into_iter().map(|index| index / 2).collect();
        }
        proof_nodes.sort_by_key(|node| (node.level, node.index));
        Ok(AuthenticatedEventRange {
            version: 1,
            block_size: AUTH_BLOCK_EVENTS,
            epoch_event_count: self.events.len(),
            epoch_block_count: block_count,
            opened_block_start,
            opened_block_end,
            opened_event_start: opened_block_start * AUTH_BLOCK_EVENTS,
            proof_nodes,
        })
    }

    fn posting(&self, field: &str, value: &Scalar) -> Option<&[usize]> {
        self.postings
            .get(field)
            .and_then(|values| values.get(value))
            .map(Vec::as_slice)
    }

    fn bound_terms(atom: &AtomSpec, bindings: &BTreeMap<String, Scalar>) -> Vec<(String, Scalar)> {
        atom.fields
            .iter()
            .filter_map(|(field, term)| match term {
                Term::Constant(value) => Some((field.clone(), value.clone())),
                Term::Variable(name) => bindings
                    .get(name)
                    .cloned()
                    .map(|value| (field.clone(), value)),
            })
            .collect()
    }

    pub fn estimate_candidates(
        &self,
        atom: &AtomSpec,
        bindings: &BTreeMap<String, Scalar>,
        allowed: Option<&BTreeSet<usize>>,
    ) -> usize {
        if let Some(allowed) = allowed {
            let bound_terms = Self::bound_terms(atom, bindings);
            return allowed
                .iter()
                .filter(|index| {
                    let event = self.event(**index);
                    atom.event_types.contains(&event.event_type)
                        && bound_terms
                            .iter()
                            .all(|(field, value)| event.value(field).as_ref() == Some(value))
                        && atom.local_predicates.iter().all(|predicate| {
                            event.value(&predicate.field).is_some_and(|value| {
                                compare(&value, predicate.op, &predicate.value)
                            })
                        })
                })
                .count();
        }
        let mut estimate = atom
            .event_types
            .iter()
            .map(|event_type| {
                self.posting("event_type", &event_type.clone().into())
                    .map_or(0, <[usize]>::len)
            })
            .sum::<usize>();
        for (field, value) in Self::bound_terms(atom, bindings) {
            estimate = estimate.min(self.posting(&field, &value).map_or(0, <[usize]>::len));
        }
        estimate
    }

    pub fn open_candidates(
        &self,
        atom: &AtomSpec,
        bindings: &BTreeMap<String, Scalar>,
        allowed: Option<&BTreeSet<usize>>,
        fixed: Option<usize>,
    ) -> (Vec<usize>, u64) {
        let bound_terms = Self::bound_terms(atom, bindings);
        let base: Vec<usize> = if let Some(index) = fixed {
            vec![index]
        } else {
            let mut sources: Vec<Vec<usize>> = Vec::new();
            let mut type_union = Vec::new();
            for event_type in &atom.event_types {
                if let Some(posting) = self.posting("event_type", &event_type.clone().into()) {
                    type_union.extend_from_slice(posting);
                }
            }
            type_union.sort_unstable();
            type_union.dedup();
            sources.push(type_union);
            for (field, value) in &bound_terms {
                sources.push(self.posting(field, value).unwrap_or_default().to_vec());
            }
            if let Some(allowed) = allowed {
                sources.push(allowed.iter().copied().collect());
            }
            sources
                .into_iter()
                .min_by_key(Vec::len)
                .unwrap_or_else(|| self.ordered.clone())
        };
        let probes = base.len() as u64;
        let candidates = base
            .into_iter()
            .filter(|index| allowed.is_none_or(|allowed| allowed.contains(index)))
            .filter(|index| {
                let event = self.event(*index);
                atom.event_types.contains(&event.event_type)
                    && bound_terms
                        .iter()
                        .all(|(field, value)| event.value(field).as_ref() == Some(value))
                    && atom.local_predicates.iter().all(|predicate| {
                        event
                            .value(&predicate.field)
                            .is_some_and(|value| compare(&value, predicate.op, &predicate.value))
                    })
            })
            .collect();
        (candidates, probes)
    }
}

                                                                              
                                                                             
                                                                  
pub fn verify_authenticated_event_range(
    trusted_epoch_root: &str,
    opened_events: &[CanonicalEvent],
    proof: &AuthenticatedEventRange,
) -> Result<()> {
    if proof.version != 1 || proof.block_size != AUTH_BLOCK_EVENTS {
        bail!("unsupported authenticated event range version or block size")
    }
    let expected_block_count = proof.epoch_event_count.div_ceil(AUTH_BLOCK_EVENTS);
    if proof.epoch_block_count != expected_block_count
        || proof.epoch_block_count == 0
        || proof.opened_block_start >= proof.opened_block_end
        || proof.opened_block_end > proof.epoch_block_count
        || proof.opened_event_start != proof.opened_block_start * AUTH_BLOCK_EVENTS
    {
        bail!("malformed authenticated event range metadata")
    }
    let expected_event_end =
        (proof.opened_block_end * AUTH_BLOCK_EVENTS).min(proof.epoch_event_count);
    let expected_event_count = expected_event_end - proof.opened_event_start;
    if opened_events.len() != expected_event_count {
        bail!("authenticated event range is truncated")
    }
    if opened_events.windows(2).any(|pair| {
        (pair[0].order_seq, &pair[0].event_id) >= (pair[1].order_seq, &pair[1].event_id)
    }) {
        bail!("opened canonical event range is not strictly ordered")
    }

    let leaf_count = proof.epoch_block_count.max(1).next_power_of_two();
    let tree_height = leaf_count.trailing_zeros() as usize;
    let mut auxiliary = BTreeMap::new();
    for node in &proof.proof_nodes {
        if node.level >= tree_height || node.index >= (leaf_count >> node.level) {
            bail!("Merkle proof node lies outside the committed tree")
        }
        if auxiliary
            .insert((node.level, node.index), hash_from_hex(&node.hash)?)
            .is_some()
        {
            bail!("duplicate Merkle proof node")
        }
    }

    let mut current = BTreeMap::new();
    for block_index in proof.opened_block_start..proof.opened_block_end {
        let local_start = block_index * AUTH_BLOCK_EVENTS - proof.opened_event_start;
        let local_end = ((block_index + 1) * AUTH_BLOCK_EVENTS).min(proof.epoch_event_count)
            - proof.opened_event_start;
        current.insert(
            block_index,
            hash_event_block(block_index, &opened_events[local_start..local_end])?,
        );
    }
    for level in 0..tree_height {
        let active_indices: Vec<_> = current.keys().copied().collect();
        let mut next = BTreeMap::new();
        let mut consumed = BTreeSet::new();
        for index in active_indices {
            if consumed.contains(&index) {
                continue;
            }
            let sibling = index ^ 1;
            let own = current[&index];
            let sibling_hash = if let Some(hash) = current.get(&sibling) {
                consumed.insert(sibling);
                *hash
            } else {
                auxiliary
                    .remove(&(level, sibling))
                    .ok_or_else(|| anyhow::anyhow!("Merkle range proof omits a required sibling"))?
            };
            consumed.insert(index);
            let parent = if index % 2 == 0 {
                hash_parent(&own, &sibling_hash)
            } else {
                hash_parent(&sibling_hash, &own)
            };
            if next.insert(index / 2, parent).is_some() {
                bail!("inconsistent Merkle range proof")
            }
        }
        current = next;
    }
    if !auxiliary.is_empty() || current.len() != 1 || !current.contains_key(&0) {
        bail!("Merkle range proof contains unused or incomplete nodes")
    }
    let reconstructed = epoch_commitment(
        proof.epoch_event_count,
        proof.epoch_block_count,
        &current[&0],
    );
    if reconstructed != trusted_epoch_root {
        bail!("authenticated event range does not reconstruct the trusted epoch root")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::compile_query_document;

    fn event(id: &str, order: i64) -> CanonicalEvent {
        CanonicalEvent::new(
            id,
            "NFTTrade",
            order,
            order,
            format!("tx{order}"),
            BTreeMap::new(),
        )
    }

    #[test]
    fn different_queries_leave_epoch_root_and_storage_unchanged() {
        let index = QueryIndependentIndex::build(vec![event("a", 1), event("b", 2)]).unwrap();
        let before = index.metadata().clone();
        for query_id in ["Q_ALPHA", "Q_BETA", "NEVER_REGISTERED"] {
            let raw = serde_json::json!({
                "query_name": query_id,
                "full_binding_plan": {
                    "query_id": query_id,
                    "result_semantics": "full_event_binding_v1",
                    "branches": [{"branch_id":"b","event_patterns":[{"id":"root","event_type":"NFTTrade"}],"constraints":[]}]
                }
            });
            compile_query_document(&raw).unwrap();
            assert_eq!(index.metadata(), &before);
        }
        assert_eq!(before.query_specific_storage_bytes, 0);
    }

    #[test]
    fn authenticated_episode_range_reconstructs_root_and_rejects_truncation() {
        let events = (0..700)
            .map(|order| event(&format!("e{order}"), order))
            .collect();
        let index = QueryIndependentIndex::build(events).unwrap();
        let opened = index.open_episode_range("e300", 25).unwrap();
        assert!(opened.events.len() < index.events().len());
        verify_authenticated_event_range(
            &index.metadata().epoch_root,
            &opened.events,
            &opened.proof,
        )
        .unwrap();

        let mut truncated = opened.events.clone();
        truncated.remove(truncated.len() / 2);
        assert!(verify_authenticated_event_range(
            &index.metadata().epoch_root,
            &truncated,
            &opened.proof,
        )
        .is_err());

        let mut changed = opened.events;
        changed[0].tx_hash.push_str("-forged");
        assert!(verify_authenticated_event_range(
            &index.metadata().epoch_root,
            &changed,
            &opened.proof,
        )
        .is_err());
    }
}
