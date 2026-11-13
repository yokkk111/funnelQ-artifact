                                                                      
   
                                                                         
                                                                            
                                                                            
                                                                               
                                                               

use crate::formal_pages::{FormalEventPageIndex, FORMAL_PAGE_ROWS, FORMAL_RECORD_BYTES};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

pub const RESIDUAL_MANIFEST_PROTOCOL: &str = "funnelq-residual-obligation-query-oblivious-epoch-v1";
const RESIDUAL_MANIFEST_DOMAIN: &[u8] = b"funnelq-residual-obligation-query-oblivious-epoch-v1\0";

const NFT_SCHEMA_ID: &str = "funnelq-nft-canonical-event-compact-v3";
const PAGE_HASH: &str = "sha256";
const PAGE_LEAF_DOMAIN: &str = "funnelq-csr-v2-event-page\\0";
const PAGE_PARENT_DOMAIN: &str = "funnelq-csr-v2-page-parent\\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualDatasetCommitment {
    pub dataset_id: String,
    pub source_epoch_id: String,
    pub canonical_event_count: usize,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalFieldEncoding {
    pub name: String,
    pub encoding: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualCanonicalSchema {
    pub schema_id: String,
    pub record_size_bytes: usize,
    pub fields: Vec<CanonicalFieldEncoding>,
    pub event_type_codes: BTreeMap<String, String>,
    pub event_id_derivation: String,
}

impl ResidualCanonicalSchema {
    fn nft_compact_v3() -> Self {
        let fields = [
            ("order", "u64-le"),
            ("timestamp", "u64-le"),
            ("move_from", "u32-le"),
            ("move_to", "u32-le"),
            ("nft", "u32-le"),
            ("collection_contract", "u32-le"),
            ("transaction", "u32-le"),
            ("event_type", "u8"),
        ]
        .into_iter()
        .map(|(name, encoding)| CanonicalFieldEncoding {
            name: name.to_owned(),
            encoding: encoding.to_owned(),
        })
        .collect();
        let event_type_codes = BTreeMap::from([
            ("1".to_owned(), "NFTTrade".to_owned()),
            ("2".to_owned(), "NFTTransfer".to_owned()),
        ]);
        Self {
            schema_id: NFT_SCHEMA_ID.to_owned(),
            record_size_bytes: FORMAL_RECORD_BYTES,
            fields,
            event_type_codes,
            event_id_derivation: "order:<u64>".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualEventPageCommitment {
    pub artifact_sha256: String,
    pub record_count: usize,
    pub page_rows: usize,
    pub page_count: usize,
    pub page_root: String,
    pub sort_key: Vec<String>,
    pub hash_algorithm: String,
    pub leaf_hash_domain: String,
    pub parent_hash_domain: String,
}

                                                                           
                                                                          
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualQueryIndependentAccess {
    pub built_before_query: bool,
    pub query_build_inputs: usize,
    pub query_specific_storage_bytes: usize,
    pub generic_posting_keys: Vec<String>,
    pub event_locator: String,
    pub same_tx_derivation: String,
    pub episode_derivation: String,
}

impl ResidualQueryIndependentAccess {
    fn canonical() -> Self {
        Self {
            built_before_query: true,
            query_build_inputs: 0,
            query_specific_storage_bytes: 0,
            generic_posting_keys: vec![
                "collection_contract".to_owned(),
                "event_type".to_owned(),
                "order".to_owned(),
                "timestamp".to_owned(),
                "transaction".to_owned(),
            ],
            event_locator: "event_id_to_collection_order_position".to_owned(),
            same_tx_derivation: "equality_on_authenticated_transaction_field".to_owned(),
            episode_derivation:
                "authenticated_anchor_collection_and_query_window_over_collection_order".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualEpochManifestCore {
    pub protocol: String,
    pub dataset: ResidualDatasetCommitment,
    pub canonical_schema: ResidualCanonicalSchema,
    pub canonical_event_pages: ResidualEventPageCommitment,
    pub query_independent_access: ResidualQueryIndependentAccess,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualEpochManifest {
    pub core: ResidualEpochManifestCore,
    pub epoch_root: String,
}

fn canonical_json(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => output.extend_from_slice(serde_json::to_string(value)?.as_bytes()),
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                canonical_json(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                output.extend_from_slice(serde_json::to_string(key)?.as_bytes());
                output.push(b':');
                canonical_json(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    let mut output = Vec::new();
    canonical_json(&value, &mut output)?;
    Ok(output)
}

fn normalize_digest(value: &str) -> Result<String> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("commitment digest must contain 32 hexadecimal bytes")
    }
    Ok(format!("0x{}", value.to_ascii_lowercase()))
}

fn source_usize(value: &Value, field: &str) -> Result<usize> {
    usize::try_from(
        value
            .get(field)
            .and_then(Value::as_u64)
            .with_context(|| format!("source Manifest has no numeric {field}"))?,
    )
    .with_context(|| format!("source Manifest {field} exceeds usize"))
}

fn source_u64(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .with_context(|| format!("source Manifest has no numeric {field}"))
}

fn source_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("source Manifest has no string {field}"))
}

impl ResidualEpochManifest {
                                                                                
                                                                            
                             
    pub fn build_before_query(source_manifest_path: &Path) -> Result<Self> {
        let formal = FormalEventPageIndex::load(source_manifest_path)?;
        let source: Value =
            serde_json::from_slice(&fs::read(source_manifest_path).with_context(|| {
                format!(
                    "read source formal Manifest {}",
                    source_manifest_path.display()
                )
            })?)?;
        let source_core = source
            .get("manifest_core")
            .context("source formal Manifest has no manifest_core")?;
        let dataset = source_core
            .get("dataset")
            .context("source formal Manifest has no dataset")?;
        let source_pages = source_core
            .get("canonical_event_pages")
            .context("source formal Manifest has no canonical_event_pages")?;

        let dataset_commitment = ResidualDatasetCommitment {
            dataset_id: source_string(dataset, "dataset_id")?.to_owned(),
            source_epoch_id: source_string(source_core, "epoch_id")?.to_owned(),
            canonical_event_count: source_usize(dataset, "canonical_event_count")?,
            min_timestamp: source_u64(dataset, "min_timestamp")?,
            max_timestamp: source_u64(dataset, "max_timestamp")?,
        };
        if dataset_commitment.canonical_event_count != formal.manifest().record_count {
            bail!("source dataset count differs from the verified event artifact")
        }
        if source_usize(source_pages, "record_count")? != formal.manifest().record_count
            || source_usize(source_pages, "record_size_bytes")?
                != formal.manifest().record_size_bytes
            || source_usize(source_pages, "page_rows")? != formal.manifest().page_rows
            || source_usize(source_pages, "page_count")? != formal.manifest().page_count
        {
            bail!("source page descriptor differs from the verified event artifact")
        }

        let core = ResidualEpochManifestCore {
            protocol: RESIDUAL_MANIFEST_PROTOCOL.to_owned(),
            dataset: dataset_commitment,
            canonical_schema: ResidualCanonicalSchema::nft_compact_v3(),
            canonical_event_pages: ResidualEventPageCommitment {
                artifact_sha256: formal.manifest().artifact_sha256.clone(),
                record_count: formal.manifest().record_count,
                page_rows: formal.manifest().page_rows,
                page_count: formal.manifest().page_count,
                page_root: normalize_digest(formal.page_root())?,
                sort_key: vec!["collection".to_owned(), "order".to_owned()],
                hash_algorithm: PAGE_HASH.to_owned(),
                leaf_hash_domain: PAGE_LEAF_DOMAIN.to_owned(),
                parent_hash_domain: PAGE_PARENT_DOMAIN.to_owned(),
            },
            query_independent_access: ResidualQueryIndependentAccess::canonical(),
        };
        Self::from_core(core)
    }

    fn from_core(core: ResidualEpochManifestCore) -> Result<Self> {
        let mut hasher = Sha256::new();
        hasher.update(RESIDUAL_MANIFEST_DOMAIN);
        hasher.update(canonical_bytes(&core)?);
        Ok(Self {
            core,
            epoch_root: format!("0x{:x}", hasher.finalize()),
        })
    }

    pub fn canonical_core_bytes(&self) -> Result<Vec<u8>> {
        canonical_bytes(&self.core)
    }

    pub fn canonical_manifest_bytes(&self) -> Result<Vec<u8>> {
        canonical_bytes(self)
    }

    pub fn write_canonical(&self, path: &Path) -> Result<()> {
        self.verify_integrity()?;
        fs::write(path, self.canonical_manifest_bytes()?)
            .with_context(|| format!("write Residual epoch Manifest {}", path.display()))
    }

    pub fn load_canonical(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)
            .with_context(|| format!("read Residual epoch Manifest {}", path.display()))?;
        let manifest: Self = serde_json::from_slice(&bytes)?;
        manifest.verify_integrity()?;
        if bytes != manifest.canonical_manifest_bytes()? {
            bail!("Residual epoch Manifest is not canonically serialized")
        }
        Ok(manifest)
    }

    pub fn verify_integrity(&self) -> Result<()> {
        if self.core.protocol != RESIDUAL_MANIFEST_PROTOCOL {
            bail!("Residual epoch Manifest protocol mismatch")
        }
        if self.core.dataset.dataset_id.trim().is_empty()
            || self.core.dataset.source_epoch_id.trim().is_empty()
            || self.core.dataset.canonical_event_count == 0
            || self.core.dataset.min_timestamp > self.core.dataset.max_timestamp
        {
            bail!("Residual epoch dataset descriptor is invalid")
        }
        if self.core.canonical_schema != ResidualCanonicalSchema::nft_compact_v3() {
            bail!("Residual epoch canonical schema is unsupported")
        }
        if self.core.query_independent_access != ResidualQueryIndependentAccess::canonical() {
            bail!("Residual epoch access metadata is not query-independent")
        }
        let pages = &self.core.canonical_event_pages;
        let expected_sort_key = vec!["collection".to_owned(), "order".to_owned()];
        if pages.record_count != self.core.dataset.canonical_event_count
            || pages.page_rows != FORMAL_PAGE_ROWS
            || pages.page_count != pages.record_count.div_ceil(pages.page_rows)
            || pages.sort_key != expected_sort_key
            || pages.hash_algorithm != PAGE_HASH
            || pages.leaf_hash_domain != PAGE_LEAF_DOMAIN
            || pages.parent_hash_domain != PAGE_PARENT_DOMAIN
        {
            bail!("Residual epoch event-page descriptor is invalid")
        }
        let canonical_artifact_digest = normalize_digest(&pages.artifact_sha256)?;
        if pages.artifact_sha256 != canonical_artifact_digest[2..] {
            bail!("Residual epoch artifact digest is not canonical lowercase hex")
        }
        if pages.page_root != normalize_digest(&pages.page_root)? {
            bail!("Residual epoch page root is not canonical lowercase prefixed hex")
        }
        let expected = Self::from_core(self.core.clone())?;
        if self.epoch_root != normalize_digest(&self.epoch_root)?
            || self.epoch_root != expected.epoch_root
        {
            bail!("Residual epoch root does not match its canonical core")
        }
        Ok(())
    }

                                                                          
                                                                             
                                                                              
                                            
    pub fn verify_against_formal_index(&self, formal: &FormalEventPageIndex) -> Result<()> {
        self.verify_integrity()?;
        let residual = &self.core.canonical_event_pages;
        let formal = formal.manifest();
        if residual.artifact_sha256 != formal.artifact_sha256
            || residual.record_count != formal.record_count
            || residual.page_rows != formal.page_rows
            || residual.page_count != formal.page_count
            || residual.page_root != formal.page_root
            || self.core.dataset.canonical_event_count != formal.record_count
            || self.core.canonical_schema.record_size_bytes != formal.record_size_bytes
        {
            bail!("Residual epoch Manifest event commitment differs from the verified formal index")
        }
        Ok(())
    }

                                                                               
                                                                          
                                                                              
    pub fn verify_against_formal_artifact(&self, source_manifest_path: &Path) -> Result<()> {
        self.verify_integrity()?;
        let expected = Self::build_before_query(source_manifest_path)?;
        if self != &expected {
            bail!("Residual epoch Manifest differs from the verified formal artifact")
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_core() -> ResidualEpochManifestCore {
        ResidualEpochManifestCore {
            protocol: RESIDUAL_MANIFEST_PROTOCOL.to_owned(),
            dataset: ResidualDatasetCommitment {
                dataset_id: "fixture".to_owned(),
                source_epoch_id: "epoch-fixture".to_owned(),
                canonical_event_count: 32,
                min_timestamp: 10,
                max_timestamp: 20,
            },
            canonical_schema: ResidualCanonicalSchema::nft_compact_v3(),
            canonical_event_pages: ResidualEventPageCommitment {
                artifact_sha256: "11".repeat(32),
                record_count: 32,
                page_rows: FORMAL_PAGE_ROWS,
                page_count: 2,
                page_root: format!("0x{}", "22".repeat(32)),
                sort_key: vec!["collection".to_owned(), "order".to_owned()],
                hash_algorithm: PAGE_HASH.to_owned(),
                leaf_hash_domain: PAGE_LEAF_DOMAIN.to_owned(),
                parent_hash_domain: PAGE_PARENT_DOMAIN.to_owned(),
            },
            query_independent_access: ResidualQueryIndependentAccess::canonical(),
        }
    }

    #[test]
    fn canonical_serialization_and_root_are_deterministic() {
        let first = ResidualEpochManifest::from_core(fixture_core()).unwrap();
        let second = ResidualEpochManifest::from_core(fixture_core()).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.canonical_manifest_bytes().unwrap(),
            second.canonical_manifest_bytes().unwrap()
        );
        first.verify_integrity().unwrap();
    }

    #[test]
    fn tampering_dataset_page_or_access_metadata_is_rejected() {
        let manifest = ResidualEpochManifest::from_core(fixture_core()).unwrap();

        let mut dataset = manifest.clone();
        dataset.core.dataset.canonical_event_count += 1;
        assert!(dataset.verify_integrity().is_err());

        let mut page = manifest.clone();
        page.core.canonical_event_pages.page_root = format!("0x{}", "33".repeat(32));
        assert!(page.verify_integrity().is_err());

        let mut access = manifest.clone();
        access.core.query_independent_access.query_build_inputs = 1;
        assert!(access.verify_integrity().is_err());
    }
}
