use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Scalar {
    Int(i64),
    Text(String),
    Bool(bool),
}

impl Scalar {
                                                                             
                                                                           
                                                             
    pub fn same_type_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Some(left.cmp(right)),
            (Self::Text(left), Self::Text(right)) => Some(left.cmp(right)),
            (Self::Bool(left), Self::Bool(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }

    pub fn from_json(value: &Value) -> Result<Self> {
        match value {
            Value::String(value) => Ok(Self::Text(value.trim().to_owned())),
            Value::Number(value) => value
                .as_i64()
                .map(Self::Int)
                .context("only signed 64-bit integer query values are supported"),
            Value::Bool(value) => Ok(Self::Bool(*value)),
            Value::Null => Ok(Self::Text(String::new())),
            other => bail!("query/event scalar must be string, integer, or boolean, got {other}"),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            Self::Text(value) => value.parse().ok(),
            Self::Bool(_) => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Text(value) if value.is_empty())
    }
}

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i64> for Scalar {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub event_id: String,
    pub event_type: String,
    pub order_seq: i64,
    pub block_ts: i64,
    pub tx_hash: String,
    #[serde(default)]
    pub provenance: Vec<String>,
    #[serde(default)]
    pub fields: BTreeMap<String, Scalar>,
}

impl CanonicalEvent {
    pub fn new(
        event_id: impl Into<String>,
        event_type: impl Into<String>,
        order_seq: i64,
        block_ts: i64,
        tx_hash: impl Into<String>,
        fields: BTreeMap<String, Scalar>,
    ) -> Self {
        Self {
            event_id: event_id.into(),
            event_type: event_type.into(),
            order_seq,
            block_ts,
            tx_hash: tx_hash.into(),
            provenance: Vec::new(),
            fields,
        }
    }

    pub fn from_json(raw: &Value) -> Result<Self> {
        let object = raw
            .as_object()
            .context("canonical event must be a JSON object")?;
        let text = |names: &[&str]| -> String {
            names
                .iter()
                .find_map(|name| object.get(*name))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned()
        };
        let integer = |names: &[&str]| -> i64 {
            names
                .iter()
                .find_map(|name| object.get(*name))
                .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
                .unwrap_or_default()
        };
        let event_id = text(&["event_id"]);
        if event_id.is_empty() {
            bail!("canonical event has no event_id")
        }
        let event_type = text(&["event_type"]);
        if event_type.is_empty() {
            bail!("canonical event {event_id} has no event_type")
        }
        let order_seq = integer(&["order_seq"]);
        let block_ts = integer(&["block_ts", "block_time_epoch"]);
        let tx_hash = text(&["tx_hash", "tx_id"]);
        let mut fields = BTreeMap::new();
        for (name, value) in object {
            if matches!(
                name.as_str(),
                "event_id"
                    | "event_type"
                    | "order_seq"
                    | "block_ts"
                    | "block_time_epoch"
                    | "tx_hash"
                    | "tx_id"
                    | "provenance"
            ) {
                continue;
            }
            if value.is_string() || value.is_number() || value.is_boolean() || value.is_null() {
                fields.insert(name.clone(), Scalar::from_json(value)?);
            }
        }
        for (canonical, aliases) in [
            ("move_from", &["move_from", "src", "src_addr"][..]),
            ("move_to", &["move_to", "dst", "dst_addr"][..]),
            (
                "collection_contract",
                &["collection_contract", "collection"][..],
            ),
            ("nft", &["nft"][..]),
        ] {
            if !fields.contains_key(canonical) {
                if let Some(value) = aliases.iter().find_map(|alias| object.get(*alias)) {
                    fields.insert(canonical.to_owned(), Scalar::from_json(value)?);
                }
            }
        }
        let provenance = object
            .get("provenance")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            event_id,
            event_type,
            order_seq,
            block_ts,
            tx_hash,
            provenance,
            fields,
        })
    }

    pub fn value(&self, field: &str) -> Option<Scalar> {
        match field {
            "event_id" => Some(self.event_id.clone().into()),
            "event_type" => Some(self.event_type.clone().into()),
            "order_seq" => Some(self.order_seq.into()),
            "block_ts" | "block_time_epoch" => Some(self.block_ts.into()),
            "tx_hash" | "tx_id" => Some(self.tx_hash.clone().into()),
            "src" | "src_addr" => self.fields.get("move_from").cloned(),
            "dst" | "dst_addr" => self.fields.get("move_to").cloned(),
            "collection" => self.fields.get("collection_contract").cloned(),
            other => self.fields.get(other).cloned(),
        }
    }

    pub fn indexed_fields(&self) -> impl Iterator<Item = (String, Scalar)> + '_ {
        let fixed = [
            ("event_type".to_owned(), self.event_type.clone().into()),
            ("tx_hash".to_owned(), self.tx_hash.clone().into()),
        ];
        fixed.into_iter().chain(self.fields.clone())
    }
}
