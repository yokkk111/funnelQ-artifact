use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub type WitnessNodeId = u32;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WitnessNode {
    Empty,
    Unit,
    Leaf {
        atom: String,
        event_id: String,
    },
    Union(Vec<WitnessNodeId>),
    Product {
        prefix: WitnessNodeId,
        suffix: WitnessNodeId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorizedWitness {
    pub nodes: Vec<WitnessNode>,
    pub root: WitnessNodeId,
    pub branch_roots: BTreeMap<String, WitnessNodeId>,
}

impl FactorizedWitness {
    pub fn expand_event_bindings(&self) -> Vec<BTreeMap<String, String>> {
        fn expand(
            witness: &FactorizedWitness,
            id: WitnessNodeId,
            memo: &mut HashMap<WitnessNodeId, Vec<BTreeMap<String, String>>>,
        ) -> Vec<BTreeMap<String, String>> {
            if let Some(value) = memo.get(&id) {
                return value.clone();
            }
            let result = match &witness.nodes[id as usize] {
                WitnessNode::Empty => Vec::new(),
                WitnessNode::Unit => vec![BTreeMap::new()],
                WitnessNode::Leaf { atom, event_id } => {
                    vec![BTreeMap::from([(atom.clone(), event_id.clone())])]
                }
                WitnessNode::Union(children) => children
                    .iter()
                    .flat_map(|child| expand(witness, *child, memo))
                    .collect(),
                WitnessNode::Product { prefix, suffix } => {
                    let prefixes = expand(witness, *prefix, memo);
                    let suffixes = expand(witness, *suffix, memo);
                    let mut out = Vec::new();
                    for prefix in &prefixes {
                        for suffix in &suffixes {
                            if prefix.keys().any(|key| suffix.contains_key(key)) {
                                continue;
                            }
                            let mut value = prefix.clone();
                            value.extend(suffix.clone());
                            out.push(value);
                        }
                    }
                    out
                }
            };
            memo.insert(id, result.clone());
            result
        }
        let mut result = expand(self, self.root, &mut HashMap::new());
        result.sort();
        result.dedup();
        result
    }

    pub fn expand_branch_event_bindings(&self) -> Vec<(String, BTreeMap<String, String>)> {
        if self.branch_roots.is_empty() {
            return self
                .expand_event_bindings()
                .into_iter()
                .map(|events| (String::new(), events))
                .collect();
        }
        let mut output = Vec::new();
        for (branch, root) in &self.branch_roots {
            let branch_witness = Self {
                nodes: self.nodes.clone(),
                root: *root,
                branch_roots: BTreeMap::new(),
            };
            output.extend(
                branch_witness
                    .expand_event_bindings()
                    .into_iter()
                    .map(|events| (branch.clone(), events)),
            );
        }
        output.sort();
        output.dedup();
        output
    }
}

#[derive(Debug)]
pub struct WitnessArena {
    nodes: Vec<WitnessNode>,
    interned: HashMap<WitnessNode, WitnessNodeId>,
    empty: WitnessNodeId,
    unit: WitnessNodeId,
}

impl Default for WitnessArena {
    fn default() -> Self {
        let mut value = Self {
            nodes: Vec::new(),
            interned: HashMap::new(),
            empty: 0,
            unit: 0,
        };
        value.empty = value.intern(WitnessNode::Empty);
        value.unit = value.intern(WitnessNode::Unit);
        value
    }
}

impl WitnessArena {
    fn intern(&mut self, node: WitnessNode) -> WitnessNodeId {
        if let Some(id) = self.interned.get(&node) {
            return *id;
        }
        let id = self.nodes.len() as WitnessNodeId;
        self.nodes.push(node.clone());
        self.interned.insert(node, id);
        id
    }

    pub fn empty(&self) -> WitnessNodeId {
        self.empty
    }

    pub fn unit(&self) -> WitnessNodeId {
        self.unit
    }

    pub fn leaf(&mut self, atom: impl Into<String>, event_id: impl Into<String>) -> WitnessNodeId {
        self.intern(WitnessNode::Leaf {
            atom: atom.into(),
            event_id: event_id.into(),
        })
    }

    pub fn product(&mut self, prefix: WitnessNodeId, suffix: WitnessNodeId) -> WitnessNodeId {
        if prefix == self.empty || suffix == self.empty {
            return self.empty;
        }
        if prefix == self.unit {
            return suffix;
        }
        if suffix == self.unit {
            return prefix;
        }
        self.intern(WitnessNode::Product { prefix, suffix })
    }

    pub fn union(&mut self, mut alternatives: Vec<WitnessNodeId>) -> WitnessNodeId {
        alternatives.retain(|id| *id != self.empty);
        alternatives.sort_unstable();
        alternatives.dedup();
        match alternatives.as_slice() {
            [] => self.empty,
            [only] => *only,
            _ => self.intern(WitnessNode::Union(alternatives)),
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn import(&mut self, witness: &FactorizedWitness) -> WitnessNodeId {
        fn visit(
            target: &mut WitnessArena,
            source: &FactorizedWitness,
            id: WitnessNodeId,
            mapped: &mut HashMap<WitnessNodeId, WitnessNodeId>,
        ) -> WitnessNodeId {
            if let Some(value) = mapped.get(&id) {
                return *value;
            }
            let value = match &source.nodes[id as usize] {
                WitnessNode::Empty => target.empty(),
                WitnessNode::Unit => target.unit(),
                WitnessNode::Leaf { atom, event_id } => target.leaf(atom.clone(), event_id.clone()),
                WitnessNode::Union(children) => {
                    let children = children
                        .iter()
                        .map(|child| visit(target, source, *child, mapped))
                        .collect();
                    target.union(children)
                }
                WitnessNode::Product { prefix, suffix } => {
                    let prefix = visit(target, source, *prefix, mapped);
                    let suffix = visit(target, source, *suffix, mapped);
                    target.product(prefix, suffix)
                }
            };
            mapped.insert(id, value);
            value
        }
        visit(self, witness, witness.root, &mut HashMap::new())
    }

    pub fn finish(self, root: WitnessNodeId) -> FactorizedWitness {
        FactorizedWitness {
            nodes: self.nodes,
            root,
            branch_roots: BTreeMap::new(),
        }
    }

    pub fn finish_with_branches(
        self,
        root: WitnessNodeId,
        branch_roots: BTreeMap<String, WitnessNodeId>,
    ) -> FactorizedWitness {
        FactorizedWitness {
            nodes: self.nodes,
            root,
            branch_roots,
        }
    }
}
