use crate::model::CanonicalEvent;
use anyhow::{ensure, Result};
use std::{cell::Cell, collections::HashMap};

pub struct QueryIndependentIndex {
    events: Vec<CanonicalEvent>,
    ids: HashMap<String, usize>,
    ordered: Vec<usize>,
    pub reads: Cell<u64>,
}
impl QueryIndependentIndex {
    pub fn build_plain(events: Vec<CanonicalEvent>) -> Result<Self> {
        let ids: HashMap<_, _> = events
            .iter()
            .enumerate()
            .map(|(i, e)| (e.event_id.clone(), i))
            .collect();
        ensure!(ids.len() == events.len(), "duplicate event identity");
        let ordered = (0..events.len()).collect();
        Ok(Self {
            events,
            ids,
            ordered,
            reads: Cell::new(0),
        })
    }
    pub fn event(&self, i: usize) -> &CanonicalEvent {
        self.reads.set(self.reads.get() + 1);
        &self.events[i]
    }
    pub fn event_index(&self, id: &str) -> Option<usize> {
        self.ids.get(id).copied()
    }
    pub fn all_indices(&self) -> &[usize] {
        &self.ordered
    }
}
