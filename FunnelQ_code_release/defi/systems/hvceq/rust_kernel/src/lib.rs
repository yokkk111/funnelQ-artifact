use std::collections::HashSet;
use std::slice;

#[repr(C)]
pub struct Occurrence {
    pub var_id: u32,
    pub values: *const i32,
}

#[repr(C)]
pub struct SpecView {
    pub len: u64,
    pub ord: *const i64,
    pub eligible: *const u8,
    pub occurrences: *const Occurrence,
    pub occurrence_count: u32,
    pub view_id: u32,
}

#[repr(C)]
pub struct OrderConstraint {
    pub left_spec: u32,
    pub right_spec: u32,
}

#[repr(C)]
pub struct NeqConstraint {
    pub left_spec: u32,
    pub right_spec: u32,
    pub left_values: *const i32,
    pub right_values: *const i32,
}

#[repr(C)]
pub struct MatchRequest {
    pub specs: *const SpecView,
    pub spec_count: u32,
    pub variable_count: u32,
    pub match_order: *const u32,
    pub anchor_spec: u32,
    pub anchor_count: u64,
    pub anchor_indices: *const u64,
    pub unit_ids: *const u64,
    pub range_starts: *const u64,
    pub range_ends: *const u64,
    pub orders: *const OrderConstraint,
    pub order_count: u32,
    pub neqs: *const NeqConstraint,
    pub neq_count: u32,
}

#[repr(C)]
#[derive(Default)]
pub struct MatchResult {
    pub status: i32,
    pub num_matches: u64,
    pub num_result_units: u64,
    pub examined: u64,
}

#[repr(C)]
pub struct RefineOccurrence {
    pub spec_id: u32,
    pub variable_id: u32,
    pub values: *const i32,
    pub wanted: i32,
    pub is_variable: u32,
    pub check_existence: u32,
    pub event_types: *const i32,
    pub wanted_event_type: i32,
}

#[repr(C)]
pub struct SameTxRefineRequest {
    pub spec_count: u32,
    pub variable_count: u32,
    pub occurrences: *const RefineOccurrence,
    pub occurrence_count: u32,
    pub candidate_count: u64,
    pub range_starts: *const u64,
    pub range_ends: *const u64,
    pub output_keep: *mut u8,
}

#[repr(C)]
pub struct AnchorRefineRequest {
    pub specs: *const SpecView,
    pub spec_count: u32,
    pub variable_count: u32,
    pub anchor_spec: u32,
    pub candidate_count: u64,
    pub anchor_indices: *const u64,
    pub range_starts: *const u64,
    pub range_ends: *const u64,
    pub output_keep: *mut u8,
}

struct Engine<'a> {
    specs: &'a [SpecView],
    order: &'a [u32],
    orders: &'a [OrderConstraint],
    neqs: &'a [NeqConstraint],
    selected: Vec<u64>,
    bindings: Vec<i32>,
    bound: Vec<bool>,
    starts: &'a [u64],
    ends: &'a [u64],
    examined: u64,
    matches: u64,
}

impl<'a> Engine<'a> {
    #[inline]
    unsafe fn candidate_ok(&self, spec_id: usize, idx: usize) -> bool {
        let spec = &self.specs[spec_id];
        if idx >= spec.len as usize || *spec.eligible.add(idx) == 0 {
            return false;
        }
        for (other_id, &other_idx) in self.selected.iter().enumerate() {
            if other_idx != u64::MAX
                && self.specs[other_id].view_id == spec.view_id
                && other_idx as usize == idx
            {
                return false;
            }
        }
        let occs = slice::from_raw_parts(spec.occurrences, spec.occurrence_count as usize);
        for (pos, occ) in occs.iter().enumerate() {
            let value = *occ.values.add(idx);
            let var = occ.var_id as usize;
            if value == 0 || (self.bound[var] && self.bindings[var] != value) {
                return false;
            }
            for previous in &occs[..pos] {
                if previous.var_id == occ.var_id && *previous.values.add(idx) != value {
                    return false;
                }
            }
        }
        let ord = *spec.ord.add(idx);
        for edge in self.orders {
            let l = edge.left_spec as usize;
            let r = edge.right_spec as usize;
            if l == spec_id && self.selected[r] != u64::MAX {
                let right_ord = *self.specs[r].ord.add(self.selected[r] as usize);
                if ord >= right_ord {
                    return false;
                }
            } else if r == spec_id && self.selected[l] != u64::MAX {
                let left_ord = *self.specs[l].ord.add(self.selected[l] as usize);
                if left_ord >= ord {
                    return false;
                }
            }
        }
        for neq in self.neqs {
            let l = neq.left_spec as usize;
            let r = neq.right_spec as usize;
            if l == spec_id && r == spec_id {
                if *neq.left_values.add(idx) == *neq.right_values.add(idx) {
                    return false;
                }
            } else if l == spec_id && self.selected[r] != u64::MAX {
                if *neq.left_values.add(idx)
                    == *neq.right_values.add(self.selected[r] as usize)
                {
                    return false;
                }
            } else if r == spec_id && self.selected[l] != u64::MAX {
                if *neq.right_values.add(idx)
                    == *neq.left_values.add(self.selected[l] as usize)
                {
                    return false;
                }
            }
        }
        true
    }

    #[inline]
    unsafe fn bind_candidate(&mut self, spec_id: usize, idx: usize) -> Vec<usize> {
        let spec = &self.specs[spec_id];
        let occs = slice::from_raw_parts(spec.occurrences, spec.occurrence_count as usize);
        let mut newly_bound = Vec::with_capacity(occs.len());
        for occ in occs {
            let var = occ.var_id as usize;
            if !self.bound[var] {
                self.bound[var] = true;
                self.bindings[var] = *occ.values.add(idx);
                newly_bound.push(var);
            }
        }
        newly_bound
    }

    unsafe fn dfs(&mut self, depth: usize) {
        if depth == self.order.len() {
            self.matches += 1;
            return;
        }
        let spec_id = self.order[depth] as usize;
        if self.selected[spec_id] != u64::MAX {
            self.dfs(depth + 1);
            return;
        }
        let start = self.starts[spec_id] as usize;
        let end = self.ends[spec_id] as usize;
        for idx in start..end {
            self.examined += 1;
            if !self.candidate_ok(spec_id, idx) {
                continue;
            }
            let newly_bound = self.bind_candidate(spec_id, idx);
            self.selected[spec_id] = idx as u64;
            self.dfs(depth + 1);
            self.selected[spec_id] = u64::MAX;
            for var in newly_bound {
                self.bound[var] = false;
                self.bindings[var] = 0;
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn hvceq_match_integer_domains(
    request: *const MatchRequest,
    result: *mut MatchResult,
) -> i32 {
    if request.is_null() || result.is_null() {
        return -1;
    }
    let req = &*request;
    let out = &mut *result;
    *out = MatchResult::default();
    if req.spec_count == 0
        || req.specs.is_null()
        || req.match_order.is_null()
    {
        out.status = -2;
        return -2;
    }
    if req.anchor_count == 0 {
        out.status = 0;
        return 0;
    }
    if req.anchor_indices.is_null() || req.range_starts.is_null() || req.range_ends.is_null() {
        out.status = -3;
        return -3;
    }
    let specs = slice::from_raw_parts(req.specs, req.spec_count as usize);
    let order = slice::from_raw_parts(req.match_order, req.spec_count as usize);
    let orders = if req.order_count == 0 {
        &[]
    } else {
        slice::from_raw_parts(req.orders, req.order_count as usize)
    };
    let neqs = if req.neq_count == 0 {
        &[]
    } else {
        slice::from_raw_parts(req.neqs, req.neq_count as usize)
    };
    let anchors = slice::from_raw_parts(req.anchor_indices, req.anchor_count as usize);
    let units = if req.unit_ids.is_null() {
        &[]
    } else {
        slice::from_raw_parts(req.unit_ids, req.anchor_count as usize)
    };
    let all_starts = slice::from_raw_parts(
        req.range_starts,
        req.anchor_count as usize * req.spec_count as usize,
    );
    let all_ends = slice::from_raw_parts(
        req.range_ends,
        req.anchor_count as usize * req.spec_count as usize,
    );
    let mut result_units = HashSet::new();
    for anchor_pos in 0..anchors.len() {
        let offset = anchor_pos * req.spec_count as usize;
        let starts = &all_starts[offset..offset + req.spec_count as usize];
        let ends = &all_ends[offset..offset + req.spec_count as usize];
        let mut engine = Engine {
            specs,
            order,
            orders,
            neqs,
            selected: vec![u64::MAX; req.spec_count as usize],
            bindings: vec![0; req.variable_count as usize],
            bound: vec![false; req.variable_count as usize],
            starts,
            ends,
            examined: 0,
            matches: 0,
        };
        let anchor_spec = req.anchor_spec as usize;
        let anchor_idx = anchors[anchor_pos] as usize;
        if !engine.candidate_ok(anchor_spec, anchor_idx) {
            continue;
        }
        let _anchor_bindings = engine.bind_candidate(anchor_spec, anchor_idx);
        engine.selected[anchor_spec] = anchor_idx as u64;
        engine.dfs(0);
        out.examined += engine.examined;
        out.num_matches += engine.matches;
        if engine.matches > 0 {
            result_units.insert(if units.is_empty() {
                anchor_pos as u64
            } else {
                units[anchor_pos]
            });
        }
    }
    out.num_result_units = result_units.len() as u64;
    out.status = 0;
    0
}

#[inline]
unsafe fn occurrence_has_value(
    occurrence: &RefineOccurrence,
    start: usize,
    end: usize,
) -> bool {
    for idx in start..end {
        if *occurrence.event_types.add(idx) != occurrence.wanted_event_type {
            continue;
        }
        let value = *occurrence.values.add(idx);
        if occurrence.is_variable != 0 {
            if value != 0 {
                return true;
            }
        } else if occurrence.wanted != 0 && value == occurrence.wanted {
            return true;
        }
    }
    false
}

#[inline]
unsafe fn occurrences_share_value(
    left: &RefineOccurrence,
    left_start: usize,
    left_end: usize,
    right: &RefineOccurrence,
    right_start: usize,
    right_end: usize,
) -> bool {
    let (small, small_start, small_end, large, large_start, large_end) =
        if left_end.saturating_sub(left_start) <= right_end.saturating_sub(right_start) {
            (left, left_start, left_end, right, right_start, right_end)
        } else {
            (right, right_start, right_end, left, left_start, left_end)
        };
    let mut values = HashSet::with_capacity(small_end.saturating_sub(small_start));
    for idx in small_start..small_end {
        if *small.event_types.add(idx) != small.wanted_event_type {
            continue;
        }
        let value = *small.values.add(idx);
        if value != 0 {
            values.insert(value);
        }
    }
    if values.is_empty() {
        return false;
    }
    for idx in large_start..large_end {
        if *large.event_types.add(idx) != large.wanted_event_type {
            continue;
        }
        let value = *large.values.add(idx);
        if value != 0 && values.contains(&value) {
            return true;
        }
    }
    false
}

#[no_mangle]
pub unsafe extern "C" fn hvceq_refine_same_tx_domains(
    request: *const SameTxRefineRequest,
) -> i32 {
    if request.is_null() {
        return -1;
    }
    let req = &*request;
    if req.candidate_count == 0 {
        return 0;
    }
    if req.occurrences.is_null()
        || req.range_starts.is_null()
        || req.range_ends.is_null()
        || req.output_keep.is_null()
    {
        return -2;
    }
    let occurrences =
        slice::from_raw_parts(req.occurrences, req.occurrence_count as usize);
    let span = req.candidate_count as usize * req.spec_count as usize;
    let starts = slice::from_raw_parts(req.range_starts, span);
    let ends = slice::from_raw_parts(req.range_ends, span);
    let output = slice::from_raw_parts_mut(req.output_keep, req.candidate_count as usize);
    for candidate in 0..req.candidate_count as usize {
        let offset = candidate * req.spec_count as usize;
        let mut ok = true;
        for occurrence in occurrences {
            if occurrence.check_existence == 0 {
                continue;
            }
            let spec = occurrence.spec_id as usize;
            if !occurrence_has_value(
                occurrence,
                starts[offset + spec] as usize,
                ends[offset + spec] as usize,
            ) {
                ok = false;
                break;
            }
        }
        if ok {
            for var in 0..req.variable_count {
                let grouped: Vec<&RefineOccurrence> = occurrences
                    .iter()
                    .filter(|occ| occ.is_variable != 0 && occ.variable_id == var)
                    .collect();
                for i in 0..grouped.len() {
                    for j in (i + 1)..grouped.len() {
                        let left = grouped[i];
                        let right = grouped[j];
                        let ls = left.spec_id as usize;
                        let rs = right.spec_id as usize;
                        if !occurrences_share_value(
                            left,
                            starts[offset + ls] as usize,
                            ends[offset + ls] as usize,
                            right,
                            starts[offset + rs] as usize,
                            ends[offset + rs] as usize,
                        ) {
                            ok = false;
                            break;
                        }
                    }
                    if !ok {
                        break;
                    }
                }
                if !ok {
                    break;
                }
            }
        }
        output[candidate] = if ok { 1 } else { 0 };
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn hvceq_refine_anchor_domains(
    request: *const AnchorRefineRequest,
) -> i32 {
    if request.is_null() {
        return -1;
    }
    let req = &*request;
    if req.candidate_count == 0 {
        return 0;
    }
    if req.specs.is_null()
        || req.anchor_indices.is_null()
        || req.range_starts.is_null()
        || req.range_ends.is_null()
        || req.output_keep.is_null()
    {
        return -2;
    }
    let specs = slice::from_raw_parts(req.specs, req.spec_count as usize);
    let anchors = slice::from_raw_parts(req.anchor_indices, req.candidate_count as usize);
    let span = req.candidate_count as usize * req.spec_count as usize;
    let starts = slice::from_raw_parts(req.range_starts, span);
    let ends = slice::from_raw_parts(req.range_ends, span);
    let output = slice::from_raw_parts_mut(req.output_keep, req.candidate_count as usize);
    let anchor_spec_id = req.anchor_spec as usize;
    for candidate in 0..req.candidate_count as usize {
        let offset = candidate * req.spec_count as usize;
        let anchor_idx = anchors[candidate] as usize;
        let anchor_spec = &specs[anchor_spec_id];
        if anchor_idx >= anchor_spec.len as usize
            || *anchor_spec.eligible.add(anchor_idx) == 0
        {
            output[candidate] = 0;
            continue;
        }
        let mut bindings = vec![0i32; req.variable_count as usize];
        let mut bound = vec![false; req.variable_count as usize];
        let anchor_occurrences = slice::from_raw_parts(
            anchor_spec.occurrences,
            anchor_spec.occurrence_count as usize,
        );
        // Preserve the Python event_matches_spec behavior: a later repeated
        // occurrence in one unbound anchor spec overwrites the local value.
        for occurrence in anchor_occurrences {
            let var = occurrence.var_id as usize;
            bindings[var] = *occurrence.values.add(anchor_idx);
            bound[var] = true;
        }
        let mut feasible = true;
        for (spec_id, spec) in specs.iter().enumerate() {
            if spec_id == anchor_spec_id {
                continue;
            }
            let occurrences =
                slice::from_raw_parts(spec.occurrences, spec.occurrence_count as usize);
            let mut found = false;
            for idx in starts[offset + spec_id] as usize..ends[offset + spec_id] as usize {
                if *spec.eligible.add(idx) == 0 {
                    continue;
                }
                let mut matches = true;
                for occurrence in occurrences {
                    let var = occurrence.var_id as usize;
                    if bound[var] && *occurrence.values.add(idx) != bindings[var] {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    found = true;
                    break;
                }
            }
            if !found {
                feasible = false;
                break;
            }
        }
        output[candidate] = if feasible { 1 } else { 0 };
    }
    0
}
