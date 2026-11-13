use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EvaluationMetrics {
                                                                               
                                                                             
    pub residual_closure_event_count: usize,
    #[serde(default)]
    pub residual_closure_event_count_sum: u64,
    #[serde(default)]
    pub residual_closure_event_count_max: usize,
    pub raw_candidate_probes: u64,
    #[serde(default)]
    pub temporal_prefilter_rejections: u64,
                                                                          
    pub concrete_partial_bindings: u64,
    #[serde(default)]
    pub nonterminal_residual_state_visits: u64,
    pub distinct_residual_keys: u64,
    pub residual_transitions: u64,
    pub residual_split_count: u64,
    pub quotient_merge_count: u64,
    pub memoized_failed_states: u64,
    pub witness_dag_nodes: u64,
    pub final_result_count: u64,
    pub server_eval_seconds: f64,
    pub verifier_replay_seconds: f64,
    pub vo_size_bytes: u64,
    pub epoch_query_specific_storage_bytes: u64,
    pub materialized_compatibility_edges: u64,
}

impl EvaluationMetrics {
    pub fn residual_state_visits(&self) -> u64 {
        if self.nonterminal_residual_state_visits == 0 {
            self.concrete_partial_bindings
        } else {
            self.nonterminal_residual_state_visits
        }
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.distinct_residual_keys == 0 {
            0.0
        } else {
            self.residual_state_visits() as f64 / self.distinct_residual_keys as f64
        }
    }

    pub fn reuse_ratio(&self) -> f64 {
        if self.distinct_residual_keys == 0 {
            0.0
        } else {
            self.quotient_merge_count as f64 / self.distinct_residual_keys as f64
        }
    }

    pub fn merge_from(&mut self, other: &Self) {
        self.residual_closure_event_count = self
            .residual_closure_event_count
            .max(other.residual_closure_event_count);
        self.residual_closure_event_count_sum += if other.residual_closure_event_count_sum == 0 {
            other.residual_closure_event_count as u64
        } else {
            other.residual_closure_event_count_sum
        };
        self.residual_closure_event_count_max = self
            .residual_closure_event_count_max
            .max(other.residual_closure_event_count_max)
            .max(other.residual_closure_event_count);
        self.raw_candidate_probes += other.raw_candidate_probes;
        self.temporal_prefilter_rejections += other.temporal_prefilter_rejections;
        self.concrete_partial_bindings += other.concrete_partial_bindings;
        self.nonterminal_residual_state_visits += if other.nonterminal_residual_state_visits == 0 {
            other.concrete_partial_bindings
        } else {
            other.nonterminal_residual_state_visits
        };
        self.distinct_residual_keys += other.distinct_residual_keys;
        self.residual_transitions += other.residual_transitions;
        self.residual_split_count += other.residual_split_count;
        self.quotient_merge_count += other.quotient_merge_count;
        self.memoized_failed_states += other.memoized_failed_states;
        self.witness_dag_nodes += other.witness_dag_nodes;
        self.final_result_count += other.final_result_count;
        self.server_eval_seconds += other.server_eval_seconds;
        self.verifier_replay_seconds += other.verifier_replay_seconds;
        self.vo_size_bytes += other.vo_size_bytes;
        self.epoch_query_specific_storage_bytes += other.epoch_query_specific_storage_bytes;
        self.materialized_compatibility_edges += other.materialized_compatibility_edges;
    }
}

#[cfg(test)]
mod tests {
    use super::EvaluationMetrics;

    #[test]
    fn aggregate_keeps_residual_closure_sum_and_max_distinct() {
        let mut aggregate = EvaluationMetrics::default();
        aggregate.merge_from(&EvaluationMetrics {
            residual_closure_event_count: 7,
            residual_closure_event_count_sum: 7,
            residual_closure_event_count_max: 7,
            concrete_partial_bindings: 3,
            nonterminal_residual_state_visits: 3,
            ..EvaluationMetrics::default()
        });
        aggregate.merge_from(&EvaluationMetrics {
            residual_closure_event_count: 11,
            residual_closure_event_count_sum: 11,
            residual_closure_event_count_max: 11,
            concrete_partial_bindings: 5,
            nonterminal_residual_state_visits: 5,
            ..EvaluationMetrics::default()
        });
        assert_eq!(aggregate.residual_closure_event_count, 11);
        assert_eq!(aggregate.residual_closure_event_count_sum, 18);
        assert_eq!(aggregate.residual_closure_event_count_max, 11);
        assert_eq!(aggregate.nonterminal_residual_state_visits, 8);
        assert_eq!(aggregate.concrete_partial_bindings, 8);
    }
}
