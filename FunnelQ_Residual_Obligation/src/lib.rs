pub mod chain;
pub mod evaluator;
pub mod equivalence_diagnostic;
pub mod formal_pages;
pub mod index;
pub mod io;
pub mod metrics;
pub mod model;
pub mod oracle;
pub mod query;
pub mod residual;
pub mod residual_manifest;
pub mod verify;
pub mod witness;

pub use chain::{
    optional_chain_preflight_config, read_bound_residual_epoch_root, ChainPreflightConfig,
    ChainReadEvidence,
};
pub use evaluator::{evaluate_query, evaluate_query_with_diagnostics, EvaluationOutput, EvaluationRequest, MatchRecord};
pub use formal_pages::{
    evaluate_formal_episode_opening, verify_formal_episode_opening, FormalEpisodeOpening,
    FormalEventPageIndex, FormalEventPageManifest, FormalMerkleProofNode, FormalOpenedPage,
    VerifiedFormalEpisode, FORMAL_NFT_PAGE_ROOT, FORMAL_PAGE_ROWS, FORMAL_RECORD_BYTES,
};
pub use index::{
    verify_authenticated_event_range, AuthenticatedEventRange, EpochMetadata, MerkleProofNode,
    OpenedEpisodeRange, OpenedSameTxRange, QueryIndependentIndex, AUTH_BLOCK_EVENTS,
};
pub use model::{CanonicalEvent, Scalar};
pub use query::{compile_query_document, QuerySpec, ScopeSpec};
pub use residual_manifest::{
    CanonicalFieldEncoding, ResidualCanonicalSchema, ResidualDatasetCommitment,
    ResidualEpochManifest, ResidualEpochManifestCore, ResidualEventPageCommitment,
    ResidualQueryIndependentAccess, RESIDUAL_MANIFEST_PROTOCOL,
};
pub use verify::{
    execute_authenticated_query, make_certified_closure, make_certified_episode_closure,
    make_certified_same_tx_closure, verify_by_query_oblivious_replay, AuthenticatedQueryResponse,
    CertifiedEpisodeRange, CertifiedResidualClosure, CertifiedSameTxRange,
    ClosureAuthenticationMode, ReplayVerificationReport, TrustedReplayContext,
};
