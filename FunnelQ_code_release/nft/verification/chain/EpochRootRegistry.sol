// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @notice Minimal immutable-per-epoch registry used by the formal VO experiment.
/// The deployer is the only writer. Anyone can read committed EpochRoots.
contract EpochRootRegistry {
    address public immutable owner;
    mapping(bytes32 => bytes32) private _epochRoots;
    mapping(bytes32 => uint256) public committedAtBlock;

    event EpochRootCommitted(
        bytes32 indexed epochId,
        bytes32 indexed epochRoot,
        uint256 blockNumber
    );

    error NotOwner();
    error ZeroEpochRoot();
    error ConflictingEpochRoot(bytes32 existingRoot, bytes32 proposedRoot);

    constructor() {
        owner = msg.sender;
    }

    function commitEpochRoot(bytes32 epochId, bytes32 epochRoot) external {
        if (msg.sender != owner) revert NotOwner();
        if (epochRoot == bytes32(0)) revert ZeroEpochRoot();
        bytes32 existing = _epochRoots[epochId];
        if (existing != bytes32(0) && existing != epochRoot) {
            revert ConflictingEpochRoot(existing, epochRoot);
        }
        if (existing == bytes32(0)) {
            _epochRoots[epochId] = epochRoot;
            committedAtBlock[epochId] = block.number;
            emit EpochRootCommitted(epochId, epochRoot, block.number);
        }
    }

    function getEpochRoot(bytes32 epochId) external view returns (bytes32) {
        return _epochRoots[epochId];
    }
}
