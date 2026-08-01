from __future__ import annotations

from typing import Any, Sequence

from .protocol import hash_obj


def parent_hash(domain: str, left: str, right: str) -> str:
    return hash_obj(domain, {"left": left, "right": right})


def root_from_leaves(leaves: Sequence[str], domain: str) -> str:
    if not leaves:
        return hash_obj(domain, {"empty": True})
    level = list(leaves)
    while len(level) > 1:
        level = [
            parent_hash(domain, level[i], level[i + 1] if i + 1 < len(level) else level[i])
            for i in range(0, len(level), 2)
        ]
    return level[0]


def layers_from_leaves(leaves: Sequence[str], domain: str) -> list[list[str]]:
    if not leaves:
        return [[hash_obj(domain, {"empty": True})]]
    layers = [list(leaves)]
    while len(layers[-1]) > 1:
        src = layers[-1]
        layers.append(
            [
                parent_hash(domain, src[i], src[i + 1] if i + 1 < len(src) else src[i])
                for i in range(0, len(src), 2)
            ]
        )
    return layers


def proof(layers: Sequence[Sequence[str]], index: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    current = int(index)
    for level in layers[:-1]:
        sibling = current ^ 1
        duplicated = sibling >= len(level)
        out.append(
            {
                "sibling": level[current] if duplicated else level[sibling],
                "siblingOnLeft": bool(sibling < current),
                "duplicated": duplicated,
            }
        )
        current //= 2
    return out


def verify_proof(
    leaf_hash: str,
    index: int,
    path: Sequence[dict[str, Any]],
    root: str,
    domain: str,
    leaf_count: int,
) -> bool:
    """Verify an inclusion proof and bind it to the claimed leaf position.

    The previous implementation trusted ``siblingOnLeft`` from the prover and
    therefore proved only membership, not membership at ``index``.  This
    implementation derives direction and duplicate-last-node behavior from the
    claimed index and the committed leaf count at every level.
    """
    count = int(leaf_count)
    position = int(index)
    if count <= 0 or position < 0 or position >= count:
        return False
    expected_height = 0
    width = count
    while width > 1:
        expected_height += 1
        width = (width + 1) // 2
    if len(path) != expected_height:
        return False

    current = str(leaf_hash)
    width = count
    for item in path:
        sibling_index = position ^ 1
        duplicated = sibling_index >= width
        expected_on_left = (not duplicated) and sibling_index < position
        if bool(item.get("duplicated", False)) != duplicated:
            return False
        if bool(item.get("siblingOnLeft", False)) != expected_on_left:
            return False
        sibling = str(item.get("sibling", ""))
        if duplicated:
            if sibling != current:
                return False
            current = parent_hash(domain, current, current)
        elif expected_on_left:
            current = parent_hash(domain, sibling, current)
        else:
            current = parent_hash(domain, current, sibling)
        position //= 2
        width = (width + 1) // 2
    return position == 0 and width == 1 and current == root


def range_frontier(
    layers: Sequence[Sequence[str]],
    left: int,
    right: int,
) -> list[dict[str, Any]]:
    """Return complement nodes for inclusive leaf range [left, right]."""
    if left < 0 or right < left or right >= len(layers[0]):
        raise ValueError("invalid Merkle range")
    height = len(layers) - 1
    total = len(layers[0])
    out: list[dict[str, Any]] = []

    def visit(level: int, index: int, start: int, span: int) -> None:
        end = min(total, start + span) - 1
        if end < left or start > right:
            if index < len(layers[level]):
                out.append({"level": level, "index": index, "hash": layers[level][index]})
            return
        if left <= start and end <= right:
            return
        if level == 0:
            return
        half = span // 2
        visit(level - 1, index * 2, start, half)
        if index * 2 + 1 < len(layers[level - 1]):
            visit(level - 1, index * 2 + 1, start + half, half)

    visit(height, 0, 0, 1 << height)
    return out


def _expected_range_frontier_keys(
    total_count: int, left: int, right: int
) -> set[tuple[int, int]]:
    if total_count <= 0 or left < 0 or right < left or right >= total_count:
        raise ValueError("invalid Merkle range")
    height = 0
    width = total_count
    while width > 1:
        height += 1
        width = (width + 1) // 2
    expected: set[tuple[int, int]] = set()

    def visit(level: int, index: int, start: int, span: int) -> None:
        end = min(total_count, start + span) - 1
        if end < left or start > right:
            expected.add((level, index))
            return
        if left <= start and end <= right:
            return
        if level == 0:
            return
        half = span // 2
        visit(level - 1, index * 2, start, half)
        if start + half < total_count:
            visit(level - 1, index * 2 + 1, start + half, half)

    visit(height, 0, 0, 1 << height)
    return expected


def _normalise_hash(value: Any) -> str | None:
    text = str(value).lower()
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        return None
    return text


def verify_open_range(
    total_count: int,
    left: int,
    leaf_hashes: Sequence[str],
    frontier: Sequence[dict[str, Any]],
    expected_root: str,
    domain: str,
) -> bool:
    """Verify a canonical contiguous Merkle range opening in sparse time.

    The proof contains the opened leaf hashes plus the canonical complement
    frontier returned by :func:`range_frontier`.  Verification reconstructs
    only nodes that actually occur in that sparse partition.  Its work is
    therefore proportional to the opened range and frontier, rather than to
    ``total_count`` for every residual range.

    The previous implementation iterated over every parent position at every
    tree level.  For a 3.2-million-leaf TxBinding tree this turned each small
    residual opening into a scan of the entire global tree.
    """
    count = int(total_count)
    start = int(left)
    if count <= 0 or not leaf_hashes or start < 0 or start + len(leaf_hashes) > count:
        return False

    root = _normalise_hash(expected_root)
    if root is None:
        return False

    right = start + len(leaf_hashes) - 1
    try:
        expected_keys = _expected_range_frontier_keys(count, start, right)
    except ValueError:
        return False

    supplied_keys: list[tuple[int, int]] = []
    for item in frontier:
        try:
            key = (int(item["level"]), int(item["index"]))
        except (KeyError, TypeError, ValueError):
            return False
        supplied_keys.append(key)
    if len(supplied_keys) != len(set(supplied_keys)) or set(supplied_keys) != expected_keys:
        return False

    height = 0
    width = count
    level_widths = [width]
    while width > 1:
        height += 1
        width = (width + 1) // 2
        level_widths.append(width)

    # Keep only the sparse nodes supplied or derived at each level.  Across all
    # levels, a contiguous opening creates fewer than roughly twice as many
    # derived nodes as opened leaves.
    nodes_by_level: list[dict[int, str]] = [dict() for _ in range(height + 1)]

    leaves = nodes_by_level[0]
    for offset, value in enumerate(leaf_hashes):
        digest = _normalise_hash(value)
        if digest is None:
            return False
        leaves[start + offset] = digest

    for item, key in zip(frontier, supplied_keys):
        level, index = key
        if level < 0 or level > height or index < 0 or index >= level_widths[level]:
            return False
        digest = _normalise_hash(item.get("hash", ""))
        if digest is None or index in nodes_by_level[level]:
            return False
        nodes_by_level[level][index] = digest

    for level in range(height):
        current = nodes_by_level[level]
        parents = nodes_by_level[level + 1]
        level_width = level_widths[level]

        # Only parents touched by sparse known nodes can be derived.
        parent_indices = {index // 2 for index in current}
        for parent_index in parent_indices:
            left_index = parent_index * 2
            right_index = left_index + 1

            # A canonical frontier node cannot also be an ancestor of opened or
            # lower-level frontier nodes.
            if parent_index in parents:
                return False

            left_hash = current.get(left_index)
            if left_hash is None:
                # A right child without its left sibling is never canonical.
                return False

            if right_index < level_width:
                right_hash = current.get(right_index)
                if right_hash is None:
                    return False
            else:
                # root_from_leaves duplicates the final odd node.
                right_hash = left_hash

            parents[parent_index] = parent_hash(domain, left_hash, right_hash)

    root_level = nodes_by_level[height]
    return len(root_level) == 1 and root_level.get(0) == root
