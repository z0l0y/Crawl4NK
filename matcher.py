import collections
from concurrent.futures import ThreadPoolExecutor
from array import array
from bisect import bisect_left
import hashlib
import json
import logging
import math
import os
import re
import threading
import unicodedata


class KMPMatcher:
    def __init__(self, pattern: str):
        self.pattern = pattern
        self.next_arr = self._build_next(pattern)

    def _build_next(self, pattern: str):
        n = len(pattern)
        next_arr = [0] * n
        if n == 0:
            return next_arr
        j = 0
        for i in range(1, n):
            while j > 0 and pattern[i] != pattern[j]:
                j = next_arr[j - 1]
            if pattern[i] == pattern[j]:
                j += 1
            next_arr[i] = j
        return next_arr

    def search(self, text: str) -> set:
        matched = set()
        if not self.pattern:
            return matched
        n, m = len(text), len(self.pattern)
        j = 0
        for i in range(n):
            while j > 0 and text[i] != self.pattern[j]:
                j = self.next_arr[j - 1]
            if text[i] == self.pattern[j]:
                j += 1
            if j == m:
                matched.add(self.pattern)
                j = self.next_arr[j - 1]
        return matched

    def search_any(self, text: str) -> bool:
        if not self.pattern:
            return False
        j = 0
        m = len(self.pattern)
        for char in text:
            while j > 0 and char != self.pattern[j]:
                j = self.next_arr[j - 1]
            if char == self.pattern[j]:
                j += 1
            if j == m:
                return True
        return False


class SingleCharMatcher:
    def __init__(self, patterns: list):
        self.patterns = set(patterns)

    def search(self, text: str) -> set:
        if not text:
            return set()
        return set(text) & self.patterns

    def search_any(self, text: str) -> bool:
        if not text:
            return False
        for char in text:
            if char in self.patterns:
                return True
        return False


class PyAhoCorasickMatcher:
    def __init__(self, patterns: list):
        import ahocorasick

        self.automaton = ahocorasick.Automaton()
        for pattern in patterns:
            self.automaton.add_word(pattern, pattern)
        self.automaton.make_automaton()

    def search(self, text: str) -> set:
        matched = set()
        if not text:
            return matched
        for _, pattern in self.automaton.iter(text):
            matched.add(pattern)
        return matched

    def search_any(self, text: str) -> bool:
        if not text:
            return False
        for _ in self.automaton.iter(text):
            return True
        return False


class SkipCharMatcher:
    def __init__(self, patterns: list, allowed_chars: str, max_skips: int):
        self.patterns = [p for p in patterns if p and len(p) > 1]
        self.max_skips = max(int(max_skips or 0), 0)

        chars = "".join(sorted(set(allowed_chars or "")))
        if not chars:
            self.regex_pairs = []
            return

        gap = f"[{re.escape(chars)}]{{0,{self.max_skips}}}"
        self.regex_pairs = []
        for pattern in self.patterns:
            regex_pattern = "".join(f"{re.escape(ch)}{gap}" for ch in pattern[:-1]) + re.escape(pattern[-1])
            self.regex_pairs.append((pattern, re.compile(regex_pattern)))

    def search(self, text: str) -> set:
        matched = set()
        if not text or not self.regex_pairs:
            return matched

        for pattern, regex_obj in self.regex_pairs:
            if regex_obj.search(text):
                matched.add(pattern)

        return matched

    def search_any(self, text: str) -> bool:
        if not text or not self.regex_pairs:
            return False

        for _, regex_obj in self.regex_pairs:
            if regex_obj.search(text):
                return True

        return False


class PackedBitset:
    __slots__ = ("words", "size")

    def __init__(self, size: int):
        self.size = max(int(size or 0), 0)
        self.words = array("Q", [0]) * ((self.size + 63) // 64)

    def set(self, index: int):
        idx = int(index)
        if idx < 0 or idx >= self.size:
            return
        self.words[idx >> 6] |= (1 << (idx & 63))

    def get(self, index: int) -> bool:
        idx = int(index)
        if idx < 0 or idx >= self.size:
            return False
        return (self.words[idx >> 6] & (1 << (idx & 63))) != 0


class LayeredTransitionTable:
    _VALID_STRATEGIES = ("memory_first", "balanced", "speed_first")
    _DEFAULT_STRATEGY = "balanced"
    _STRATEGY_PRESETS = {
        "memory_first": {
            "linear_limit": 2,
            "dense_min_edges": 16,
            "dense_max_span": 72,
            "dense_min_density": 0.78,
            "hash_min_edges": 0,
        },
        "balanced": {
            "linear_limit": 4,
            "dense_min_edges": 10,
            "dense_max_span": 128,
            "dense_min_density": 0.58,
            "hash_min_edges": 24,
        },
        "speed_first": {
            "linear_limit": 6,
            "dense_min_edges": 8,
            "dense_max_span": 256,
            "dense_min_density": 0.42,
            "hash_min_edges": 8,
        },
    }

    @classmethod
    def normalize_strategy(cls, strategy: str | None) -> str:
        token = str(strategy or cls._DEFAULT_STRATEGY).strip().lower()
        if token not in cls._VALID_STRATEGIES:
            return cls._DEFAULT_STRATEGY
        return token

    def __init__(
        self,
        transition_dicts: list,
        strategy: str = _DEFAULT_STRATEGY,
        linear_limit: int | None = None,
        dense_min_edges: int | None = None,
        dense_max_span: int | None = None,
        dense_min_density: float | None = None,
        hash_min_edges: int | None = None,
    ):
        state_count = len(transition_dicts)
        self.strategy = self.normalize_strategy(strategy)

        preset = self._STRATEGY_PRESETS[self.strategy]
        linear_limit = preset["linear_limit"] if linear_limit is None else linear_limit
        dense_min_edges = preset["dense_min_edges"] if dense_min_edges is None else dense_min_edges
        dense_max_span = preset["dense_max_span"] if dense_max_span is None else dense_max_span
        dense_min_density = preset["dense_min_density"] if dense_min_density is None else dense_min_density
        hash_min_edges = preset["hash_min_edges"] if hash_min_edges is None else hash_min_edges

        self.state_dense = PackedBitset(state_count)
        self.state_hash = PackedBitset(state_count)
        self.state_sorted = PackedBitset(state_count)

        self.meta0 = array("I", [0]) * state_count
        self.meta1 = array("I", [0]) * state_count
        self.meta2 = array("I", [0]) * state_count

        self.sparse_keys = array("I")
        self.sparse_values = array("I")
        self.dense_values = array("I")
        self.hash_maps = [None] * state_count
        self.linear_limit = max(int(linear_limit or 1), 1)

        min_edges = max(int(dense_min_edges or 1), 1)
        max_span = max(int(dense_max_span or 1), 1)
        min_density = float(dense_min_density or 0.0)
        hash_edges = max(int(hash_min_edges or 0), 0)

        for state, edge_map in enumerate(transition_dicts):
            if not edge_map:
                continue

            items = sorted(edge_map.items())
            edge_count = len(items)
            first_key = items[0][0]
            last_key = items[-1][0]
            span = (last_key - first_key) + 1

            can_use_dense = (
                edge_count >= min_edges
                and span <= max_span
                and (edge_count / float(span)) >= min_density
            )

            if can_use_dense:
                self.state_dense.set(state)
                self.meta0[state] = int(first_key)
                self.meta1[state] = len(self.dense_values)
                self.meta2[state] = int(span)

                self.dense_values.extend([0] * span)
                dense_offset = self.meta1[state]
                dense_base = self.meta0[state]
                dense_values = self.dense_values
                for key, target in items:
                    dense_values[dense_offset + (key - dense_base)] = target
                continue

            use_hash = False
            if hash_edges and edge_count >= hash_edges:
                use_hash = True
            if self.strategy == "speed_first" and state == 0 and edge_count > 0:
                use_hash = True

            if use_hash:
                self.state_hash.set(state)
                self.hash_maps[state] = dict(items)
                continue

            self.meta0[state] = len(self.sparse_keys)
            self.meta1[state] = edge_count
            if edge_count > self.linear_limit:
                self.state_sorted.set(state)

            for key, target in items:
                self.sparse_keys.append(key)
                self.sparse_values.append(target)

    def get(self, state: int, char_id: int) -> int:
        if self.state_dense.get(state):
            base = self.meta0[state]
            offset = self.meta1[state]
            span = self.meta2[state]
            relative = char_id - base
            if relative < 0 or relative >= span:
                return 0
            return self.dense_values[offset + relative]

        if self.state_hash.get(state):
            return self.hash_maps[state].get(char_id, 0)

        size = self.meta1[state]
        if size == 0:
            return 0

        offset = self.meta0[state]
        keys = self.sparse_keys
        values = self.sparse_values
        end = offset + size

        if not self.state_sorted.get(state):
            i = offset
            while i < end:
                if keys[i] == char_id:
                    return values[i]
                i += 1
            return 0

        pos = bisect_left(keys, char_id, offset, end)
        if pos < end and keys[pos] == char_id:
            return values[pos]
        return 0


class ACAhoCorasick:
    def __init__(self, patterns: list, codec=None, transition_strategy: str = "balanced"):
        self.codec = codec if codec is not None else CharacterIdCodec()
        self.transition_strategy = LayeredTransitionTable.normalize_strategy(transition_strategy)
        self.patterns = [word for word in patterns if word]
        self.transitions = [dict()]
        self.fail = [0]
        self.outputs = [[]]
        self.node_count = 0
        self.transition_table = None
        self.output_offsets = None
        self.output_lengths = None
        self.output_pool = None
        self.output_has_state = None
        self._build_trie(self.patterns)
        self._build_fail_pointers()
        self._compact_storage()
        self.codec.freeze()

    def _new_state(self):
        self.transitions.append({})
        self.fail.append(0)
        self.outputs.append([])
        self.node_count += 1
        return self.node_count

    def _build_trie(self, patterns: list):
        for pattern_index, word in enumerate(patterns):
            if not word:
                continue
            curr = 0
            for char in word:
                char_id = self.codec.get_or_register_id(char, force=True)
                if char_id is None:
                    continue
                if char_id not in self.transitions[curr]:
                    self.transitions[curr][char_id] = self._new_state()
                curr = self.transitions[curr][char_id]
            self.outputs[curr].append(pattern_index)

    def _build_fail_pointers(self):
        queue = collections.deque()
        for child_state in self.transitions[0].values():
            self.fail[child_state] = 0
            queue.append(child_state)

        while queue:
            curr = queue.popleft()
            for char_id, next_state in self.transitions[curr].items():
                queue.append(next_state)

                fail_state = self.fail[curr]
                while fail_state and char_id not in self.transitions[fail_state]:
                    fail_state = self.fail[fail_state]

                self.fail[next_state] = self.transitions[fail_state].get(char_id, 0)

                inherited_state = self.fail[next_state]
                if self.outputs[inherited_state]:
                    self.outputs[next_state].extend(self.outputs[inherited_state])

    def _compact_storage(self):
        self.transition_table = LayeredTransitionTable(
            self.transitions,
            strategy=self.transition_strategy,
        )
        self.transitions = None
        self.fail = array("I", self.fail)

        state_count = len(self.outputs)
        output_offsets = array("I", [0]) * state_count
        output_lengths = array("I", [0]) * state_count
        output_pool = array("I")
        output_has_state = PackedBitset(state_count)

        for state, state_outputs in enumerate(self.outputs):
            if not state_outputs:
                continue
            output_has_state.set(state)
            output_offsets[state] = len(output_pool)
            output_lengths[state] = len(state_outputs)
            output_pool.extend(state_outputs)

        self.output_offsets = output_offsets
        self.output_lengths = output_lengths
        self.output_pool = output_pool
        self.output_has_state = output_has_state
        self.outputs = None

    def search(self, text: str) -> set:
        match_result = set()
        state = 0
        table = self.transition_table
        dense_bits = table.state_dense.words
        hash_bits = table.state_hash.words
        sorted_bits = table.state_sorted.words
        meta0 = table.meta0
        meta1 = table.meta1
        meta2 = table.meta2
        sparse_keys = table.sparse_keys
        sparse_values = table.sparse_values
        dense_values = table.dense_values
        hash_maps = table.hash_maps
        fail = self.fail
        output_offsets = self.output_offsets
        output_lengths = self.output_lengths
        output_pool = self.output_pool
        patterns = self.patterns

        for char in text:
            char_id = self.codec.get_id(char)
            if char_id is None:
                state = 0
                continue

            lookup_state = state
            next_state = 0
            while True:
                word_idx = lookup_state >> 6
                bit_mask = (1 << (lookup_state & 63))

                if dense_bits[word_idx] & bit_mask:
                    base = meta0[lookup_state]
                    relative = char_id - base
                    if 0 <= relative < meta2[lookup_state]:
                        next_state = dense_values[meta1[lookup_state] + relative]
                    else:
                        next_state = 0
                elif hash_bits[word_idx] & bit_mask:
                    next_state = hash_maps[lookup_state].get(char_id, 0)
                else:
                    size = meta1[lookup_state]
                    if size == 0:
                        next_state = 0
                    else:
                        offset = meta0[lookup_state]
                        end = offset + size

                        if sorted_bits[word_idx] & bit_mask:
                            pos = bisect_left(sparse_keys, char_id, offset, end)
                            if pos < end and sparse_keys[pos] == char_id:
                                next_state = sparse_values[pos]
                            else:
                                next_state = 0
                        else:
                            idx = offset
                            while idx < end:
                                if sparse_keys[idx] == char_id:
                                    next_state = sparse_values[idx]
                                    break
                                idx += 1
                            else:
                                next_state = 0

                if next_state or lookup_state == 0:
                    break
                lookup_state = fail[lookup_state]

            state = next_state

            output_count = output_lengths[state]
            if output_count:
                start = output_offsets[state]
                end = start + output_count
                for pool_idx in range(start, end):
                    match_result.add(patterns[output_pool[pool_idx]])

        return match_result

    def search_any(self, text: str) -> bool:
        state = 0
        table = self.transition_table
        dense_bits = table.state_dense.words
        hash_bits = table.state_hash.words
        sorted_bits = table.state_sorted.words
        meta0 = table.meta0
        meta1 = table.meta1
        meta2 = table.meta2
        sparse_keys = table.sparse_keys
        sparse_values = table.sparse_values
        dense_values = table.dense_values
        hash_maps = table.hash_maps
        fail = self.fail
        output_has_bits = self.output_has_state.words

        for char in text:
            char_id = self.codec.get_id(char)
            if char_id is None:
                state = 0
                continue

            lookup_state = state
            next_state = 0
            while True:
                word_idx = lookup_state >> 6
                bit_mask = (1 << (lookup_state & 63))

                if dense_bits[word_idx] & bit_mask:
                    base = meta0[lookup_state]
                    relative = char_id - base
                    if 0 <= relative < meta2[lookup_state]:
                        next_state = dense_values[meta1[lookup_state] + relative]
                    else:
                        next_state = 0
                elif hash_bits[word_idx] & bit_mask:
                    next_state = hash_maps[lookup_state].get(char_id, 0)
                else:
                    size = meta1[lookup_state]
                    if size == 0:
                        next_state = 0
                    else:
                        offset = meta0[lookup_state]
                        end = offset + size

                        if sorted_bits[word_idx] & bit_mask:
                            pos = bisect_left(sparse_keys, char_id, offset, end)
                            if pos < end and sparse_keys[pos] == char_id:
                                next_state = sparse_values[pos]
                            else:
                                next_state = 0
                        else:
                            idx = offset
                            while idx < end:
                                if sparse_keys[idx] == char_id:
                                    next_state = sparse_values[idx]
                                    break
                                idx += 1
                            else:
                                next_state = 0

                if next_state or lookup_state == 0:
                    break
                lookup_state = fail[lookup_state]

            state = next_state

            if output_has_bits[state >> 6] & (1 << (state & 63)):
                return True

        return False


class HybridMatcher:
    def __init__(
        self,
        single_patterns: list,
        multi_patterns: list,
        ac_codec=None,
        transition_strategy: str = "balanced",
    ):
        self.single_matcher = SingleCharMatcher(single_patterns) if single_patterns else None
        if len(multi_patterns) == 1:
            self.multi_matcher = KMPMatcher(multi_patterns[0])
        elif len(multi_patterns) > 1:
            self.multi_matcher = ACAhoCorasick(
                multi_patterns,
                codec=ac_codec,
                transition_strategy=transition_strategy,
            )
        else:
            self.multi_matcher = None

    def search(self, text: str) -> set:
        matched = set()
        if self.single_matcher:
            matched.update(self.single_matcher.search(text))
        if self.multi_matcher:
            matched.update(self.multi_matcher.search(text))
        return matched

    def search_any(self, text: str) -> bool:
        if self.single_matcher and self.single_matcher.search_any(text):
            return True
        if self.multi_matcher and self.multi_matcher.search_any(text):
            return True
        return False


class CharacterIdCodec:
    def __init__(self, base_mapping: dict | None = None, allow_dynamic: bool = True):
        self.char_to_id = {}
        self._used_ids = set()
        self.allow_dynamic = bool(allow_dynamic)

        base = base_mapping if isinstance(base_mapping, dict) else {}
        for raw_char, raw_id in base.items():
            ch = str(raw_char or "")
            if not ch:
                continue
            try:
                parsed_id = int(raw_id)
            except Exception:
                continue
            if parsed_id <= 0 or parsed_id in self._used_ids:
                continue
            self.char_to_id[ch] = parsed_id
            self._used_ids.add(parsed_id)

        self.next_id = max(self._used_ids) + 1 if self._used_ids else 1
        self._frozen = False

    def _allocate_next_id(self) -> int:
        while self.next_id in self._used_ids:
            self.next_id += 1
        allocated = self.next_id
        self.next_id += 1
        return allocated

    def get_or_register_id(self, ch: str, force: bool = False):
        token = str(ch or "")
        if not token:
            return None

        existing = self.char_to_id.get(token)
        if existing is not None:
            return existing

        if self._frozen and not force:
            return None
        if (not self.allow_dynamic) and not force:
            return None

        new_id = self._allocate_next_id()
        self.char_to_id[token] = new_id
        self._used_ids.add(new_id)
        return new_id

    def register_text(self, text: str, force: bool = False):
        for ch in text or "":
            self.get_or_register_id(ch, force=force)

    def freeze(self):
        self._frozen = True

    def get_id(self, ch: str):
        return self.char_to_id.get(ch)


class WeightedScoringAutomaton:
    def __init__(self, feature_specs: list, codec=None, transition_strategy: str = "balanced"):
        self.codec = codec if codec is not None else CharacterIdCodec()
        self.transition_strategy = LayeredTransitionTable.normalize_strategy(transition_strategy)
        self.feature_specs = []

        self.transitions = [dict()]
        self.fail = [0]
        self.outputs = [[]]
        self.state_count = 0
        self.transition_table = None
        self.output_offsets = None
        self.output_lengths = None
        self.output_pool = None
        self.output_has_state = None

        if not isinstance(feature_specs, list):
            feature_specs = []

        for spec in feature_specs:
            if not isinstance(spec, dict):
                continue
            pattern = str(spec.get("pattern") or "")
            if not pattern:
                continue
            self.codec.register_text(pattern, force=True)

        for spec in feature_specs:
            if not isinstance(spec, dict):
                continue
            pattern = str(spec.get("pattern") or "")
            if not pattern:
                continue

            feature_idx = len(self.feature_specs)
            self.feature_specs.append(dict(spec))
            self._insert_pattern(pattern, feature_idx)

        self._build_fail_pointers()
        self._compact_storage()
        self.codec.freeze()

        self.feature_specs = tuple(self.feature_specs)

    def _new_state(self):
        self.transitions.append({})
        self.fail.append(0)
        self.outputs.append([])
        self.state_count += 1
        return self.state_count

    def _insert_pattern(self, pattern: str, feature_idx: int):
        state = 0
        for ch in pattern:
            ch_id = self.codec.get_or_register_id(ch, force=True)
            if ch_id is None:
                continue
            if ch_id not in self.transitions[state]:
                self.transitions[state][ch_id] = self._new_state()
            state = self.transitions[state][ch_id]
        self.outputs[state].append(feature_idx)

    def _build_fail_pointers(self):
        queue = collections.deque()

        for child_state in self.transitions[0].values():
            self.fail[child_state] = 0
            queue.append(child_state)

        while queue:
            state = queue.popleft()
            for ch_id, next_state in self.transitions[state].items():
                queue.append(next_state)

                fail_state = self.fail[state]
                while fail_state and ch_id not in self.transitions[fail_state]:
                    fail_state = self.fail[fail_state]

                self.fail[next_state] = self.transitions[fail_state].get(ch_id, 0)
                inherited = self.outputs[self.fail[next_state]]
                if inherited:
                    self.outputs[next_state].extend(inherited)

    def _compact_storage(self):
        self.transition_table = LayeredTransitionTable(
            self.transitions,
            strategy=self.transition_strategy,
        )
        self.transitions = None
        self.fail = array("I", self.fail)

        state_count = len(self.outputs)
        output_offsets = array("I", [0]) * state_count
        output_lengths = array("I", [0]) * state_count
        output_pool = array("I")
        output_has_state = PackedBitset(state_count)

        for state, state_outputs in enumerate(self.outputs):
            if not state_outputs:
                continue
            output_has_state.set(state)
            output_offsets[state] = len(output_pool)
            output_lengths[state] = len(state_outputs)
            output_pool.extend(state_outputs)

        self.output_offsets = output_offsets
        self.output_lengths = output_lengths
        self.output_pool = output_pool
        self.output_has_state = output_has_state
        self.outputs = None

    def search_with_counts(self, text: str) -> dict:
        if not text:
            return {}

        counts = {}
        state = 0
        table = self.transition_table
        dense_bits = table.state_dense.words
        hash_bits = table.state_hash.words
        sorted_bits = table.state_sorted.words
        meta0 = table.meta0
        meta1 = table.meta1
        meta2 = table.meta2
        sparse_keys = table.sparse_keys
        sparse_values = table.sparse_values
        dense_values = table.dense_values
        hash_maps = table.hash_maps
        fail = self.fail
        output_offsets = self.output_offsets
        output_lengths = self.output_lengths
        output_pool = self.output_pool

        for ch in text:
            ch_id = self.codec.get_id(ch)
            if ch_id is None:
                state = 0
                continue

            lookup_state = state
            next_state = 0
            while True:
                word_idx = lookup_state >> 6
                bit_mask = (1 << (lookup_state & 63))

                if dense_bits[word_idx] & bit_mask:
                    base = meta0[lookup_state]
                    relative = ch_id - base
                    if 0 <= relative < meta2[lookup_state]:
                        next_state = dense_values[meta1[lookup_state] + relative]
                    else:
                        next_state = 0
                elif hash_bits[word_idx] & bit_mask:
                    next_state = hash_maps[lookup_state].get(ch_id, 0)
                else:
                    size = meta1[lookup_state]
                    if size == 0:
                        next_state = 0
                    else:
                        offset = meta0[lookup_state]
                        end = offset + size

                        if sorted_bits[word_idx] & bit_mask:
                            pos = bisect_left(sparse_keys, ch_id, offset, end)
                            if pos < end and sparse_keys[pos] == ch_id:
                                next_state = sparse_values[pos]
                            else:
                                next_state = 0
                        else:
                            idx = offset
                            while idx < end:
                                if sparse_keys[idx] == ch_id:
                                    next_state = sparse_values[idx]
                                    break
                                idx += 1
                            else:
                                next_state = 0

                if next_state or lookup_state == 0:
                    break
                lookup_state = fail[lookup_state]

            state = next_state

            output_count = output_lengths[state]
            if output_count:
                start = output_offsets[state]
                end = start + output_count
                for pool_idx in range(start, end):
                    feature_idx = output_pool[pool_idx]
                    counts[feature_idx] = counts.get(feature_idx, 0) + 1

        return counts

    def search(self, text: str) -> set:
        counts = self.search_with_counts(text)
        return {self.feature_specs[idx].get("feature_key") for idx in counts.keys()}

    def search_any(self, text: str) -> bool:
        if not text:
            return False

        state = 0
        table = self.transition_table
        dense_bits = table.state_dense.words
        hash_bits = table.state_hash.words
        sorted_bits = table.state_sorted.words
        meta0 = table.meta0
        meta1 = table.meta1
        meta2 = table.meta2
        sparse_keys = table.sparse_keys
        sparse_values = table.sparse_values
        dense_values = table.dense_values
        hash_maps = table.hash_maps
        fail = self.fail
        output_has_bits = self.output_has_state.words
        for ch in text:
            ch_id = self.codec.get_id(ch)
            if ch_id is None:
                state = 0
                continue

            lookup_state = state
            next_state = 0
            while True:
                word_idx = lookup_state >> 6
                bit_mask = (1 << (lookup_state & 63))

                if dense_bits[word_idx] & bit_mask:
                    base = meta0[lookup_state]
                    relative = ch_id - base
                    if 0 <= relative < meta2[lookup_state]:
                        next_state = dense_values[meta1[lookup_state] + relative]
                    else:
                        next_state = 0
                elif hash_bits[word_idx] & bit_mask:
                    next_state = hash_maps[lookup_state].get(ch_id, 0)
                else:
                    size = meta1[lookup_state]
                    if size == 0:
                        next_state = 0
                    else:
                        offset = meta0[lookup_state]
                        end = offset + size

                        if sorted_bits[word_idx] & bit_mask:
                            pos = bisect_left(sparse_keys, ch_id, offset, end)
                            if pos < end and sparse_keys[pos] == ch_id:
                                next_state = sparse_values[pos]
                            else:
                                next_state = 0
                        else:
                            idx = offset
                            while idx < end:
                                if sparse_keys[idx] == ch_id:
                                    next_state = sparse_values[idx]
                                    break
                                idx += 1
                            else:
                                next_state = 0

                if next_state or lookup_state == 0:
                    break
                lookup_state = fail[lookup_state]

            state = next_state

            if output_has_bits[state >> 6] & (1 << (state & 63)):
                return True

        return False


class TextMatcher:
    _DEFAULT_SKIP_ALLOWED_CHARS = " \t\r\n-_/|,，.。·!！?？:：;；~`'\"()[]{}<>*+&^%$#@"
    _DEFAULT_CACHE_DIR = os.path.join("ENV", ".cache", "text_matcher")
    _DEFAULT_CHAR_ID_DICT_PATHS = [
        os.path.join("dictionaries", "common_hanzi_3500.json"),
    ]
    _DIGIT_VARIANT_MAP = str.maketrans({
        "零": "0", "〇": "0",
        "一": "1", "二": "2", "三": "3", "四": "4", "五": "5", "六": "6", "七": "7", "八": "8", "九": "9",
        "壹": "1", "贰": "2", "叁": "3", "肆": "4", "伍": "5", "陆": "6", "柒": "7", "捌": "8", "玖": "9",
        "两": "2", "兩": "2", "俩": "2",
    })
    _PUNCT_VARIANT_MAP = str.maketrans({
        "，": ",",
        "。": ".",
        "；": ";",
        "：": ":",
        "！": "!",
        "？": "?",
        "（": "(",
        "）": ")",
        "【": "[",
        "】": "]",
        "《": "<",
        "》": ">",
        "“": '"',
        "”": '"',
        "‘": "'",
        "’": "'",
        "、": ",",
        "·": ".",
        "　": " ",
    })
    _DEFAULT_SCORE_KEYWORDS = [
        {"keyword": "mysql", "weight": 12, "scope": "content", "count": "once"},
        {"keyword": "索引", "weight": 10, "scope": "content", "count": "once"},
        {"keyword": "缓存", "weight": 10, "scope": "content", "count": "once"},
        {"keyword": "事务", "weight": 8, "scope": "content", "count": "once"},
        {"keyword": "并发", "weight": 8, "scope": "content", "count": "once"},
        {"keyword": "mq", "weight": 7, "scope": "content", "count": "once"},
        {"keyword": "redis", "weight": 8, "scope": "content", "count": "once"},
        {"keyword": "项目", "weight": 5, "scope": "content", "count": "repeat"},
        {"keyword": "一面", "weight": 5, "scope": "content", "count": "once"},
        {"keyword": "二面", "weight": 5, "scope": "content", "count": "once"},
        {"keyword": "hr", "weight": 4, "scope": "both", "count": "once"},
    ]
    _DEFAULT_TAIL_DRAIN_WORD_PENALTIES = {
        "内推": 15,
        "简历": 12,
        "互助": 8,
        "咨询": 8,
        "联系": 8,
        "关注": 8,
        "微信": 20,
        "vx": 18,
        "面试": 3,
        "校招": 3,
        "秋招": 3,
        "春招": 3,
        "实习": 2,
    }
    _DEFAULT_ALG_MARKERS = [
        "leetcode",
        "力扣",
        "hot100",
        "hot 100",
        "lc",
        "算法题",
        "刷题",
        "题解",
        "coding interview",
    ]

    def __init__(
        self,
        patterns: list,
        backend: str = "auto",
        native_min_patterns: int = 64,
        allow_overrides: list | None = None,
        normalization: dict | None = None,
        skip_char_match: dict | None = None,
        force_combine: dict | None = None,
        pattern_cache: dict | None = None,
        char_id_compression: dict | None = None,
        score_filter: dict | None = None,
    ):
        self._init_normalization(normalization)
        self._init_pattern_cache(pattern_cache)
        self._init_char_id_compression(char_id_compression)

        self._build_lock = threading.Lock()
        self._builder_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="matcher-build")
        self._scoring_automaton_future = None
        self._scoring_automaton = None
        self._scoring_automaton_ready = False
        self._scoring_category_index = {}
        self._quality_executor = None
        self._quality_executor_lock = threading.Lock()
        self._quality_executor_workers = 0
        self._thread_local = threading.local()

        self.score_parallel_enabled = False
        self.score_parallel_workers = 1
        self.score_parallel_batch_size = 1
        self.score_async_build = True

        self.patterns = self._prepare_patterns(patterns, cache_bucket="main")
        self.allow_overrides = self._prepare_patterns(
            allow_overrides or [],
            split_tokens=False,
            cache_bucket="allow_overrides",
        )
        self.use_kmp = False
        self.algo_name = "No Matcher"
        self.backend = (backend or "auto").strip().lower()
        self.native_min_patterns = max(int(native_min_patterns or 1), 1)
        self.matcher = None
        self.allow_override_matcher = self._build_allow_override_matcher(self.allow_overrides)
        self.skip_matcher = None
        self.title_force_patterns = []
        self.title_force_matcher = None
        self.content_combine_patterns = []
        self.content_combine_compacted = []
        self._combine_strip_table = str.maketrans("", "")

        self._init_force_combine(force_combine)
        self._init_score_filter(score_filter)

        single_patterns = [p for p in self.patterns if len(p) == 1]
        multi_patterns = [p for p in self.patterns if len(p) > 1]

        if not self.patterns:
            logging.info("警告：未提供任何匹配模式关键字。")
            self._init_skip_matcher(skip_char_match, [])
            return

        if single_patterns and not multi_patterns:
            self.matcher = SingleCharMatcher(single_patterns)
            self.algo_name = "TokenSet"
            logging.info(f"初始化单字符词元匹配机制。关键字数量: {len(self.patterns)}")

        elif not single_patterns and len(multi_patterns) == 1:
            self.matcher = KMPMatcher(multi_patterns[0])
            self.use_kmp = True
            self.algo_name = "KMP"
            logging.info(f"初始化 KMP 机制，当前加载关键字: {self.patterns}")

        elif not single_patterns and len(multi_patterns) > 1:
            native_matcher = self._try_build_native_matcher(multi_patterns)
            if native_matcher is not None:
                self.matcher = native_matcher
                self.algo_name = "AC Native"
                logging.info(
                    f"初始化 AC 原生后端(pyahocorasick)。关键字数量: {len(self.patterns)}"
                )
            else:
                self.matcher = ACAhoCorasick(
                    multi_patterns,
                    codec=self._create_char_id_codec(),
                    transition_strategy=self.char_id_transition_strategy,
                )
                self.algo_name = "AC Automaton"
                logging.info(f"初始化 AC 自动机机制。关键字数量: {len(self.patterns)}")
        else:
            self.matcher = HybridMatcher(
                single_patterns,
                multi_patterns,
                ac_codec=self._create_char_id_codec(),
                transition_strategy=self.char_id_transition_strategy,
            )
            self.algo_name = "Hybrid"
            logging.info(
                f"初始化混合匹配机制。单字符词元: {len(single_patterns)}，多字符词元: {len(multi_patterns)}"
            )

        self._init_skip_matcher(skip_char_match, multi_patterns)

    def _init_pattern_cache(self, pattern_cache: dict | None):
        cfg = pattern_cache if isinstance(pattern_cache, dict) else {}
        self.pattern_cache_enabled = bool(cfg.get("enabled", False))
        self.pattern_cache_version = str(cfg.get("version", "v1") or "v1")

        cache_dir = str(cfg.get("cache_dir", self._DEFAULT_CACHE_DIR) or self._DEFAULT_CACHE_DIR)
        if not os.path.isabs(cache_dir):
            cache_dir = os.path.join(os.getcwd(), cache_dir)
        self.pattern_cache_dir = cache_dir

        if not self.pattern_cache_enabled:
            return

        try:
            os.makedirs(self.pattern_cache_dir, exist_ok=True)
        except Exception as e:
            self.pattern_cache_enabled = False
            logging.warning(f"模式缓存目录创建失败，已禁用缓存: {e}")

    def _init_char_id_compression(self, char_id_compression: dict | None):
        cfg = char_id_compression if isinstance(char_id_compression, dict) else {}
        self.char_id_enabled = bool(cfg.get("enabled", True))
        self.char_id_allow_dynamic = bool(cfg.get("allow_dynamic_extension", True))
        self.char_id_use_index_field = bool(cfg.get("use_index_field", True))
        self.char_id_debug_log = bool(cfg.get("debug_log", False))
        self.char_id_transition_strategy = LayeredTransitionTable.normalize_strategy(
            cfg.get("transition_strategy", "balanced")
        )

        configured_paths = cfg.get("dictionary_paths")
        if not configured_paths:
            single_path = str(cfg.get("dictionary_path", "") or "").strip()
            if single_path:
                configured_paths = [single_path]
            else:
                configured_paths = list(self._DEFAULT_CHAR_ID_DICT_PATHS)
        elif isinstance(configured_paths, str):
            configured_paths = [configured_paths]
        elif not isinstance(configured_paths, (list, tuple)):
            configured_paths = list(self._DEFAULT_CHAR_ID_DICT_PATHS)

        normalized_paths = []
        seen_paths = set()
        for path_item in configured_paths:
            raw = str(path_item or "").strip()
            if not raw:
                continue
            resolved = raw if os.path.isabs(raw) else os.path.join(os.getcwd(), raw)
            normalized = os.path.normpath(resolved)
            if normalized in seen_paths:
                continue
            seen_paths.add(normalized)
            normalized_paths.append(normalized)

        self.char_id_dictionary_paths = normalized_paths
        self.char_id_base_mapping = {}
        self.char_id_loaded_paths = []

        if not self.char_id_enabled:
            return

        mapping = {}
        next_id = 1
        for dict_path in self.char_id_dictionary_paths:
            mapping, next_id, loaded = self._load_char_id_mapping_from_file(
                dict_path,
                mapping,
                next_id,
            )
            if loaded:
                self.char_id_loaded_paths.append(dict_path)

        self.char_id_base_mapping = mapping
        if self.char_id_base_mapping:
            if self.char_id_debug_log:
                logging.info(
                    f"字符ID压缩已启用。加载字符数: {len(self.char_id_base_mapping)}，字典文件数: {len(self.char_id_loaded_paths)}"
                )
        else:
            logging.warning("字符ID字典未加载成功，将回退为运行期动态字符压缩。")

    def _load_char_id_mapping_from_file(self, file_path: str, current_mapping: dict, next_id: int):
        if not os.path.exists(file_path):
            return current_mapping, next_id, False

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                raw_text = f.read()
        except Exception as e:
            logging.warning(f"加载字符ID字典失败，已跳过 {file_path}: {e}")
            return current_mapping, next_id, False

        payload = None
        try:
            parsed = json.loads(raw_text)
            if isinstance(parsed, list):
                payload = parsed
            elif isinstance(parsed, dict):
                payload = [parsed]
        except Exception:
            payload = None

        if payload is None:
            fallback_items = []
            for line in raw_text.splitlines():
                stripped = line.strip()
                if not stripped or stripped in ("[", "]"):
                    continue
                if stripped.endswith(","):
                    stripped = stripped[:-1].strip()
                if not stripped or stripped in ("[", "]"):
                    continue
                try:
                    item = json.loads(stripped)
                except Exception:
                    continue
                if isinstance(item, dict):
                    fallback_items.append(item)

            if fallback_items:
                payload = fallback_items

        if payload is None:
            logging.warning(f"字符ID字典格式异常，已跳过 {file_path}")
            return current_mapping, next_id, False

        merged = dict(current_mapping)
        used_ids = set(merged.values())
        local_next_id = max(used_ids) + 1 if used_ids else max(int(next_id or 1), 1)

        for item in payload:
            if not isinstance(item, dict):
                continue

            ch = str(item.get("char") or "")
            if len(ch) != 1 or ch in merged:
                continue

            preferred_id = None
            if self.char_id_use_index_field:
                raw_idx = item.get("index")
                try:
                    parsed_idx = int(raw_idx)
                    if parsed_idx > 0:
                        preferred_id = parsed_idx
                except Exception:
                    preferred_id = None

            if preferred_id is not None and preferred_id not in used_ids:
                assigned_id = preferred_id
            else:
                while local_next_id in used_ids:
                    local_next_id += 1
                assigned_id = local_next_id
                local_next_id += 1

            merged[ch] = assigned_id
            used_ids.add(assigned_id)

        if used_ids:
            local_next_id = max(local_next_id, max(used_ids) + 1)

        return merged, local_next_id, True

    def _create_char_id_codec(self):
        if not self.char_id_enabled:
            return CharacterIdCodec()
        return CharacterIdCodec(
            base_mapping=self.char_id_base_mapping,
            allow_dynamic=self.char_id_allow_dynamic,
        )

    def _build_pattern_cache_path(self, cache_bucket: str, patterns: list, split_tokens: bool) -> str:
        payload = {
            "version": self.pattern_cache_version,
            "bucket": cache_bucket,
            "split_tokens": bool(split_tokens),
            "patterns": list(patterns or []),
            "normalization": {
                "enable_nfkc": self.norm_enable_nfkc,
                "map_digit_variants": self.norm_map_digit_variants,
                "strip_zero_width": self.norm_strip_zero_width,
                "collapse_repeats": self.norm_collapse_repeats,
                "repeat_threshold": self.norm_repeat_threshold,
            },
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        return os.path.join(self.pattern_cache_dir, f"{cache_bucket}_{digest}.json")

    def _load_pattern_cache(self, cache_path: str) -> list | None:
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                return None
            return [item for item in data if isinstance(item, str) and item]
        except Exception:
            return None

    def _save_pattern_cache(self, cache_path: str, patterns: list):
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(patterns, f, ensure_ascii=False)
        except Exception:
            return

    def _init_normalization(self, normalization: dict | None):
        cfg = normalization if isinstance(normalization, dict) else {}
        self.norm_enable_nfkc = bool(cfg.get("enable_nfkc", True))
        self.norm_map_digit_variants = bool(cfg.get("map_digit_variants", True))
        self.norm_strip_zero_width = bool(cfg.get("strip_zero_width", True))
        self.norm_unify_punctuation = bool(cfg.get("unify_punctuation", True))
        self.norm_fold_spaces = bool(cfg.get("fold_spaces", True))
        self.norm_collapse_repeats = bool(cfg.get("collapse_repeats", False))
        self.norm_repeat_threshold = max(int(cfg.get("repeat_threshold", 3) or 3), 2)

        raw_aliases = cfg.get("semantic_aliases", {})
        self.norm_semantic_aliases = []
        if isinstance(raw_aliases, dict):
            for src, dst in raw_aliases.items():
                s = str(src or "").strip()
                d = str(dst or "").strip()
                if s:
                    self.norm_semantic_aliases.append((s, d))
        elif isinstance(raw_aliases, list):
            for item in raw_aliases:
                if not isinstance(item, dict):
                    continue
                s = str(item.get("from", "") or "").strip()
                d = str(item.get("to", "") or "").strip()
                if s:
                    self.norm_semantic_aliases.append((s, d))

        self._repeat_regex = None
        self._space_regex = re.compile(r"\s+")
        if self.norm_collapse_repeats:
            self._repeat_regex = re.compile(r"(.)\1{" + str(self.norm_repeat_threshold - 1) + r",}")

    def _init_force_combine(self, force_combine: dict | None):
        cfg = force_combine if isinstance(force_combine, dict) else {}

        self.title_force_patterns = self._prepare_patterns(
            cfg.get("title_force_contains", []),
            split_tokens=False,
            cache_bucket="title_force",
        )
        self.title_force_matcher = self._build_allow_override_matcher(self.title_force_patterns)

        self.content_combine_patterns = self._prepare_patterns(
            cfg.get("content_combine_contains", []),
            split_tokens=False,
            cache_bucket="content_combine",
        )

        combine_strip_chars = str(cfg.get("combine_strip_chars", self._DEFAULT_SKIP_ALLOWED_CHARS) or "")
        self._combine_strip_table = str.maketrans("", "", combine_strip_chars)

        compacted_patterns = []
        seen = set()
        for pattern in self.content_combine_patterns:
            compacted = pattern.translate(self._combine_strip_table)
            if len(compacted) < 2 or compacted in seen:
                continue
            seen.add(compacted)
            compacted_patterns.append(compacted)
        self.content_combine_compacted = compacted_patterns

        if self.title_force_patterns:
            logging.info(f"启用标题强规则。规则数量: {len(self.title_force_patterns)}")
        if self.content_combine_compacted:
            logging.info(f"启用正文组合规则。规则数量: {len(self.content_combine_compacted)}")

    def _init_score_filter(self, score_filter: dict | None):
        cfg = score_filter if isinstance(score_filter, dict) else {}

        self.score_enabled = bool(cfg.get("enabled", False))
        self.score_threshold = max(int(cfg.get("threshold", 90) or 90), 0)
        self.score_debug_log = bool(cfg.get("debug_log", False))
        cpu_count = max(int(os.cpu_count() or 4), 1)
        self.score_parallel_enabled = bool(cfg.get("parallel_enabled", True))
        self.score_parallel_workers = max(int(cfg.get("parallel_workers", min(max(cpu_count // 2, 2), 8)) or 1), 1)
        self.score_parallel_batch_size = max(int(cfg.get("parallel_batch_size", 8) or 8), 1)
        self.score_async_build = bool(cfg.get("async_build", True))

        self.score_base = int(cfg.get("base_score", 18) or 18)

        self.score_title_hit_weight = max(int(cfg.get("title_hit_weight", 12) or 12), 0)
        self.score_title_hit_cap = max(int(cfg.get("title_hit_cap", 36) or 36), 0)
        self.score_content_hit_weight = max(int(cfg.get("content_hit_weight", 8) or 8), 0)
        self.score_content_hit_cap = max(int(cfg.get("content_hit_cap", 34) or 34), 0)

        self.score_length_ideal_chars = max(int(cfg.get("length_ideal_chars", 2200) or 2200), 1)
        self.score_length_min_chars = max(int(cfg.get("length_min_chars", 240) or 240), 0)
        self.score_length_max_score = max(int(cfg.get("length_max_score", 34) or 34), 0)
        self.score_short_content_chars = max(int(cfg.get("short_content_chars", 420) or 420), 0)
        self.score_short_content_penalty = max(int(cfg.get("short_content_penalty", 18) or 18), 0)

        self.score_default_keyword_weight = max(int(cfg.get("default_keyword_weight", 6) or 6), 1)
        self.score_weighted_keyword_cap = max(int(cfg.get("weighted_keyword_max_score", 44) or 44), 0)
        weighted_keywords_cfg = cfg.get("weighted_keywords", self._DEFAULT_SCORE_KEYWORDS)
        self.score_weighted_keywords = self._prepare_weighted_keywords(weighted_keywords_cfg)

        self.score_tail_scan_chars = max(int(cfg.get("tail_scan_chars", 260) or 260), 0)
        self.score_tail_tag_penalty = max(int(cfg.get("tail_tag_penalty", 3) or 3), 0)
        self.score_tail_tag_penalty_cap = max(int(cfg.get("tail_tag_penalty_cap", 24) or 24), 0)
        self.score_tail_drain_penalty_cap = max(int(cfg.get("tail_drain_penalty_cap", 30) or 30), 0)
        self.score_default_tail_drain_penalty = max(int(cfg.get("default_tail_drain_penalty", 6) or 6), 1)
        tail_drain_words_cfg = cfg.get("tail_drain_words", self._DEFAULT_TAIL_DRAIN_WORD_PENALTIES)
        self.score_tail_drain_word_penalties = self._prepare_tail_drain_penalties(tail_drain_words_cfg)

        self.score_alg_enabled = bool(cfg.get("alg_enabled", True))
        default_alg_path = os.path.join("data", "algorithms", "alg.json")
        self.score_alg_path = str(cfg.get("alg_path", default_alg_path) or default_alg_path)
        if not os.path.isabs(self.score_alg_path):
            self.score_alg_path = os.path.join(os.path.dirname(__file__), self.score_alg_path)

        self.score_alg_total_cap = max(int(cfg.get("alg_total_cap", 60) or 60), 0)
        self.score_alg_marker_weight = max(int(cfg.get("alg_marker_weight", 10) or 10), 0)
        self.score_alg_marker_cap = max(int(cfg.get("alg_marker_cap", 20) or 20), 0)
        self.score_alg_topic_weight = max(int(cfg.get("alg_topic_weight", 4) or 4), 0)
        self.score_alg_topic_cap = max(int(cfg.get("alg_topic_cap", 24) or 24), 0)
        self.score_alg_problem_weight = max(int(cfg.get("alg_problem_weight", 7) or 7), 0)
        self.score_alg_problem_cap = max(int(cfg.get("alg_problem_cap", 42) or 42), 0)
        self.score_alg_problem_id_weight = max(int(cfg.get("alg_problem_id_weight", 6) or 6), 0)
        self.score_alg_problem_id_cap = max(int(cfg.get("alg_problem_id_cap", 24) or 24), 0)

        self.score_alg_hot_enabled = bool(cfg.get("alg_hot_enabled", True))
        self.score_alg_hot_total_cap = max(int(cfg.get("alg_hot_total_cap", 48) or 48), 0)
        self.score_alg_hot_title_cap = max(int(cfg.get("alg_hot_title_cap", 30) or 30), 0)
        self.score_alg_hot_id_cap = max(int(cfg.get("alg_hot_id_cap", 26) or 26), 0)
        self.score_alg_hot_max_per_problem = max(int(cfg.get("alg_hot_max_per_problem", 12) or 12), 1)
        try:
            self.score_alg_hot_freq_scale = float(cfg.get("alg_hot_freq_scale", 3.6) or 3.6)
        except Exception:
            self.score_alg_hot_freq_scale = 3.6
        try:
            self.score_alg_hot_log_base = float(cfg.get("alg_hot_log_base", 10.0) or 10.0)
        except Exception:
            self.score_alg_hot_log_base = 10.0
        if self.score_alg_hot_log_base <= 1.0:
            self.score_alg_hot_log_base = 10.0

        self.alg_marker_patterns = self._prepare_patterns(
            cfg.get("alg_markers", self._DEFAULT_ALG_MARKERS),
            split_tokens=False,
            cache_bucket="alg_markers",
        )
        self.alg_marker_matcher = self._build_allow_override_matcher(self.alg_marker_patterns)
        self.alg_topic_patterns = []
        self.alg_topic_matcher = None
        self.alg_problem_patterns = []
        self.alg_problem_matcher = None
        self.alg_problem_compacted_patterns = []
        self.alg_problem_compacted_matcher = None
        self.alg_problem_compacted_map = {}
        self.alg_problem_ids = set()
        self.alg_hot_problem_entries = []
        self.alg_hot_title_freq = {}
        self.alg_hot_id_freq = {}
        self.alg_hot_entry_by_title = {}
        self.alg_hot_entry_by_id = {}
        self.alg_hot_title_patterns = []
        self.alg_hot_title_matcher = None

        self._init_algorithm_library(cfg)

        if self.score_enabled:
            self._start_scoring_automaton_build()

        if self.score_enabled:
            logging.info(
                f"启用质量评分过滤。阈值: {self.score_threshold}，加权关键词: {len(self.score_weighted_keywords)}，标签惩罚词: {len(self.score_tail_drain_word_penalties)}，算法词库: {len(self.alg_problem_patterns)} 题/{len(self.alg_topic_patterns)} 类，面试高频: {len(self.alg_hot_problem_entries)} 条，并行评分线程: {self.score_parallel_workers}，异步构建: {self.score_async_build}"
            )

    def _prepare_weighted_keywords(self, weighted_keywords) -> list:
        if weighted_keywords is None:
            return []

        raw_entries = []
        if isinstance(weighted_keywords, dict):
            for keyword, weight in weighted_keywords.items():
                raw_entries.append({"keyword": keyword, "weight": weight})
        elif isinstance(weighted_keywords, (list, tuple)):
            raw_entries = list(weighted_keywords)
        elif isinstance(weighted_keywords, str):
            raw_entries = [weighted_keywords]
        else:
            return []

        prepared = []
        dedup = {}

        for entry in raw_entries:
            keywords = []
            weight = self.score_default_keyword_weight
            scope = "both"
            count_mode = "once"

            if isinstance(entry, str):
                keywords = self._split_pattern_tokens(entry)
            elif isinstance(entry, dict):
                keywords = self._split_pattern_tokens(str(entry.get("keyword", "") or ""))
                weight = entry.get("weight", self.score_default_keyword_weight)
                scope = str(entry.get("scope", "both") or "both").strip().lower()
                count_mode = str(entry.get("count", entry.get("count_mode", "once")) or "once").strip().lower()
            else:
                continue

            if scope not in ("title", "content", "both"):
                scope = "both"
            if count_mode not in ("once", "repeat"):
                count_mode = "once"

            try:
                weight = max(int(weight), 0)
            except Exception:
                weight = self.score_default_keyword_weight

            if weight <= 0:
                continue

            for keyword in keywords:
                normalized_keyword = self._normalize_text(keyword)
                if not normalized_keyword:
                    continue

                dedup_key = (normalized_keyword, scope, count_mode)
                old_weight = dedup.get(dedup_key)
                if old_weight is None or weight > old_weight:
                    dedup[dedup_key] = weight

        for (keyword, scope, count_mode), weight in dedup.items():
            prepared.append(
                {
                    "keyword": keyword,
                    "weight": weight,
                    "scope": scope,
                    "count": count_mode,
                }
            )

        return prepared

    def _prepare_tail_drain_penalties(self, penalties) -> dict:
        if penalties is None:
            return {}

        entries = []
        if isinstance(penalties, dict):
            for keyword, penalty in penalties.items():
                entries.append((keyword, penalty))
        elif isinstance(penalties, str):
            for token in self._split_pattern_tokens(penalties):
                entries.append((token, self.score_default_tail_drain_penalty))
        elif isinstance(penalties, (list, tuple)):
            for item in penalties:
                if isinstance(item, str):
                    for token in self._split_pattern_tokens(item):
                        entries.append((token, self.score_default_tail_drain_penalty))
                elif isinstance(item, dict):
                    keyword = item.get("keyword", "")
                    penalty = item.get("penalty", item.get("weight", self.score_default_tail_drain_penalty))
                    entries.append((keyword, penalty))

        merged = {}
        for keyword, penalty in entries:
            normalized_keyword = self._normalize_text(str(keyword or "").strip())
            if not normalized_keyword:
                continue

            try:
                penalty_score = max(int(penalty), 0)
            except Exception:
                penalty_score = self.score_default_tail_drain_penalty

            if penalty_score <= 0:
                continue

            old = merged.get(normalized_keyword)
            if old is None or penalty_score > old:
                merged[normalized_keyword] = penalty_score

        return merged

    def _count_keyword_hits(self, normalized_text: str, normalized_keyword: str) -> int:
        if not normalized_text or not normalized_keyword:
            return 0

        if re.search(r"[a-z0-9]", normalized_keyword):
            pattern = rf"(?<![a-z0-9]){re.escape(normalized_keyword)}(?![a-z0-9])"
            return len(re.findall(pattern, normalized_text))

        return normalized_text.count(normalized_keyword)

    def _normalize_tag_items(self, tags: list | None) -> list:
        normalized = []
        seen = set()
        for tag in tags or []:
            item = str(tag or "").strip()
            if not item:
                continue
            if item.startswith("#"):
                item = item[1:]
            item = item.strip(".,，。!！?？:：;；|/\\")
            item = self._normalize_text(item)
            if not item or item in seen:
                continue
            seen.add(item)
            normalized.append(item)
        return normalized

    def _extract_tail_hashtags(self, content: str) -> list:
        if not content:
            return []

        tail_text = content[-self.score_tail_scan_chars:] if self.score_tail_scan_chars > 0 else content
        normalized_tail = self._normalize_text(tail_text)
        if not normalized_tail:
            return []

        found = []
        seen = set()
        for match in re.finditer(r"(?:^|[\s\u3000])#([^\s#]{1,24})", normalized_tail):
            tag = (match.group(1) or "").strip().strip(".,，。!！?？:：;；|/\\")
            if not tag or tag in seen:
                continue
            seen.add(tag)
            found.append(tag)

        return found

    def _normalize_unique_strings(self, items, min_len: int = 1) -> list:
        normalized = []
        seen = set()
        for item in items or []:
            token = self._normalize_text(str(item or "").strip())
            if not token or len(token) < min_len or token in seen:
                continue
            seen.add(token)
            normalized.append(token)
        return normalized

    def _to_non_negative_int(self, value, default: int = 0) -> int:
        try:
            parsed = int(value)
            if parsed < 0:
                return default
            return parsed
        except Exception:
            return default

    def _score_from_hot_frequency(self, frequency: int) -> int:
        freq = self._to_non_negative_int(frequency, 0)
        if freq <= 0:
            return 0

        raw = math.log(freq + 1, self.score_alg_hot_log_base) * self.score_alg_hot_freq_scale
        score = int(round(raw))
        score = max(score, 1)
        return min(score, self.score_alg_hot_max_per_problem)

    def _choose_hot_entry(self, old_entry: dict | None, new_entry: dict) -> dict:
        if not old_entry:
            return dict(new_entry)

        old_freq = self._to_non_negative_int(old_entry.get("frequency", 0), 0)
        new_freq = self._to_non_negative_int(new_entry.get("frequency", 0), 0)
        if new_freq > old_freq:
            winner = dict(new_entry)
            loser = old_entry
        else:
            winner = dict(old_entry)
            loser = new_entry

        for key in ("difficulty", "last_date", "url", "source", "frontend_id"):
            if not winner.get(key) and loser.get(key):
                winner[key] = loser.get(key)

        return winner

    def _build_hot_match_details(self, hot_title_hits: list, hot_id_hits: list) -> list:
        merged = {}

        for problem_id in hot_id_hits or []:
            entry = self.alg_hot_entry_by_id.get(problem_id)
            if not entry:
                continue

            key = f"id:{problem_id}"
            if key not in merged:
                merged[key] = {
                    "id": entry.get("id"),
                    "frontend_id": entry.get("frontend_id"),
                    "title": entry.get("title_display") or entry.get("title"),
                    "difficulty": entry.get("difficulty", ""),
                    "last_date": entry.get("last_date", ""),
                    "frequency": self._to_non_negative_int(entry.get("frequency", 0), 0),
                    "url": entry.get("url", ""),
                    "source": entry.get("source", ""),
                    "matched_by": ["id"],
                }
            elif "id" not in merged[key]["matched_by"]:
                merged[key]["matched_by"].append("id")

        for title in hot_title_hits or []:
            entry = self.alg_hot_entry_by_title.get(title)
            if not entry:
                continue

            if entry.get("id") is not None:
                key = f"id:{entry.get('id')}"
            else:
                key = f"title:{entry.get('title') or title}"

            if key not in merged:
                merged[key] = {
                    "id": entry.get("id"),
                    "frontend_id": entry.get("frontend_id"),
                    "title": entry.get("title_display") or entry.get("title") or title,
                    "difficulty": entry.get("difficulty", ""),
                    "last_date": entry.get("last_date", ""),
                    "frequency": self._to_non_negative_int(entry.get("frequency", 0), 0),
                    "url": entry.get("url", ""),
                    "source": entry.get("source", ""),
                    "matched_by": ["title"],
                }
            elif "title" not in merged[key]["matched_by"]:
                merged[key]["matched_by"].append("title")

        return sorted(
            merged.values(),
            key=lambda item: (-self._to_non_negative_int(item.get("frequency", 0), 0), str(item.get("title", ""))),
        )

    def _build_scoring_feature_specs(self) -> list:
        specs = []
        seen = set()

        def add_spec(category: str, pattern: str, **meta):
            normalized_pattern = self._normalize_text(pattern)
            if not normalized_pattern:
                return

            dedup_key = (
                category,
                normalized_pattern,
                str(meta.get("scope", "both") or "both"),
                str(meta.get("count_mode", "once") or "once"),
                str(meta.get("lookup_key", "") or ""),
            )
            if dedup_key in seen:
                return
            seen.add(dedup_key)

            spec = {
                "feature_key": f"{category}:{len(specs)}",
                "category": category,
                "pattern": normalized_pattern,
            }
            spec.update(meta)
            specs.append(spec)

        for pattern in self.patterns:
            add_spec(
                "must_contain",
                pattern,
                scope="both",
                title_weight=self.score_title_hit_weight,
                content_weight=self.score_content_hit_weight,
            )

        for rule in self.score_weighted_keywords:
            add_spec(
                "weighted_keyword",
                rule.get("keyword", ""),
                scope=rule.get("scope", "both"),
                count_mode=rule.get("count", "once"),
                weight=int(rule.get("weight", 0) or 0),
                raw_keyword=str(rule.get("keyword", "") or ""),
            )

        for marker in self.alg_marker_patterns:
            add_spec("alg_marker", marker)

        for topic in self.alg_topic_patterns:
            add_spec("alg_topic", topic)

        for problem in self.alg_problem_patterns:
            add_spec("alg_problem", problem)

        for hot_title in self.alg_hot_title_patterns:
            add_spec("alg_hot_title", hot_title, lookup_key=hot_title)

        return specs

    def _build_scoring_category_index(self, feature_specs: list) -> dict:
        index = {}
        for idx, spec in enumerate(feature_specs):
            category = str(spec.get("category") or "")
            if not category:
                continue
            index.setdefault(category, []).append(idx)
        return index

    def _start_scoring_automaton_build(self):
        feature_specs = self._build_scoring_feature_specs()
        self._scoring_category_index = self._build_scoring_category_index(feature_specs)
        scoring_codec = self._create_char_id_codec()

        if not feature_specs:
            self._scoring_automaton = None
            self._scoring_automaton_future = None
            self._scoring_automaton_ready = True
            return

        self._scoring_automaton = None
        self._scoring_automaton_ready = False

        if not self.score_async_build:
            try:
                self._scoring_automaton = WeightedScoringAutomaton(
                    feature_specs,
                    scoring_codec,
                    transition_strategy=self.char_id_transition_strategy,
                )
                self._scoring_automaton_ready = True
            except Exception as e:
                logging.warning(f"打分自动机构建失败，已回退普通评分路径: {e}")
                self._scoring_automaton = None
                self._scoring_automaton_ready = False
            return

        self._scoring_automaton_future = self._builder_executor.submit(
            WeightedScoringAutomaton,
            feature_specs,
            scoring_codec,
            self.char_id_transition_strategy,
        )
        logging.info(f"已异步启动打分自动机构建。特征数量: {len(feature_specs)}")

    def _ensure_scoring_automaton_ready(self, wait: bool = False) -> bool:
        if self._scoring_automaton_ready and self._scoring_automaton is not None:
            return True

        future = self._scoring_automaton_future
        if future is None:
            return self._scoring_automaton is not None

        if not future.done() and not wait:
            return False

        with self._build_lock:
            if self._scoring_automaton_ready and self._scoring_automaton is not None:
                return True

            try:
                automaton = future.result()
            except Exception as e:
                logging.warning(f"异步打分自动机构建失败，已回退普通评分路径: {e}")
                self._scoring_automaton_future = None
                self._scoring_automaton = None
                self._scoring_automaton_ready = False
                return False

            self._scoring_automaton = automaton
            self._scoring_automaton_ready = True
            self._scoring_automaton_future = None
            logging.info("异步打分自动机构建完成。")
            return True

    def _collect_scoring_counts(self, text: str, wait: bool = False) -> dict:
        if not self._ensure_scoring_automaton_ready(wait=wait):
            return {}
        if self._scoring_automaton is None:
            return {}
        return self._scoring_automaton.search_with_counts(text or "")

    def _evaluate_weighted_keywords_via_automaton(self, normalized_title: str, normalized_content: str):
        if not self.score_weighted_keywords:
            return 0, []

        if not self._ensure_scoring_automaton_ready(wait=False):
            return None

        title_counts = self._collect_scoring_counts(normalized_title)
        content_counts = self._collect_scoring_counts(normalized_content)
        weighted_indices = self._scoring_category_index.get("weighted_keyword", [])
        if not weighted_indices:
            return 0, []

        weighted_score = 0
        weighted_hits = []

        for idx in weighted_indices:
            spec = self._scoring_automaton.feature_specs[idx]
            scope = str(spec.get("scope", "both") or "both")
            count_mode = str(spec.get("count_mode", "once") or "once")
            weight = self._to_non_negative_int(spec.get("weight", 0), 0)
            if weight <= 0:
                continue

            hit_count = 0
            if scope in ("title", "both"):
                hit_count += int(title_counts.get(idx, 0) or 0)
            if scope in ("content", "both"):
                hit_count += int(content_counts.get(idx, 0) or 0)

            if hit_count <= 0:
                continue

            gain = weight if count_mode == "once" else weight * hit_count
            if weighted_score + gain > self.score_weighted_keyword_cap:
                gain = max(self.score_weighted_keyword_cap - weighted_score, 0)

            if gain <= 0:
                break

            weighted_score += gain
            weighted_hits.append(
                {
                    "keyword": spec.get("raw_keyword", spec.get("pattern", "")),
                    "scope": scope,
                    "count": hit_count,
                    "score": gain,
                }
            )

            if weighted_score >= self.score_weighted_keyword_cap:
                break

        return weighted_score, weighted_hits

    def _evaluate_algorithm_signal_via_automaton(self, raw_title: str, raw_content: str, normalized_title: str, normalized_content: str):
        if not self.score_alg_enabled:
            return {
                "score": 0,
                "marker_hits": [],
                "topic_hits": [],
                "problem_hits": [],
                "problem_id_hits": [],
                "hot_score": 0,
                "hot_title_score": 0,
                "hot_id_score": 0,
                "hot_title_hits": [],
                "hot_id_hits": [],
                "hot_matches": [],
                "marker_score": 0,
                "topic_score": 0,
                "problem_score": 0,
                "problem_id_score": 0,
            }

        if not self._ensure_scoring_automaton_ready(wait=False):
            return None

        merged_text = "\n".join([x for x in (normalized_title, normalized_content) if x])
        merged_counts = self._collect_scoring_counts(merged_text)

        marker_hits = set()
        topic_hits = set()
        problem_hits = set()
        hot_title_hits = set()

        for idx in self._scoring_category_index.get("alg_marker", []):
            if merged_counts.get(idx, 0) > 0:
                marker_hits.add(str(self._scoring_automaton.feature_specs[idx].get("pattern", "")))

        for idx in self._scoring_category_index.get("alg_topic", []):
            if merged_counts.get(idx, 0) > 0:
                topic_hits.add(str(self._scoring_automaton.feature_specs[idx].get("pattern", "")))

        for idx in self._scoring_category_index.get("alg_problem", []):
            if merged_counts.get(idx, 0) > 0:
                problem_hits.add(str(self._scoring_automaton.feature_specs[idx].get("pattern", "")))

        for idx in self._scoring_category_index.get("alg_hot_title", []):
            if merged_counts.get(idx, 0) <= 0:
                continue
            spec = self._scoring_automaton.feature_specs[idx]
            lookup_key = str(spec.get("lookup_key") or spec.get("pattern") or "")
            if lookup_key:
                hot_title_hits.add(lookup_key)

        raw_text = "\n".join([str(raw_title or ""), str(raw_content or "")])
        problem_id_hits = self._extract_alg_problem_ids(raw_text)

        marker_score = min(len(marker_hits) * self.score_alg_marker_weight, self.score_alg_marker_cap)
        topic_score = min(len(topic_hits) * self.score_alg_topic_weight, self.score_alg_topic_cap)
        problem_score = min(len(problem_hits) * self.score_alg_problem_weight, self.score_alg_problem_cap)
        problem_id_score = min(len(problem_id_hits) * self.score_alg_problem_id_weight, self.score_alg_problem_id_cap)

        hot_title_score = 0
        hot_id_score = 0
        hot_matches = []
        hot_id_hits = []

        if self.score_alg_hot_enabled and self.alg_hot_problem_entries:
            hot_title_score_raw = 0
            for title in hot_title_hits:
                hot_title_score_raw += self._score_from_hot_frequency(self.alg_hot_title_freq.get(title, 0))
            hot_title_score = min(hot_title_score_raw, self.score_alg_hot_title_cap)

            hot_id_hits = sorted(set(problem_id_hits) & set(self.alg_hot_id_freq.keys()))
            hot_id_score_raw = 0
            for problem_id in hot_id_hits:
                hot_id_score_raw += self._score_from_hot_frequency(self.alg_hot_id_freq.get(problem_id, 0))
            hot_id_score = min(hot_id_score_raw, self.score_alg_hot_id_cap)

            hot_matches = self._build_hot_match_details(sorted(hot_title_hits), hot_id_hits)

        hot_score = hot_title_score + hot_id_score
        if self.score_alg_hot_total_cap > 0:
            hot_score = min(hot_score, self.score_alg_hot_total_cap)

        score = marker_score + topic_score + problem_score + problem_id_score + hot_score
        if self.score_alg_total_cap > 0:
            score = min(score, self.score_alg_total_cap)

        return {
            "score": int(score),
            "marker_hits": sorted(marker_hits),
            "topic_hits": sorted(topic_hits),
            "problem_hits": sorted(problem_hits),
            "problem_id_hits": sorted(problem_id_hits),
            "hot_score": int(hot_score),
            "hot_title_score": int(hot_title_score),
            "hot_id_score": int(hot_id_score),
            "hot_title_hits": sorted(hot_title_hits),
            "hot_id_hits": hot_id_hits,
            "hot_matches": hot_matches,
            "marker_score": int(marker_score),
            "topic_score": int(topic_score),
            "problem_score": int(problem_score),
            "problem_id_score": int(problem_id_score),
        }

    def _get_quality_executor(self):
        workers = max(int(self.score_parallel_workers or 1), 1)
        with self._quality_executor_lock:
            if self._quality_executor is None or self._quality_executor_workers != workers:
                if self._quality_executor is not None:
                    self._quality_executor.shutdown(wait=False, cancel_futures=False)
                self._quality_executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="matcher-score")
                self._quality_executor_workers = workers
        return self._quality_executor

    def _evaluate_post_quality_parallel_worker(self, title: str, content: str, tags):
        context = getattr(self._thread_local, "context", None)
        if context is None:
            context = {}
            self._thread_local.context = context

        context["title_chars"] = len(str(title or ""))
        context["content_chars"] = len(str(content or ""))
        try:
            return self.evaluate_post_quality(title, content, tags)
        finally:
            context.clear()

    def evaluate_posts_quality_parallel(self, post_items: list) -> list:
        if not post_items:
            return []

        if (not self.score_parallel_enabled) or len(post_items) <= 1:
            return [
                self.evaluate_post_quality(
                    item.get("title", ""),
                    item.get("content", ""),
                    item.get("tags", []),
                )
                for item in post_items
            ]

        executor = self._get_quality_executor()
        futures = []
        for item in post_items:
            futures.append(
                executor.submit(
                    self._evaluate_post_quality_parallel_worker,
                    item.get("title", ""),
                    item.get("content", ""),
                    item.get("tags", []),
                )
            )

        results = [None] * len(post_items)
        for idx, future in enumerate(futures):
            try:
                results[idx] = future.result()
            except Exception as e:
                logging.warning(f"并行评分失败，已回退单条评分: {e}")
                item = post_items[idx]
                results[idx] = self.evaluate_post_quality(
                    item.get("title", ""),
                    item.get("content", ""),
                    item.get("tags", []),
                )

        return results

    def shutdown(self):
        with self._quality_executor_lock:
            if self._quality_executor is not None:
                self._quality_executor.shutdown(wait=False, cancel_futures=True)
                self._quality_executor = None
                self._quality_executor_workers = 0

        if self._builder_executor is not None:
            self._builder_executor.shutdown(wait=False, cancel_futures=True)
            self._builder_executor = None

    def _build_matcher_from_raw_patterns(self, patterns, cache_bucket: str = "default"):
        prepared_patterns = self._prepare_patterns(patterns or [], split_tokens=False, cache_bucket=cache_bucket)
        return prepared_patterns, self._build_allow_override_matcher(prepared_patterns)

    def _load_alg_library(self, file_path: str) -> dict:
        if not file_path or not os.path.exists(file_path):
            return {}

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception as e:
            logging.warning(f"读取算法词库失败，已忽略: {e}")

        return {}

    def _init_algorithm_library(self, score_filter_cfg: dict):
        if not self.score_alg_enabled:
            return

        alg_data = self._load_alg_library(self.score_alg_path)
        if not alg_data:
            logging.warning(f"算法词库未加载成功: {self.score_alg_path}")
            return

        marker_candidates = list(self.alg_marker_patterns)
        marker_candidates.extend(alg_data.get("markers", []))
        self.alg_marker_patterns = self._normalize_unique_strings(marker_candidates, min_len=2)
        self.alg_marker_matcher = self._build_allow_override_matcher(self.alg_marker_patterns)

        topic_candidates = []
        topic_candidates.extend(alg_data.get("categories", []))
        topic_candidates.extend(alg_data.get("topic_keywords", []))
        topic_candidates.extend(score_filter_cfg.get("alg_topic_keywords", []))
        self.alg_topic_patterns, self.alg_topic_matcher = self._build_matcher_from_raw_patterns(
            topic_candidates,
            cache_bucket="alg_topics",
        )

        problem_candidates = []
        problem_candidates.extend(alg_data.get("problem_titles", []))
        problem_candidates.extend(score_filter_cfg.get("alg_problem_titles", []))
        self.alg_problem_patterns, self.alg_problem_matcher = self._build_matcher_from_raw_patterns(
            problem_candidates,
            cache_bucket="alg_problems",
        )

        compacted_patterns = []
        compacted_map = {}
        seen_compacted = set()
        for title in self.alg_problem_patterns:
            compacted = title.translate(self._combine_strip_table)
            if len(compacted) < 2 or compacted in seen_compacted:
                continue
            seen_compacted.add(compacted)
            compacted_patterns.append(compacted)
            compacted_map[compacted] = title

        self.alg_problem_compacted_patterns = compacted_patterns
        self.alg_problem_compacted_matcher = self._build_allow_override_matcher(compacted_patterns)
        self.alg_problem_compacted_map = compacted_map

        ids = set()
        for item in alg_data.get("problem_ids", []):
            try:
                value = int(item)
            except Exception:
                continue
            if 1 <= value <= 9999:
                ids.add(value)

        self.alg_problem_ids = ids
        self._init_interview_hot_library(alg_data)

    def _normalize_hot_problem_entry(self, item: dict) -> dict | None:
        if not isinstance(item, dict):
            return None

        title_display = str(item.get("title", "") or "").strip()
        title = self._normalize_text(title_display)
        source = str(item.get("source", "") or "").strip()
        difficulty = str(item.get("difficulty", "") or "").strip()
        last_date = str(item.get("last_date", "") or "").strip()
        url = str(item.get("url", item.get("link", "")) or "").strip()
        frontend_id = str(item.get("frontend_id", item.get("problem_id", "")) or "").strip()

        raw_id = item.get("id")
        problem_id = None
        if raw_id is not None and str(raw_id).strip() != "":
            try:
                parsed_id = int(raw_id)
                if 1 <= parsed_id <= 9999:
                    problem_id = parsed_id
            except Exception:
                problem_id = None

        frequency = self._to_non_negative_int(
            item.get("frequency", item.get("count", item.get("freq", 0))),
            0,
        )
        if frequency <= 0:
            frequency = 1

        if not title and problem_id is None:
            return None

        return {
            "id": problem_id,
            "frontend_id": frontend_id,
            "title": title,
            "title_display": title_display,
            "frequency": frequency,
            "source": source,
            "difficulty": difficulty,
            "last_date": last_date,
            "url": url,
        }

    @staticmethod
    def _build_hot_entry_payload(entry: dict) -> dict:
        return {
            "id": entry.get("id"),
            "frontend_id": entry.get("frontend_id", ""),
            "title": entry.get("title", ""),
            "title_display": entry.get("title_display", ""),
            "frequency": entry.get("frequency", 1),
            "source": entry.get("source", ""),
            "difficulty": entry.get("difficulty", ""),
            "last_date": entry.get("last_date", ""),
            "url": entry.get("url", ""),
        }

    def _init_interview_hot_library(self, alg_data: dict):
        entries = alg_data.get("interview_hot_problems", [])
        if not isinstance(entries, list) or not entries:
            return

        normalized_entries = []
        title_freq = {}
        id_freq = {}
        title_entry = {}
        id_entry = {}

        for item in entries:
            normalized = self._normalize_hot_problem_entry(item)
            if not normalized:
                continue

            normalized_entries.append(normalized)

            title = normalized.get("title", "")
            frequency = normalized.get("frequency", 1)
            problem_id = normalized.get("id")
            payload = self._build_hot_entry_payload(normalized)

            if title:
                old_freq = title_freq.get(title, 0)
                if frequency > old_freq:
                    title_freq[title] = frequency

                old_entry = title_entry.get(title)
                title_entry[title] = self._choose_hot_entry(old_entry, payload)

            if problem_id is not None:
                old_freq = id_freq.get(problem_id, 0)
                if frequency > old_freq:
                    id_freq[problem_id] = frequency

                old_entry = id_entry.get(problem_id)
                id_entry[problem_id] = self._choose_hot_entry(old_entry, payload)

        self.alg_hot_problem_entries = normalized_entries
        self.alg_hot_title_freq = title_freq
        self.alg_hot_id_freq = id_freq
        self.alg_hot_entry_by_title = title_entry
        self.alg_hot_entry_by_id = id_entry

        hot_titles = list(title_freq.keys())
        self.alg_hot_title_patterns, self.alg_hot_title_matcher = self._build_matcher_from_raw_patterns(
            hot_titles,
            cache_bucket="alg_hot_titles",
        )

    def _search_with_matcher(self, matcher, text: str) -> set:
        if not matcher or not text:
            return set()
        try:
            return set(matcher.search(text))
        except Exception:
            return set()

    def _extract_alg_problem_ids(self, text: str) -> set:
        if not text or not self.alg_problem_ids:
            return set()

        hits = set()
        for match in re.finditer(r"(?<!\d)(\d{1,4})(?:(?:\s*[\.．、\)])|(?:\s*题)|(?=\s|$|[，。；;,:：!！?？#]))", text):
            try:
                value = int(match.group(1))
            except Exception:
                continue
            if value in self.alg_problem_ids:
                hits.add(value)

        for match in re.finditer(r"(?:lc|leetcode|力扣)\s*[-#]?\s*(\d{1,4})", text, flags=re.IGNORECASE):
            try:
                value = int(match.group(1))
            except Exception:
                continue
            if value in self.alg_problem_ids:
                hits.add(value)

        return hits

    def _evaluate_algorithm_signal(self, raw_title: str, raw_content: str, normalized_title: str, normalized_content: str) -> dict:
        if not self.score_alg_enabled:
            return {
                "score": 0,
                "marker_hits": [],
                "topic_hits": [],
                "problem_hits": [],
                "problem_id_hits": [],
                "hot_score": 0,
                "hot_title_score": 0,
                "hot_id_score": 0,
                "hot_title_hits": [],
                "hot_id_hits": [],
                "marker_score": 0,
                "topic_score": 0,
                "problem_score": 0,
                "problem_id_score": 0,
            }

        auto_signal = self._evaluate_algorithm_signal_via_automaton(
            raw_title,
            raw_content,
            normalized_title,
            normalized_content,
        )
        if auto_signal is not None:
            return auto_signal

        merged_text = "\n".join([x for x in (normalized_title, normalized_content) if x])
        merged_compacted = merged_text.translate(self._combine_strip_table)

        marker_hits = self._search_with_matcher(self.alg_marker_matcher, merged_text)
        topic_hits = self._search_with_matcher(self.alg_topic_matcher, merged_text)
        problem_hits = self._search_with_matcher(self.alg_problem_matcher, merged_text)

        compacted_hits = self._search_with_matcher(self.alg_problem_compacted_matcher, merged_compacted)
        for compacted in compacted_hits:
            mapped = self.alg_problem_compacted_map.get(compacted)
            if mapped:
                problem_hits.add(mapped)

        raw_text = "\n".join([str(raw_title or ""), str(raw_content or "")])
        problem_id_hits = self._extract_alg_problem_ids(raw_text)

        marker_score = min(len(marker_hits) * self.score_alg_marker_weight, self.score_alg_marker_cap)
        topic_score = min(len(topic_hits) * self.score_alg_topic_weight, self.score_alg_topic_cap)
        problem_score = min(len(problem_hits) * self.score_alg_problem_weight, self.score_alg_problem_cap)
        problem_id_score = min(len(problem_id_hits) * self.score_alg_problem_id_weight, self.score_alg_problem_id_cap)

        hot_title_hits = []
        hot_id_hits = []
        hot_matches = []
        hot_title_score = 0
        hot_id_score = 0
        hot_score = 0

        if self.score_alg_hot_enabled and self.alg_hot_problem_entries:
            hot_title_hits_set = self._search_with_matcher(self.alg_hot_title_matcher, merged_text)
            hot_title_hits = sorted(hot_title_hits_set)
            hot_title_score_raw = 0
            for title in hot_title_hits:
                hot_title_score_raw += self._score_from_hot_frequency(self.alg_hot_title_freq.get(title, 0))
            hot_title_score = min(hot_title_score_raw, self.score_alg_hot_title_cap)

            hot_id_hits_set = set(problem_id_hits) & set(self.alg_hot_id_freq.keys())
            hot_id_hits = sorted(hot_id_hits_set)
            hot_id_score_raw = 0
            for problem_id in hot_id_hits:
                hot_id_score_raw += self._score_from_hot_frequency(self.alg_hot_id_freq.get(problem_id, 0))
            hot_id_score = min(hot_id_score_raw, self.score_alg_hot_id_cap)

            hot_score = hot_title_score + hot_id_score
            if self.score_alg_hot_total_cap > 0:
                hot_score = min(hot_score, self.score_alg_hot_total_cap)

            hot_matches = self._build_hot_match_details(hot_title_hits, hot_id_hits)

        score = marker_score + topic_score + problem_score + problem_id_score + hot_score
        if self.score_alg_total_cap > 0:
            score = min(score, self.score_alg_total_cap)

        return {
            "score": int(score),
            "marker_hits": sorted(marker_hits),
            "topic_hits": sorted(topic_hits),
            "problem_hits": sorted(problem_hits),
            "problem_id_hits": sorted(problem_id_hits),
            "hot_score": int(hot_score),
            "hot_title_score": int(hot_title_score),
            "hot_id_score": int(hot_id_score),
            "hot_title_hits": hot_title_hits,
            "hot_id_hits": hot_id_hits,
            "hot_matches": hot_matches,
            "marker_score": int(marker_score),
            "topic_score": int(topic_score),
            "problem_score": int(problem_score),
            "problem_id_score": int(problem_id_score),
        }

    def _init_skip_matcher(self, skip_char_match: dict | None, multi_patterns: list):
        cfg = skip_char_match if isinstance(skip_char_match, dict) else {}
        enabled = bool(cfg.get("enabled", False))
        max_skips = max(int(cfg.get("max_skips", 2) or 2), 0)
        allowed_chars = str(cfg.get("allowed_chars", self._DEFAULT_SKIP_ALLOWED_CHARS) or "")

        if not enabled or not multi_patterns:
            return

        self.skip_matcher = SkipCharMatcher(multi_patterns, allowed_chars, max_skips)
        if self.skip_matcher.regex_pairs:
            logging.info(
                f"启用跳字符匹配。支持字符集长度: {len(set(allowed_chars))}，每段最大跳过: {max_skips}"
            )
        else:
            self.skip_matcher = None

    def _build_allow_override_matcher(self, patterns: list):
        if not patterns:
            return None

        single_patterns = [p for p in patterns if len(p) == 1]
        multi_patterns = [p for p in patterns if len(p) > 1]

        if single_patterns and not multi_patterns:
            return SingleCharMatcher(single_patterns)
        if not single_patterns and len(multi_patterns) == 1:
            return KMPMatcher(multi_patterns[0])
        if not single_patterns and len(multi_patterns) > 1:
            return ACAhoCorasick(
                multi_patterns,
                codec=self._create_char_id_codec(),
                transition_strategy=self.char_id_transition_strategy,
            )

        return HybridMatcher(
            single_patterns,
            multi_patterns,
            ac_codec=self._create_char_id_codec(),
            transition_strategy=self.char_id_transition_strategy,
        )

    def _try_build_native_matcher(self, multi_patterns: list):
        if self.backend in ("python", "ac-python", "pure-python"):
            return None

        if self.backend == "auto" and len(multi_patterns) < self.native_min_patterns:
            return None

        if self.backend not in ("auto", "native", "pyahocorasick", "ac-native"):
            return None

        try:
            return PyAhoCorasickMatcher(multi_patterns)
        except Exception as e:
            logging.warning(f"AC 原生后端不可用，已回退 Python 实现: {e}")
            return None

    def _prepare_patterns(self, patterns: list, split_tokens: bool = True, cache_bucket: str = "default") -> list:
        input_patterns = list(patterns or [])
        cache_path = None

        if self.pattern_cache_enabled:
            cache_path = self._build_pattern_cache_path(cache_bucket, input_patterns, split_tokens)
            cached_patterns = self._load_pattern_cache(cache_path)
            if cached_patterns is not None:
                return cached_patterns

        normalized = []
        seen = set()
        for pattern in input_patterns:
            if split_tokens:
                source_tokens = self._split_pattern_tokens(pattern)
            else:
                source_tokens = [pattern]

            for token in source_tokens:
                normalized_token = self._normalize_text(token)
                if normalized_token and normalized_token not in seen:
                    seen.add(normalized_token)
                    normalized.append(normalized_token)

        if cache_path is not None:
            self._save_pattern_cache(cache_path, normalized)

        return normalized

    def _split_pattern_tokens(self, pattern: str) -> list:
        raw = (pattern or "").strip()
        if not raw:
            return []

        parts = re.split(r"[\s,，;；、|/]+", raw)
        tokens = []
        for part in parts:
            token = part.strip().strip("[](){}<>\"'`")
            if token:
                tokens.append(token)

        return tokens

    def _normalize_text(self, text: str) -> str:
        normalized = (text or "")
        if not normalized:
            return ""

        if self.norm_enable_nfkc:
            normalized = unicodedata.normalize("NFKC", normalized)

        normalized = normalized.lower()

        if self.norm_unify_punctuation:
            normalized = normalized.translate(self._PUNCT_VARIANT_MAP)

        if self.norm_map_digit_variants:
            normalized = normalized.translate(self._DIGIT_VARIANT_MAP)

        if self.norm_strip_zero_width:
            normalized = normalized.replace("\u200b", "").replace("\ufeff", "")

        if self.norm_fold_spaces:
            normalized = self._space_regex.sub(" ", normalized).strip()

        if self.norm_semantic_aliases:
            for src, dst in self.norm_semantic_aliases:
                src_norm = src.lower()
                dst_norm = dst.lower()
                if src_norm:
                    normalized = normalized.replace(src_norm, dst_norm)

        if self._repeat_regex is not None:
            normalized = self._repeat_regex.sub(r"\1", normalized)

        return normalized

    def _search_any_with_optional_skip(self, text: str) -> bool:
        if self.matcher and self.matcher.search_any(text):
            return True
        if self.skip_matcher and self.skip_matcher.search_any(text):
            return True
        return False

    def _search_with_optional_skip(self, text: str) -> set:
        matched = set()
        if self.matcher:
            matched.update(self.matcher.search(text))
        if self.skip_matcher:
            matched.update(self.skip_matcher.search(text))
        return matched

    def _is_title_force_hit(self, normalized_title: str) -> bool:
        if not self.title_force_matcher:
            return True
        if not normalized_title:
            return False
        return self.title_force_matcher.search_any(normalized_title)

    def _content_combine_matches(self, normalized_content: str) -> set:
        matched = set()
        if not self.content_combine_compacted or not normalized_content:
            return matched

        compacted_content = normalized_content.translate(self._combine_strip_table)
        if not compacted_content:
            return matched

        for pattern in self.content_combine_compacted:
            if pattern in compacted_content:
                matched.add(pattern)
        return matched

    def _is_allow_override_hit_normalized(self, normalized_title: str, normalized_content: str) -> bool:
        if not self.allow_override_matcher:
            return False
        if normalized_title and self.allow_override_matcher.search_any(normalized_title):
            return True
        if normalized_content and self.allow_override_matcher.search_any(normalized_content):
            return True
        return False

    def evaluate_post_quality(self, title: str, content: str, tags: list | None = None) -> dict:
        if not self.score_enabled:
            return {
                "enabled": False,
                "score": 0,
                "threshold": self.score_threshold,
                "passed": True,
                "breakdown": {},
            }

        normalized_title = self._normalize_text(title)
        normalized_content = self._normalize_text(content)

        title_matched = self._search_with_optional_skip(normalized_title)
        content_matched = self._search_with_optional_skip(normalized_content)
        if not content_matched:
            content_matched = self._content_combine_matches(normalized_content)

        title_score = min(len(title_matched) * self.score_title_hit_weight, self.score_title_hit_cap)
        content_score = min(len(content_matched) * self.score_content_hit_weight, self.score_content_hit_cap)

        weighted_score = 0
        weighted_hits = []
        automaton_weighted = self._evaluate_weighted_keywords_via_automaton(normalized_title, normalized_content)
        if automaton_weighted is not None:
            weighted_score, weighted_hits = automaton_weighted
        else:
            for rule in self.score_weighted_keywords:
                keyword = rule["keyword"]
                scope = rule["scope"]
                count_mode = rule["count"]
                weight = int(rule["weight"])

                hit_count = 0
                if scope in ("title", "both"):
                    hit_count += self._count_keyword_hits(normalized_title, keyword)
                if scope in ("content", "both"):
                    hit_count += self._count_keyword_hits(normalized_content, keyword)

                if hit_count <= 0:
                    continue

                gain = weight if count_mode == "once" else weight * hit_count
                if weighted_score + gain > self.score_weighted_keyword_cap:
                    gain = max(self.score_weighted_keyword_cap - weighted_score, 0)

                if gain <= 0:
                    break

                weighted_score += gain
                weighted_hits.append(
                    {
                        "keyword": keyword,
                        "scope": scope,
                        "count": hit_count,
                        "score": gain,
                    }
                )
                if weighted_score >= self.score_weighted_keyword_cap:
                    break

        alg_signal = self._evaluate_algorithm_signal(title, content, normalized_title, normalized_content)
        alg_score = int(alg_signal.get("score", 0))

        content_chars = len(re.sub(r"\s+", "", normalized_content))
        if content_chars <= 0:
            length_score = 0
        else:
            ratio = min(content_chars / float(self.score_length_ideal_chars), 1.0)
            length_score = int(round(self.score_length_max_score * ratio))

        if 0 < content_chars < self.score_length_min_chars:
            length_score = min(length_score, max(self.score_length_max_score // 4, 4))

        short_penalty = 0
        if 0 < content_chars < self.score_short_content_chars:
            short_penalty = self.score_short_content_penalty

        normalized_tags = self._normalize_tag_items(tags)
        tail_hashtags = self._extract_tail_hashtags(content)
        all_tail_tags = self._normalize_tag_items(normalized_tags + tail_hashtags)

        tail_tag_penalty = min(len(all_tail_tags) * self.score_tail_tag_penalty, self.score_tail_tag_penalty_cap)

        tail_drain_penalty = 0
        tail_drain_hits = []
        for tag in all_tail_tags:
            for drain_word, penalty in self.score_tail_drain_word_penalties.items():
                if drain_word and drain_word in tag:
                    tail_drain_penalty += penalty
                    tail_drain_hits.append(drain_word)
                    break

        tail_drain_penalty = min(tail_drain_penalty, self.score_tail_drain_penalty_cap)

        final_score = (
            self.score_base
            + title_score
            + content_score
            + weighted_score
            + alg_score
            + length_score
            - short_penalty
            - tail_tag_penalty
            - tail_drain_penalty
        )
        final_score = max(int(final_score), 0)

        passed = final_score >= self.score_threshold
        breakdown = {
            "base_score": self.score_base,
            "title_score": title_score,
            "content_score": content_score,
            "weighted_keyword_score": weighted_score,
            "alg_score": alg_score,
            "alg_marker_score": alg_signal.get("marker_score", 0),
            "alg_topic_score": alg_signal.get("topic_score", 0),
            "alg_problem_score": alg_signal.get("problem_score", 0),
            "alg_problem_id_score": alg_signal.get("problem_id_score", 0),
            "alg_hot_score": alg_signal.get("hot_score", 0),
            "alg_hot_title_score": alg_signal.get("hot_title_score", 0),
            "alg_hot_id_score": alg_signal.get("hot_id_score", 0),
            "length_score": length_score,
            "short_penalty": short_penalty,
            "tail_tag_penalty": tail_tag_penalty,
            "tail_drain_penalty": tail_drain_penalty,
            "content_chars": content_chars,
            "title_matches": sorted(title_matched),
            "content_matches": sorted(content_matched),
            "weighted_hits": weighted_hits,
            "alg_marker_hits": alg_signal.get("marker_hits", []),
            "alg_topic_hits": alg_signal.get("topic_hits", []),
            "alg_problem_hits": alg_signal.get("problem_hits", []),
            "alg_problem_id_hits": alg_signal.get("problem_id_hits", []),
            "alg_hot_title_hits": alg_signal.get("hot_title_hits", []),
            "alg_hot_id_hits": alg_signal.get("hot_id_hits", []),
            "alg_hot_matches": alg_signal.get("hot_matches", []),
            "tail_tags": all_tail_tags,
            "tail_drain_hits": tail_drain_hits,
        }

        if self.score_debug_log and logging.getLogger().isEnabledFor(logging.INFO):
            logging.info(
                f"[{self.algo_name}] 质量评分: {final_score}/{self.score_threshold} | 题分={title_score} 文分={content_score} 词分={weighted_score} 算法分={alg_score} 高频题分={alg_signal.get('hot_score', 0)} 长度分={length_score} 惩罚={short_penalty + tail_tag_penalty + tail_drain_penalty}"
            )

        return {
            "enabled": True,
            "score": final_score,
            "threshold": self.score_threshold,
            "passed": passed,
            "breakdown": breakdown,
        }

    def normalize_text(self, text: str) -> str:
        return self._normalize_text(text)

    def is_allow_override_hit(self, title: str, content: str = "") -> bool:
        normalized_title = self._normalize_text(title)
        normalized_content = self._normalize_text(content)
        return self._is_allow_override_hit_normalized(normalized_title, normalized_content)

    def match(self, title: str, content: str) -> bool:
        normalized_title = self._normalize_text(title)
        normalized_content = self._normalize_text(content)

        if self._is_allow_override_hit_normalized(normalized_title, normalized_content):
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"[{self.algo_name}] 白名单覆盖命中，已放行。")
            return True

        if not self._is_title_force_hit(normalized_title):
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f"[{self.algo_name}] 标题未命中强规则，已拦截。标题: {title[:20]}...")
            return False

        if not self.matcher:
            return True

        title_hit = self._search_any_with_optional_skip(normalized_title)
        if not title_hit:
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f"[{self.algo_name}] 标题匹配失败。标题中命中的关键字: [] | 目标标题: {title[:20]}...")
            return False

        content_hit = self._search_any_with_optional_skip(normalized_content)
        combine_matched = set()
        if not content_hit:
            combine_matched = self._content_combine_matches(normalized_content)
            content_hit = bool(combine_matched)

        result = title_hit and content_hit

        if logging.getLogger().isEnabledFor(logging.INFO) and result:
            title_matched = self._search_with_optional_skip(normalized_title)
            content_matched = self._search_with_optional_skip(normalized_content)
            if combine_matched:
                content_matched.update(combine_matched)
            logging.info(f"[{self.algo_name}] 匹配成功！标题命中: {list(title_matched)} | 正文命中: {list(content_matched)}")
        elif logging.getLogger().isEnabledFor(logging.DEBUG) and not result:
            content_matched = self._search_with_optional_skip(normalized_content)
            if combine_matched:
                content_matched.update(combine_matched)
            logging.debug(f"[{self.algo_name}] 标题通过但正文匹配失败。正文命中: {list(content_matched)}")

        return result
