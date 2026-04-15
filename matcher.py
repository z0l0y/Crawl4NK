import collections
import logging

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


class ACNode:
    def __init__(self, state: int = 0, character: str = ''):
        self.state = state
        self.character = character
        self.children = {}
        self.fail = None
        self.keywords = []


class ACAhoCorasick:
    def __init__(self, patterns: list):
        self.root = ACNode()
        self.node_count = 0
        self._build_trie(patterns)
        self._build_fail_pointers()

    def _build_trie(self, patterns: list):
        for word in patterns:
            if not word:
                continue
            curr = self.root
            for char in word:
                if char not in curr.children:
                    self.node_count += 1
                    curr.children[char] = ACNode(self.node_count, char)
                curr = curr.children[char]
            if word not in curr.keywords:
                curr.keywords.append(word)

    def _build_fail_pointers(self):
        queue = collections.deque()
        for child in self.root.children.values():
            child.fail = self.root
            queue.append(child)

        while queue:
            curr = queue.popleft()
            for char, next_node in curr.children.items():
                queue.append(next_node)

                fail_node = curr.fail
                while fail_node and char not in fail_node.children:
                    fail_node = fail_node.fail
                    if fail_node == self.root:
                        break

                if fail_node and char in fail_node.children:
                    next_node.fail = fail_node.children[char]
                    for kw in next_node.fail.keywords:
                        if kw not in next_node.keywords:
                            next_node.keywords.append(kw)
                else:
                    next_node.fail = self.root

    def search(self, data: str) -> set:
        match_result = set()
        node = self.root
        for char in data:
            while node and char not in node.children:
                node = node.fail
                if node is None or node.state == 0:
                    break

            if node and char in node.children:
                node = node.children[char]
                if node.keywords:
                    match_result.update(node.keywords)
            else:
                node = self.root

        return match_result


class TextMatcher:
    def __init__(self, patterns: list):
        self.patterns = [p for p in patterns if p]
        self.use_kmp = (len(self.patterns) == 1)
        if self.use_kmp:
            self.matcher = KMPMatcher(self.patterns[0])
            logging.info(f"初始化 KMP 机制，当前加载关键字: {self.patterns}")
        elif len(self.patterns) > 1:
            self.matcher = ACAhoCorasick(self.patterns)
            logging.info(f"初始化 AC 自动机机制。关键字数量: {len(self.patterns)}，当前加载关键字: {self.patterns}")
        else:
            self.matcher = None
            logging.info("警告：未提供任何匹配模式关键字。")

    def match(self, title: str, content: str) -> bool:
        if not self.matcher:
             return True

        algo_name = "KMP" if self.use_kmp else "AC Automaton"
        
        title_matched = self.matcher.search(title)
        if not title_matched:
            logging.debug(f"[{algo_name}] 标题匹配失败。标题中命中的关键字: [] | 目标标题: {title[:20]}...")
            return False

        content_matched = self.matcher.search(content)
        result = (len(title_matched) > 0) and (len(content_matched) > 0)
        
        if result:
            logging.info(f"[{algo_name}] 匹配成功！标题命中: {list(title_matched)} | 正文命中: {list(content_matched)}")
        else:
            logging.debug(f"[{algo_name}] 标题通过但正文匹配失败。正文命中: {list(content_matched)}")
            
        return result
