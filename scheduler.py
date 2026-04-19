import heapq
from dataclasses import dataclass
from typing import Iterable, List


@dataclass(order=True, frozen=True)
class PageTask:
    priority: float
    page: int


class SearchPageScheduler:
    SUPPORTED_STRATEGIES = {"bfs", "dfs", "best_first"}

    def __init__(self, page_limit: int, strategy: str = "bfs"):
        self.page_limit = max(int(page_limit or 0), 0)
        normalized = str(strategy or "bfs").strip().lower()
        self.strategy = normalized if normalized in self.SUPPORTED_STRATEGIES else "bfs"

    def iter_pages(self) -> Iterable[int]:
        if self.page_limit <= 0:
            return []

        if self.strategy == "dfs":
            return list(range(self.page_limit, 0, -1))

        if self.strategy == "best_first":
            return self._iter_best_first_pages()

        return list(range(1, self.page_limit + 1))

    def _iter_best_first_pages(self) -> List[int]:
        tasks = []
        for page in range(1, self.page_limit + 1):
            freshness_score = float(page)
            exploration_bonus = 0.15 * float(page % 5)
            priority = freshness_score + exploration_bonus
            heapq.heappush(tasks, PageTask(page=page, priority=priority))

        ordered = []
        while tasks:
            task = heapq.heappop(tasks)
            ordered.append(task.page)

        return ordered
