import heapq
from dataclasses import dataclass
from typing import Iterable, List


@dataclass(order=True, frozen=True)
class PageTask:
    priority: float
    page: int


class SearchPageScheduler:
    SUPPORTED_STRATEGIES = {"bfs", "dfs", "best_first"}

    def __init__(
        self,
        page_limit: int,
        strategy: str = "bfs",
        best_first_frontload: int = 3,
        best_first_explore_stride: int = 5,
    ):
        self.page_limit = max(int(page_limit or 0), 0)
        normalized = str(strategy or "bfs").strip().lower()
        self.strategy = normalized if normalized in self.SUPPORTED_STRATEGIES else "bfs"

        try:
            frontload = int(best_first_frontload)
        except (TypeError, ValueError):
            frontload = 3
        try:
            stride = int(best_first_explore_stride)
        except (TypeError, ValueError):
            stride = 5

        self.best_first_frontload = max(frontload, 1)
        self.best_first_explore_stride = max(stride, 2)

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
        frontload = min(self.best_first_frontload, self.page_limit)
        for page in range(1, self.page_limit + 1):
            if page <= frontload:
                priority = float(page) / 100.0
            else:
                offset = page - frontload - 1
                bucket = offset % self.best_first_explore_stride
                layer = offset // self.best_first_explore_stride
                priority = float(frontload + bucket) + (float(layer) / 100.0)
            heapq.heappush(tasks, PageTask(page=page, priority=priority))

        ordered = []
        while tasks:
            task = heapq.heappop(tasks)
            ordered.append(task.page)

        return ordered
