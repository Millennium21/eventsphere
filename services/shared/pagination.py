"""Offset/limit pagination shared by every list endpoint.

Offset/limit is simple and matches the "pagination" requirement; for scalability,
keyset (cursor) pagination is the natural next step once any single listing gets
deep enough that OFFSET starts costing real query time.
"""

from __future__ import annotations

from typing import TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1, description="1-indexed page number")
    page_size: int = Field(default=20, ge=1, le=100)

    @property
    def limit(self) -> int:
        return self.page_size

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


class Page[T](BaseModel):
    items: list[T]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        if self.page_size <= 0:
            return 0
        return (self.total + self.page_size - 1) // self.page_size

    @classmethod
    def create(cls, items: list[T], total: int, params: PaginationParams) -> Page[T]:
        return cls(items=items, total=total, page=params.page, page_size=params.page_size)
