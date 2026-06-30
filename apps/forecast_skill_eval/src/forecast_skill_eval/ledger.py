from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ExclusionLedgerEntry:
    """A unified record for one excluded scoring candidate."""

    stage: str
    reason: str
    code: str | None = None
    period_key: int | None = None
    year: int | None = None


@dataclass
class ExclusionLedger:
    """Collect and summarize exclusions from observed, norm, and pair stages."""

    _entries: list[ExclusionLedgerEntry] = field(default_factory=list)

    @property
    def entries(self) -> tuple[ExclusionLedgerEntry, ...]:
        """Return recorded exclusions as an immutable tuple."""
        return tuple(self._entries)

    def add(
        self,
        *,
        stage: str,
        reason: str,
        code: str | None = None,
        period_key: int | None = None,
        year: int | None = None,
    ) -> None:
        """Record one exclusion."""
        if not stage:
            raise ValueError("stage must not be empty")
        if not reason:
            raise ValueError("reason must not be empty")
        self._entries.append(
            ExclusionLedgerEntry(
                stage=stage,
                reason=reason,
                code=code,
                period_key=period_key,
                year=year,
            )
        )

    def merge(
        self,
        other: ExclusionLedger | Iterable[Any],
        *,
        stage: str | None = None,
    ) -> None:
        """Fold another ledger or stage-specific reason objects into this ledger."""
        if isinstance(other, ExclusionLedger):
            for entry in other.entries:
                self.add(
                    stage=stage or entry.stage,
                    reason=entry.reason,
                    code=entry.code,
                    period_key=entry.period_key,
                    year=entry.year,
                )
            return

        for entry in other:
            self.add(
                stage=stage or _required_attr(entry, "stage"),
                reason=_required_attr(entry, "reason"),
                code=getattr(entry, "code", None),
                period_key=getattr(entry, "period_key", None),
                year=getattr(entry, "year", None),
            )

    def counts_by_stage_reason(self) -> dict[tuple[str, str], int]:
        """Return exclusion counts keyed by ``(stage, reason)``."""
        counts = Counter((entry.stage, entry.reason) for entry in self._entries)
        return dict(counts)


def _required_attr(entry: Any, name: str) -> str:
    value = getattr(entry, name, None)
    if not value:
        raise ValueError(f"merged ledger entry is missing {name}")
    return str(value)
