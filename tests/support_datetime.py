"""Imports the datetime classes by name, like most application code does."""

from __future__ import annotations

from datetime import UTC, date, datetime


def stamp() -> tuple[str, str, str]:
    return (
        datetime.now(UTC).isoformat(),
        date.today().isoformat(),
        str(type(datetime.now())),
    )
