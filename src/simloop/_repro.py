"""Saved reproductions: everything needed to replay one failing run exactly."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ._runtime import MainFunction, RunResult, SimConfig, run

FORMAT_VERSION = 1


@dataclass
class Reproduction:
    """A failing run as data: seed, config and (optionally edited) choices.

    ``choices`` of ``None`` means "draw from the seed", which is how an
    unshrunk failure reproduces. A shrunk failure carries its choice sequence.
    """

    seed: int
    config: SimConfig = field(default_factory=SimConfig)
    choices: list[int] | None = None
    target: str | None = None
    """``module:function`` the run came from, when known."""
    signature: str | None = None
    message: str | None = None

    @classmethod
    def from_result(cls, result: RunResult[Any], target: str | None = None) -> Reproduction:
        failure = result.failure
        return cls(
            seed=result.seed,
            config=result.config.replace(keep_trace=True),
            choices=list(result.choices) if result.replayed else None,
            target=target,
            signature=failure.signature if failure else None,
            message=failure.message if failure else None,
        )

    def run(self, main: MainFunction[Any], **changes: Any) -> RunResult[Any]:
        config = self.config.replace(**changes) if changes else self.config
        return run(main, seed=self.seed, config=config, replay=self.choices)

    def to_dict(self) -> dict[str, Any]:
        return {
            "simloop": FORMAT_VERSION,
            "target": self.target,
            "seed": self.seed,
            "signature": self.signature,
            "message": self.message,
            "config": self.config.to_dict(),
            "choices": self.choices,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Reproduction:
        version = data.get("simloop")
        if version != FORMAT_VERSION:
            raise ValueError(f"unsupported reproduction format {version!r}")
        choices = data.get("choices")
        return cls(
            seed=int(data["seed"]),
            config=SimConfig.from_dict(data.get("config", {})),
            choices=None if choices is None else [int(c) for c in choices],
            target=data.get("target"),
            signature=data.get("signature"),
            message=data.get("message"),
        )

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2) + "\n")
        return path

    @classmethod
    def load(cls, path: str | Path) -> Reproduction:
        return cls.from_dict(json.loads(Path(path).read_text()))

    def filename(self) -> str:
        slug = re.sub(r"[^A-Za-z0-9]+", "-", self.signature or "run").strip("-").lower()
        return f"{slug[:60]}-seed{self.seed}.json"
