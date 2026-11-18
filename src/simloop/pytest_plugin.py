"""pytest integration, registered through the ``pytest11`` entry point.

Adds command-line options that override every ``@simloop.test``::

    pytest --simloop-seeds 5000          # explore more seeds
    pytest --simloop-seed 1234 -k name   # rerun one seed with a full trace
    pytest --simloop-no-shrink           # report the original failing run
"""

from __future__ import annotations

from pathlib import Path

import pytest

from . import _testing


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("simloop", "deterministic simulation testing")
    group.addoption("--simloop-seeds", type=int, default=None, help="seeds per @simloop.test")
    group.addoption("--simloop-start", type=int, default=0, help="first seed to explore")
    group.addoption("--simloop-seed", type=int, default=None, help="run exactly this seed")
    group.addoption("--simloop-no-shrink", action="store_true", help="do not shrink failing runs")
    group.addoption(
        "--simloop-no-db", action="store_true", help="do not read or write .simloop/examples"
    )
    group.addoption(
        "--simloop-confirm",
        type=int,
        default=200,
        help="fifo seeds searched to confirm a random-scheduler failure (0: skip)",
    )
    group.addoption(
        "--simloop-workers",
        type=int,
        default=None,
        help="worker processes per @simloop.test exploration (0: one per CPU)",
    )
    group.addoption(
        "--simloop-html",
        metavar="DIR",
        default=None,
        help="write an HTML timeline of each failing run into DIR",
    )
    group.addoption(
        "--simloop-trace-last", type=int, default=30, help="trace events shown on failure"
    )


def pytest_configure(config: pytest.Config) -> None:
    settings = _testing.settings
    settings.seeds = config.getoption("simloop_seeds")
    settings.start = config.getoption("simloop_start")
    settings.seed = config.getoption("simloop_seed")
    settings.shrink = False if config.getoption("simloop_no_shrink") else None
    settings.trace_last = config.getoption("simloop_trace_last")
    settings.confirm = config.getoption("simloop_confirm")
    settings.workers = config.getoption("simloop_workers")
    html = config.getoption("simloop_html")
    settings.html = None if html is None else Path(html)
    root = Path(config.rootpath)
    settings.database = (
        None if config.getoption("simloop_no_db") else root / ".simloop" / "examples"
    )


def pytest_unconfigure(config: pytest.Config) -> None:
    _testing.settings = _testing.TestSettings()
