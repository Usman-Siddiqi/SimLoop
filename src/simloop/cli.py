"""Command-line interface: ``simloop run`` and ``simloop explore``."""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ._explore import explore
from ._runtime import MainFunction, RunResult, SimConfig, run

_HASHSEED_ENV = "PYTHONHASHSEED"


def load_target(spec: str) -> MainFunction[Any]:
    """Resolve ``module:function`` or ``path/to/file.py:function``."""
    module_part, sep, attr = spec.rpartition(":")
    if not sep or not module_part or not attr:
        raise SystemExit(f"error: target must look like 'module:function', got {spec!r}")
    if module_part.endswith(".py") or os.sep in module_part:
        path = Path(module_part).resolve()
        if not path.is_file():
            raise SystemExit(f"error: no such file: {module_part}")
        sys.path.insert(0, str(path.parent))
        module_spec = importlib.util.spec_from_file_location(path.stem, path)
        if module_spec is None or module_spec.loader is None:
            raise SystemExit(f"error: cannot import {module_part}")
        module = importlib.util.module_from_spec(module_spec)
        sys.modules[path.stem] = module
        module_spec.loader.exec_module(module)
    else:
        sys.path.insert(0, os.getcwd())
        module = importlib.import_module(module_part)
    try:
        target = getattr(module, attr)
    except AttributeError:
        raise SystemExit(f"error: {module_part} has no attribute {attr!r}") from None
    if not callable(target):
        raise SystemExit(f"error: {spec} is not callable")
    return target  # type: ignore[no-any-return]


def _config(args: argparse.Namespace) -> SimConfig:
    return SimConfig(
        scheduler=args.scheduler,
        timer_jitter=args.jitter,
        max_steps=args.max_steps or None,
        max_time=args.max_time,
    )


def _reproduce_command(args: argparse.Namespace, seed: int) -> str:
    parts = ["simloop", "run", args.target, "--seed", str(seed)]
    if args.scheduler != "random":
        parts += ["--scheduler", args.scheduler]
    if args.jitter:
        parts += ["--jitter", repr(args.jitter)]
    if args.max_time is not None:
        parts += ["--max-time", repr(args.max_time)]
    return " ".join(parts)


def _print_failure(result: RunResult[Any], trace_last: int) -> None:
    assert result.failure is not None
    print(f"FAILED (seed {result.seed}) after {result.steps} steps, t={result.sim_time:.6f}s")
    print()
    print(result.failure.format())
    if result.trace.keep and trace_last:
        print()
        print(f"Last {trace_last} trace events:")
        print(result.trace.format(last=trace_last))


def _cmd_run(args: argparse.Namespace) -> int:
    main = load_target(args.target)
    result = run(main, seed=args.seed, config=_config(args))
    if args.trace:
        print(result.trace.format())
        print()
    if result.ok:
        print(
            f"ok (seed {result.seed}): {result.steps} steps, "
            f"{result.sim_time:.6f}s simulated in {result.wall_time:.3f}s"
        )
        print(f"trace digest {result.digest}")
        return 0
    _print_failure(result, 0 if args.trace else args.trace_last)
    print()
    print(f"trace digest {result.digest}")
    return 1


def _cmd_explore(args: argparse.Namespace) -> int:
    main = load_target(args.target)
    report = explore(
        main,
        seeds=range(args.start, args.start + args.seeds),
        config=_config(args),
        max_failures=args.max_failures,
        time_budget=args.time_budget,
    )
    print(report.summary())
    if report.ok:
        return 0
    for group in report.groups:
        print(f"\nreproduce [{group.kind}]: {_reproduce_command(args, group.seeds[0])}")
    first = report.groups[0].example
    if first is not None and args.trace_last:
        print()
        _print_failure(first, args.trace_last)
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simloop", description="Deterministic simulation testing for asyncio."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("target", help="coroutine function, as module:function or file.py:function")
    common.add_argument("--scheduler", choices=["random", "fifo"], default="random")
    common.add_argument("--jitter", type=float, default=0.0, help="max timer jitter (seconds)")
    common.add_argument("--max-steps", type=int, default=1_000_000, help="0 for no limit")
    common.add_argument("--max-time", type=float, default=None, help="simulated seconds")
    common.add_argument(
        "--trace-last", type=int, default=30, help="trace events to show on failure"
    )

    run_parser = sub.add_parser("run", parents=[common], help="run one seed")
    run_parser.add_argument("--seed", type=int, default=0)
    run_parser.add_argument("--trace", action="store_true", help="print the full trace")
    run_parser.set_defaults(handler=_cmd_run)

    explore_parser = sub.add_parser("explore", parents=[common], help="run many seeds")
    explore_parser.add_argument("--seeds", type=int, default=1000, help="number of seeds")
    explore_parser.add_argument("--start", type=int, default=0, help="first seed")
    explore_parser.add_argument("--max-failures", type=int, default=None)
    explore_parser.add_argument("--time-budget", type=float, default=None, help="wall seconds")
    explore_parser.set_defaults(handler=_cmd_explore)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    if argv is None and _HASHSEED_ENV not in os.environ:
        # String hashing is randomized per process, which changes set and dict
        # iteration order in the code under test. Pin it so seeds reproduce
        # across invocations.
        os.environ[_HASHSEED_ENV] = "0"
        os.execv(sys.executable, [sys.executable, "-m", "simloop", *sys.argv[1:]])
    args = build_parser().parse_args(argv)
    code: int = args.handler(args)
    return code


if __name__ == "__main__":
    sys.exit(main())
