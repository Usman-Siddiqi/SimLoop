"""Command-line interface: ``simloop run``, ``explore`` and ``replay``."""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ._explore import FailureGroup, explore
from ._repro import Reproduction
from ._runtime import MainFunction, RunResult, SimConfig, run
from ._selfcheck import check_determinism
from ._shrink import shrink
from .scheduling import resolve_scheduler

_HASHSEED_ENV = "PYTHONHASHSEED"
DEFAULT_SAVE_DIR = Path(".simloop") / "failures"


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


def _scheduler_name(value: str) -> str:
    try:
        resolve_scheduler(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return value


def _config(args: argparse.Namespace) -> SimConfig:
    return SimConfig(
        scheduler=args.scheduler,
        timer_jitter=args.jitter,
        max_steps=args.max_steps or None,
        max_time=args.max_time,
        step_time=args.step_time,
    )


def _reproduce_command(args: argparse.Namespace, seed: int) -> str:
    parts = ["simloop", "run", args.target, "--seed", str(seed)]
    if args.scheduler != "random":
        parts += ["--scheduler", args.scheduler]
    if args.jitter:
        parts += ["--jitter", repr(args.jitter)]
    if args.max_time is not None:
        parts += ["--max-time", repr(args.max_time)]
    if args.max_steps != 1_000_000:
        parts += ["--max-steps", str(args.max_steps)]
    if args.step_time:
        parts += ["--step-time", repr(args.step_time)]
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


def _shrink_and_save(
    args: argparse.Namespace, main: MainFunction[Any], failing: RunResult[Any]
) -> RunResult[Any]:
    """Shrink ``failing`` if asked, save a reproduction, and return the run to show."""
    shown = failing
    if args.shrink:
        result = shrink(main, failing, max_runs=args.shrink_runs, time_budget=args.shrink_time)
        print(result.summary())
        shown = result.shrunk
    if args.save or args.save_dir is not None:
        repro = Reproduction.from_result(shown, target=args.target)
        path = repro.save(Path(args.save_dir or DEFAULT_SAVE_DIR) / repro.filename())
        print(f"saved reproduction: {path}")
        print(f"replay with: simloop replay {path}")
    return shown


def _cmd_run(args: argparse.Namespace) -> int:
    main = load_target(args.target)
    result = run(main, seed=args.seed, config=_config(args))
    if not result.ok:
        result = _shrink_and_save(args, main, result)
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
        confirm=args.confirm,
        workers=args.workers,
    )
    print(report.summary())
    if report.ok:
        return 0
    for group in report.groups:
        print(f"\nreproduce [{group.kind}]: {_reproduce_command(args, group.seeds[0])}")
    shown = [_explain_group(args, main, group) for group in report.groups]
    first = shown[0]
    if first is not None and args.trace_last:
        print()
        _print_failure(first, args.trace_last)
    return 1


def _explain_group(
    args: argparse.Namespace, main: MainFunction[Any], group: FailureGroup
) -> RunResult[Any] | None:
    if group.example is None or (not args.shrink and not args.save and args.save_dir is None):
        return group.example
    print(f"\n[{group.kind}] {group.signature}")
    return _shrink_and_save(args, main, group.example)


def _cmd_check(args: argparse.Namespace) -> int:
    main = load_target(args.target)
    config = _config(args)
    for seed in range(args.seed, args.seed + args.seeds):
        divergence = check_determinism(main, seed, config=config, runs=args.runs)
        if divergence is not None:
            print(divergence.format())
            return 1
    print(f"deterministic: {args.seeds} seed(s), {args.runs} runs each")
    return 0


def _cmd_replay(args: argparse.Namespace) -> int:
    repro = Reproduction.load(args.file)
    target = args.target or repro.target
    if target is None:
        raise SystemExit("error: the reproduction names no target; pass --target")
    result = repro.run(load_target(target))
    if args.trace:
        print(result.trace.format())
        print()
    if result.ok:
        print(f"ok (seed {result.seed}): the saved failure no longer reproduces")
        return 0
    if repro.signature not in (None, result.failure and result.failure.signature):
        print(f"note: failed differently than saved ({repro.signature})")
    _print_failure(result, 0 if args.trace else args.trace_last)
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simloop", description="Deterministic simulation testing for asyncio."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("target", help="coroutine function, as module:function or file.py:function")
    common.add_argument(
        "--scheduler",
        type=_scheduler_name,
        default="random",
        help="random (default), fifo (asyncio's own order), pct or pct:DEPTH",
    )
    common.add_argument("--jitter", type=float, default=0.0, help="max timer jitter (seconds)")
    common.add_argument("--max-steps", type=int, default=1_000_000, help="0 for no limit")
    common.add_argument("--max-time", type=float, default=None, help="simulated seconds")
    common.add_argument(
        "--step-time", type=float, default=0.0, help="simulated seconds each callback takes"
    )
    common.add_argument(
        "--trace-last", type=int, default=30, help="trace events to show on failure"
    )

    found = argparse.ArgumentParser(add_help=False)
    found.add_argument(
        "--shrink", action="store_true", help="minimize failures before reporting them"
    )
    found.add_argument("--shrink-runs", type=int, default=1000, help="run budget per failure")
    found.add_argument("--shrink-time", type=float, default=60.0, help="wall seconds per failure")
    found.add_argument("--save", action="store_true", help="save reproduction files")
    found.add_argument(
        "--save-dir", default=None, help=f"where --save writes (default {DEFAULT_SAVE_DIR})"
    )

    run_parser = sub.add_parser("run", parents=[common, found], help="run one seed")
    run_parser.add_argument("--seed", type=int, default=0)
    run_parser.add_argument("--trace", action="store_true", help="print the full trace")
    run_parser.set_defaults(handler=_cmd_run)

    explore_parser = sub.add_parser("explore", parents=[common, found], help="run many seeds")
    explore_parser.add_argument("--seeds", type=int, default=1000, help="number of seeds")
    explore_parser.add_argument("--start", type=int, default=0, help="first seed")
    explore_parser.add_argument("--max-failures", type=int, default=None)
    explore_parser.add_argument("--time-budget", type=float, default=None, help="wall seconds")
    explore_parser.add_argument(
        "--confirm",
        type=int,
        default=200,
        help="seeds to search with asyncio's own order for each random-scheduler failure (0: skip)",
    )
    explore_parser.add_argument(
        "-j",
        "--workers",
        type=int,
        default=1,
        help="worker processes (0: one per CPU); the report does not depend on it",
    )
    explore_parser.set_defaults(handler=_cmd_explore)

    check_parser = sub.add_parser(
        "check", parents=[common], help="rerun seeds and report the first divergence"
    )
    check_parser.add_argument("--seed", type=int, default=0, help="first seed")
    check_parser.add_argument("--seeds", type=int, default=1, help="number of seeds")
    check_parser.add_argument("--runs", type=int, default=3, help="runs per seed")
    check_parser.set_defaults(handler=_cmd_check)

    replay_parser = sub.add_parser("replay", help="rerun a saved reproduction file")
    replay_parser.add_argument("file", help="JSON file written by --save")
    replay_parser.add_argument("--target", default=None, help="override the saved target")
    replay_parser.add_argument("--trace", action="store_true", help="print the full trace")
    replay_parser.add_argument("--trace-last", type=int, default=30)
    replay_parser.set_defaults(handler=_cmd_replay)
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
