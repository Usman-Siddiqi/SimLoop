"""SimLoop: deterministic simulation testing for Python asyncio."""

from . import faults
from ._choices import ChoiceSource
from ._explore import ExploreReport, FailureGroup, confirm_feasible, explore
from ._loop import SimEventLoop
from ._properties import Property
from ._repro import Reproduction
from ._runtime import (
    Failure,
    RunResult,
    SimConfig,
    Simulation,
    always,
    current,
    current_host,
    eventually,
    in_simulation,
    log,
    network,
    now,
    rng,
    run,
)
from ._selfcheck import Divergence, check_determinism
from ._shrink import ShrinkResult, shrink
from ._testing import SimulationFailed, test
from ._trace import Trace, TraceEvent
from .errors import (
    DeadlockError,
    InvariantViolation,
    NondeterminismError,
    SimulationError,
    StepLimitExceeded,
    TimeLimitExceeded,
    UnsupportedOperation,
)
from .net import Host, LinkConfig, Network
from .scheduling import FifoScheduler, RandomScheduler, Scheduler

__version__ = "0.1.0.dev0"

__all__ = [
    "ChoiceSource",
    "DeadlockError",
    "Divergence",
    "ExploreReport",
    "Failure",
    "FailureGroup",
    "FifoScheduler",
    "Host",
    "InvariantViolation",
    "LinkConfig",
    "Network",
    "NondeterminismError",
    "Property",
    "RandomScheduler",
    "Reproduction",
    "RunResult",
    "Scheduler",
    "ShrinkResult",
    "SimConfig",
    "SimEventLoop",
    "Simulation",
    "SimulationError",
    "SimulationFailed",
    "StepLimitExceeded",
    "TimeLimitExceeded",
    "Trace",
    "TraceEvent",
    "UnsupportedOperation",
    "__version__",
    "always",
    "check_determinism",
    "confirm_feasible",
    "current",
    "current_host",
    "eventually",
    "explore",
    "faults",
    "in_simulation",
    "log",
    "network",
    "now",
    "rng",
    "run",
    "shrink",
    "test",
]
