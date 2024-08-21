from __future__ import annotations

import pytest

from dataclasses import dataclass
from ydist.types import MetaCommand, WorkerId


@dataclass
class SessionMetaCommand(MetaCommand):
    pass


@dataclass
class WorkerMetaCommand(MetaCommand):
    worker_id: WorkerId


pytest_ydist_metacommands: set[type[MetaCommand]] = set()


@pytest.hookimpl()
def pytest_ydist_register_metacommands() -> list[type[MetaCommand]]:
    """Register metacommand types for use with ydist.

    This is used by ydist to be able to deserialize metacommands.
    """
    return list(pytest_ydist_metacommands)


# ----------------------------------------------------------------------------------------
# Planned Features
# ----------------------------------------------------------------------------------------


# @dataclass
# class Cancelation(WorkerMetaCommand):
#     seq_nr: SeqNr
#     abort: bool
# 
# 
# @dataclass
# class TestWorkSteal(WorkerMetaCommand):
#     # Attempt to steal some quantity of tests from the back of a `RunTests` command
#     worker_id: WorkerId
#     quantity: TestQuantity
# 
# 
# @dataclass
# class KillWorker(SessionMetaCommand):
#     worker_id: WorkerId
#     replace: bool
# 
# 
# @dataclass
# class TestQuantity(abc.ABC):
#     pass
# 
# 
# @dataclass
# class Relative(abc.ABC):
#     ratio: float
# 
# 
# @dataclass
# class Abosulte(abc.ABC):
#     quantity: int
