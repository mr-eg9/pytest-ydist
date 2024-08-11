from __future__ import annotations

from dataclasses import dataclass

from ydist.types import Command, TestIdx

@dataclass
class RunTests(Command):
    tests: list[TestIdx]


@dataclass
class ShutdownWorker(Command):
    pass
