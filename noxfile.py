from __future__ import annotations
from pathlib import Path
from typing import Iterable
import nox
from nox import Session

__all__ = [
    "unit_tests",
    "integration_tests",
]

_ROOT : Path = Path(__file__).parent


def _test_command(path: Path) -> Iterable[str]:
    base_command = ["poetry", "run"]
    pytest_command = ["pytest", "-v", f"{path}"]
    return base_command + pytest_command


def _unit_tests(session: Session) -> None:
    command = _test_command(_ROOT / "test" / "unit")
    session.run(*command)


def _integration_tests(session: Session) -> None:
    command = _test_command(_ROOT / "test" / "integration")
    session.run(*command)


@nox.session(name="unit-tests", python=False)
def unit_tests(session: Session) -> None:
    """Runs all unit tests"""
    _unit_tests(session)


@nox.session(name="integration-tests", python=False)
def integration_tests(session: Session) -> None:
    """Runs the all integration tests"""
    _integration_tests(session)


@nox.session(name="all-tests", python=False)
def all_tests(session: Session) -> None:
    """Runs all tests (Unit and Integration)"""
    command = _test_command(_ROOT / "test")
    session.run(*command)
