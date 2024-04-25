from __future__ import annotations
from pathlib import Path
from typing import Iterable
from contextlib import contextmanager
import nox
from nox import Session

__all__ = [
    "unit_tests",
    "integration_tests",
]

_ROOT: Path = Path(__file__).parent


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


@contextmanager
def test_db(session: Session, db_version: str, port: int):
    with_db = db_version not in ["", None]

    def nop():
        pass

    def start_db():
        session.run(
            "itde",
            "spawn-test-environment",
            "--environment-name",
            "test",
            "--database-port-forward",
            f"{port}",
            "--bucketfs-port-forward",
            "2580",
            "--docker-db-image-version",
            db_version,
            "--db-mem-size",
            "4GB",
            external=True,
        )

    def stop_db():
        session.run("docker", "kill", "db_container_test", external=True)

    start = start_db if with_db else nop
    stop = stop_db if with_db else nop

    start()
    yield
    stop()


@nox.session(name="unit-tests", python=False)
def unit_tests(session: Session) -> None:
    """Runs all unit tests"""
    _unit_tests(session)


@nox.session(name="integration-tests", python=False)
def integration_tests(session: Session) -> None:
    """Runs the all integration tests"""
    with test_db(session, db_version="7.1.17", port=8563):
        _integration_tests(session)


@ nox.session(name="all-tests", python=False)
def all_tests(session: Session) -> None:
    """Runs all tests (Unit and Integration)"""
    command=_test_command(_ROOT / "test")
    session.run(*command)
