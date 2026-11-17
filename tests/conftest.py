"""A tiny pytest-compatible shim so the suite runs without pytest installed.

Supports: ``pytest.raises``, ``pytest.fixture`` (function scope), ``tmp_path``,
``pytest.mark.parametrize``, ``pytest.skip`` and assertion rewriting-free
execution.  The runner is ``tests/run_tests.py``.
"""

from __future__ import annotations

import contextlib
import inspect
import tempfile
import traceback
from pathlib import Path


class _Raises:
    def __init__(self, expected, match=None):
        self.expected = expected
        self.match = match
        self.value = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError(f"expected {self.expected} but nothing was raised")
        if not issubclass(exc_type, self.expected):
            return False
        self.value = exc
        if self.match is not None:
            import re

            if re.search(self.match, str(exc)) is None:
                raise AssertionError(f"exception message {str(exc)!r} does not match {self.match!r}")
        return True


def raises(expected, match=None):
    return _Raises(expected, match)


class _Skip(Exception):
    pass


def skip(reason=""):
    raise _Skip(reason)


class _Mark:
    @staticmethod
    def parametrize(name, values):
        def decorator(fn):
            fn._params = (name, list(values))
            return fn

        return decorator

    @staticmethod
    def skipif(condition, reason=""):
        def decorator(fn):
            if condition:
                fn._skip = reason
            return fn

        return decorator


mark = _Mark()


def fixture(fn=None):
    def wrap(f):
        f._is_fixture = True
        return f

    if fn is None:
        return wrap
    return wrap(fn)
