"""Minimal test runner: discovers ``test_*.py`` and runs test functions.

It implements enough of pytest for this repo to run in a bare Python install:
``tmp_path`` injection, function-scoped fixtures, ``parametrize`` decorators,
and ``raises``/``skip`` helpers from ``tests/conftest.py``.

Usage::

    python tests/run_tests.py [pattern ...]
"""

from __future__ import annotations

import importlib
import inspect
import sys
import tempfile
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(TESTS))

import conftest  # noqa: E402
sys.modules.setdefault("pytest", conftest)


def _fixtures(module):
    return {
        name: fn
        for name, fn in vars(module).items()
        if getattr(fn, "_is_fixture", False)
    }


def _call_with_fixtures(fn, module, tmp_root):
    kwargs = {}
    fixtures = _fixtures(module)
    for name, param in inspect.signature(fn).parameters.items():
        if name == "tmp_path":
            kwargs[name] = Path(tempfile.mkdtemp(dir=tmp_root))
        elif name in fixtures:
            kwargs[name] = _call_with_fixtures(fixtures[name], module, tmp_root)
        else:
            raise RuntimeError(f"unknown fixture {name!r} for {fn.__name__}")
    return fn(**kwargs)


def run(patterns):
    test_files = sorted(TESTS.glob("test_*.py"))
    if patterns:
        test_files = [p for p in test_files if any(pat in p.name for pat in patterns)]
    tmp_root = tempfile.mkdtemp(prefix="recon-tests-")
    passed = failed = skipped = 0
    failures = []
    for path in test_files:
        module_name = path.stem
        module = importlib.import_module(module_name)
        for name in sorted(vars(module)):
            if not name.startswith("test_"):
                continue
            fn = getattr(module, name)
            if not callable(fn):
                continue
            if getattr(fn, "_skip", None):
                skipped += 1
                continue
            params = getattr(fn, "_params", None)
            cases = []
            if params:
                pname, values = params
                for value in values:
                    cases.append((f"{name}[{value!r}]", {pname: value}))
            else:
                cases.append((name, {}))
            for case_name, extra in cases:
                kwargs = dict(extra)

                def bound(**kw):
                    return _invoke(fn, module, tmp_root, kw)

                try:
                    _invoke(fn, module, tmp_root, kwargs)
                    passed += 1
                    print(f"PASS {module_name}.{case_name}")
                except conftest._Skip as exc:
                    skipped += 1
                    print(f"SKIP {module_name}.{case_name}: {exc}")
                except Exception:
                    failed += 1
                    print(f"FAIL {module_name}.{case_name}")
                    failures.append((case_name, traceback.format_exc()))
    print(f"\n{passed} passed, {failed} failed, {skipped} skipped")
    for case_name, tb in failures:
        print(f"\n===== {case_name} =====\n{tb}")
    return 0 if failed == 0 else 1


def _invoke(fn, module, tmp_root, extra):
    kwargs = dict(extra)
    fixtures = _fixtures(module)
    for name, param in inspect.signature(fn).parameters.items():
        if name in kwargs:
            continue
        if name == "tmp_path":
            kwargs[name] = Path(tempfile.mkdtemp(dir=tmp_root))
        elif name in fixtures:
            kwargs[name] = _call_with_fixtures(fixtures[name], module, tmp_root)
        else:
            raise RuntimeError(f"unknown fixture {name!r} for {fn.__name__}")
    return fn(**kwargs)


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
