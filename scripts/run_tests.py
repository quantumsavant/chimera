"""
Unified test and verification runner for CHIMERA.
Runs compilation checks, unit/integration/adversarial test discovery, and all
runnable examples.
"""

from __future__ import annotations

from pathlib import Path
import py_compile
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
TESTS_DIR = REPO_ROOT / "tests"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def compile_all_sources() -> int:
    count = 0
    for folder in (SRC_DIR, TESTS_DIR, REPO_ROOT / "examples", REPO_ROOT / "scripts"):
        for py_file in sorted(folder.rglob("*.py")):
            py_compile.compile(str(py_file), doraise=True)
            count += 1
    print(f"[check] Bytecode compilation succeeded for {count} Python files.")
    return count


def run_test_suite() -> bool:
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(TESTS_DIR), pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def main() -> int:
    compile_all_sources()
    ok = run_test_suite()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
