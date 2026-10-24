"""
Unified test and verification runner for CHIMERA.
Runs bytecode compilation checks, unit/integration/adversarial test discovery,
and executes all runnable example scripts and CLI diagnostics.
"""

from __future__ import annotations

from pathlib import Path
import py_compile
import subprocess
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
TESTS_DIR = REPO_ROOT / "tests"
EXAMPLES_DIR = REPO_ROOT / "examples"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def compile_all_sources() -> int:
    count = 0
    for folder in (SRC_DIR, TESTS_DIR, EXAMPLES_DIR, REPO_ROOT / "scripts"):
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


def run_all_examples() -> bool:
    example_files = sorted(EXAMPLES_DIR.glob("*.py"))
    for ex_file in example_files:
        proc = subprocess.run(
            [sys.executable, str(ex_file)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=30.0,
        )
        if proc.returncode != 0:
            print(
                f"[error] Example {ex_file.name} failed with exit code {proc.returncode}:\n"
                f"STDOUT: {proc.stdout}\nSTDERR: {proc.stderr}",
                file=sys.stderr,
            )
            return False
        print(f"[example] {ex_file.name}: {proc.stdout.strip().splitlines()[-1]}")
    return True


def main() -> int:
    compile_all_sources()
    tests_ok = run_test_suite()
    if not tests_ok:
        return 1
    examples_ok = run_all_examples()
    return 0 if examples_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
