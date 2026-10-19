# Contributing to CHIMERA

Contributions that improve correctness, crash recovery, cross-platform compatibility, or test coverage are welcome.

## Development Principles
1. **Standard Library First**: CHIMERA intentionally avoids external runtime dependencies. Any proposed change must use the Python 3.11+ standard library.
2. **Deterministic Testing**: Never use arbitrary `time.sleep()` calls to synchronize concurrent tests. Use `threading.Barrier`, `threading.Event`, `VirtualClock`, and deterministic engine hooks (`on_task_claimed_hook`, `before_task_commit_hook`, and `crash_hook`).
3. **No Silent Failures**: Never swallow exceptions or silently ignore corrupted interior log records.

## Running Verification Locally
```bash
python3 scripts/run_tests.py
python3 examples/basic_workflow.py
python3 examples/retry_example.py
python3 examples/recovery_example.py
```
