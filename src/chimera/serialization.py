"""
Canonical, lossless JSON serialization, payload validation, cryptographic
definition hashing, and process-callable importability verification.

Canonical JSON Rules:
1. Allowed types: `None`, `bool`, `int`, finite `float` (RFC 8259: `NaN`,
   `Infinity`, and `-Infinity` are rejected), `str`, `list`, `tuple` (encoded
   as JSON arrays), and `dict` with `str` keys.
2. Unsupported types (`set`, `frozenset`, `bytes`, `complex`, functions,
   arbitrary objects, non-string dict keys) raise `PayloadSerializationError`
   with the exact path (e.g., `$.config.items[1]`). Silent `repr()` fallbacks
   are strictly prohibited.
3. Encoding uses UTF-8 with `ensure_ascii=True`, `sort_keys=True`,
   `separators=(",", ":")`, and `allow_nan=False`.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import math
from typing import Any, Callable

from chimera.exceptions import (
    LogCorruptionError,
    OversizedPayloadError,
    PayloadSerializationError,
    TaskOutputLimitExceededError,
    UnserializableCallableError,
)

DEFAULT_MAX_PAYLOAD_BYTES: int = 4 * 1024 * 1024  # 4 MiB
DEFAULT_MAX_FRAME_BYTES: int = 8 * 1024 * 1024  # 8 MiB
MAX_NESTING_DEPTH: int = 64


def validate_canonical_json_value(
    value: Any,
    *,
    path: str = "$",
    depth: int = 0,
) -> Any:
    """
    Recursively validate that `value` can be losslessly represented in strict
    canonical JSON and return a normalized deep copy (converting tuples to lists).
    """
    if depth > MAX_NESTING_DEPTH:
        raise PayloadSerializationError(
            f"Payload at '{path}' exceeds maximum nesting depth ({MAX_NESTING_DEPTH})."
        )

    if value is None or isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise PayloadSerializationError(
                f"Non-finite float value ({value!r}) at '{path}' is not permitted in durable canonical JSON."
            )
        return value

    if isinstance(value, (list, tuple)):
        return [
            validate_canonical_json_value(
                item, path=f"{path}[{idx}]", depth=depth + 1
            )
            for idx, item in enumerate(value)
        ]

    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise PayloadSerializationError(
                    f"Non-string dictionary key {k!r} ({type(k).__name__}) at '{path}' "
                    "is not permitted in durable canonical JSON."
                )
            normalized[k] = validate_canonical_json_value(
                v, path=f"{path}.{k}", depth=depth + 1
            )
        return normalized

    raise PayloadSerializationError(
        f"Unsupported non-JSON-serializable type '{type(value).__name__}' at '{path}'. "
        "Durable payloads and results must use only None, bool, int, finite float, str, list/tuple, and str-keyed dict."
    )


def canonical_json_dumps(
    value: Any,
    *,
    max_bytes: int | None = DEFAULT_MAX_PAYLOAD_BYTES,
    is_task_output: bool = False,
) -> str:
    """Validate and serialize `value` to a deterministic canonical ASCII JSON string."""
    normalized = validate_canonical_json_value(value)
    try:
        encoded_str = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise PayloadSerializationError(
            f"Failed to serialize value to canonical JSON: {exc}"
        ) from exc

    byte_len = len(encoded_str.encode("utf-8"))
    if max_bytes is not None and byte_len > max_bytes:
        if is_task_output:
            raise TaskOutputLimitExceededError(
                f"Serialized task output size ({byte_len} bytes) exceeds limit ({max_bytes} bytes)."
            )
        raise OversizedPayloadError(
            f"Serialized payload size ({byte_len} bytes) exceeds limit ({max_bytes} bytes)."
        )
    return encoded_str


def canonical_json_bytes(
    value: Any,
    *,
    max_bytes: int | None = DEFAULT_MAX_PAYLOAD_BYTES,
    is_task_output: bool = False,
) -> bytes:
    """Validate and serialize `value` to deterministic canonical ASCII JSON bytes."""
    return canonical_json_dumps(
        value, max_bytes=max_bytes, is_task_output=is_task_output
    ).encode("utf-8")


def _reject_non_finite_constant(token: str) -> Any:
    raise ValueError(f"Non-standard JSON constant {token!r} is prohibited.")


def canonical_json_loads(
    raw: bytes | str,
    *,
    max_bytes: int | None = DEFAULT_MAX_FRAME_BYTES,
) -> Any:
    """Parse strict RFC 8259 JSON, rejecting NaN/Infinity constants and oversized inputs."""
    if isinstance(raw, bytes):
        if max_bytes is not None and len(raw) > max_bytes:
            raise OversizedPayloadError(
                f"JSON input size ({len(raw)} bytes) exceeds limit ({max_bytes} bytes)."
            )
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise LogCorruptionError(f"Invalid UTF-8 in JSON payload: {exc}") from exc
    else:
        byte_len = len(raw.encode("utf-8"))
        if max_bytes is not None and byte_len > max_bytes:
            raise OversizedPayloadError(
                f"JSON input size ({byte_len} bytes) exceeds limit ({max_bytes} bytes)."
            )
        text = raw

    try:
        return json.loads(text, parse_constant=_reject_non_finite_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise LogCorruptionError(f"Malformed or non-canonical JSON payload: {exc}") from exc


def compute_canonical_sha256(value: Any) -> str:
    """Compute a deterministic SHA-256 hex digest over the canonical JSON representation of `value`."""
    raw = canonical_json_bytes(value, max_bytes=None)
    return hashlib.sha256(raw).hexdigest()


def describe_callable_identity(func: Callable[..., Any]) -> str:
    """Return a human-readable `<module>:<qualname>` identifier for `func`."""
    mod = getattr(func, "__module__", None) or "<unknown_module>"
    qual = getattr(func, "__qualname__", None) or getattr(
        func, "__name__", type(func).__qualname__
    )
    return f"{mod}:{qual}"


def validate_callable_for_process_backend(
    func: Callable[..., Any],
    task_id: str,
) -> tuple[str, str]:
    """
    Verify that `func` is a top-level importable callable suitable for execution
    in a `spawn` child process.

    Rejects lambdas, nested `<locals>` closures, and unimportable callables with
    `UnserializableCallableError` rather than silently falling back to threads.
    """
    if not callable(func):
        raise UnserializableCallableError(
            f"Task '{task_id}' func is not callable."
        )

    target = func
    if not inspect.isfunction(func) and not inspect.isbuiltin(func) and not inspect.ismethod(func):
        raise UnserializableCallableError(
            f"Task '{task_id}' callable '{describe_callable_identity(func)}' is a stateful callable instance; "
            "the PROCESS execution backend requires a top-level importable module function."
        )

    module_name = getattr(target, "__module__", None)
    qualname = getattr(target, "__qualname__", None) or getattr(
        target, "__name__", None
    )

    if not module_name or not qualname:
        raise UnserializableCallableError(
            f"Task '{task_id}' callable is missing __module__ or __qualname__ for PROCESS execution."
        )

    if qualname == "<lambda>" or qualname.endswith(".<lambda>") or "<lambda>" in qualname:
        raise UnserializableCallableError(
            f"Task '{task_id}' uses a lambda function ('{module_name}:{qualname}'), which cannot be "
            "dispatched across processes in the PROCESS backend. Define a top-level module function instead."
        )

    if "<locals>" in qualname:
        raise UnserializableCallableError(
            f"Task '{task_id}' uses a local closure ('{module_name}:{qualname}'), which cannot be "
            "dispatched across processes in the PROCESS backend. Define a top-level module function instead."
        )

    try:
        mod = importlib.import_module(module_name)
        obj: Any = mod
        for part in qualname.split("."):
            obj = getattr(obj, part)
    except Exception as exc:
        raise UnserializableCallableError(
            f"Task '{task_id}' callable '{module_name}:{qualname}' cannot be imported by child processes: {exc}"
        ) from exc

    if not callable(obj):
        raise UnserializableCallableError(
            f"Task '{task_id}' resolved attribute '{module_name}:{qualname}' is not callable."
        )

    return module_name, qualname


def resolve_process_callable(
    module_name: str,
    qualname: str,
    main_file: str | None = None,
) -> Callable[..., Any]:
    """Import and return `(module_name, qualname)` inside a spawned worker process."""
    import importlib.util
    import sys

    candidates = []
    try:
        candidates.append(importlib.import_module(module_name))
    except Exception:
        pass

    if module_name == "__main__" and "__mp_main__" in sys.modules:
        candidates.append(sys.modules["__mp_main__"])

    last_exc: Exception | None = None
    for mod in candidates:
        try:
            obj: Any = mod
            for part in qualname.split("."):
                obj = getattr(obj, part)
            if callable(obj):
                return obj
        except Exception as exc:
            last_exc = exc

    raise UnserializableCallableError(
        f"Resolved target '{module_name}:{qualname}' could not be imported: {last_exc}"
    )
