# RampFlow Ground Operations Lab

RampFlow is a dependency-free Python domain library for airport turnaround and ramp operations. It models operational resources and constraints rather than exposing a toy CRUD service. The package includes turnaround task scheduling, resource reservations, runway slots, stand allocation, taxi routing, fueling, deicing, baggage reconciliation, maintenance deferrals, passenger connections, crew duty, mobile resource dispatch, apron safety, NOTAM recurrence, disruption recovery, and immutable operational snapshots.

The implementation uses only the Python standard library at runtime. State-changing services use explicit version, generation, reservation, or replay semantics where the real-world workflow requires them. Cross-domain workflows publish outward events only after their domain change succeeds and use operation IDs for replay safety. Public tests exercise representative invariants, import every packaged module, and the CLI provides a dependency-free health check and snapshot digest utility.

## Stream cursor and projection semantics

- `EventStream.page`/`wait_page` reject an expired cursor by raising `CursorExpired` (carrying `floor` and `reset_cursor`); the stream never silently skips evicted events. An expired projection must rebuild from an `OperationalSnapshot` and resume paging at `reset_cursor`.
- `VersionedStore.batch` validates the whole batch against a shadow state before committing; a mid-batch version conflict leaves the store untouched.
- `RampWorkflowControl.acknowledge_stream_projection` runs under the same guard as every other workflow: the projection action commits first and the acknowledgement event is published only after it succeeds, with operation-ID replay safety.

## Quick check

```bash
PYTHONPATH=src python -m unittest discover -s tests
PYTHONPATH=src python -m rampflow.cli health --json
```

## Package layout

- `src/rampflow/`: production domain code.
- `src/rampflow/workflows/`: cross-domain commit and event-consistency boundary.
- `tests/`: public regression and smoke tests.
- `pyproject.toml`: package metadata. Runtime dependencies are empty.

The repository is intentionally self-contained and does not require network access or external services for its domain logic.
