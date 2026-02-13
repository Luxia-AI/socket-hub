# Socket Hub Service

`socket-hub` is the realtime ingress layer for client room interactions.

## Responsibilities

- Socket.IO connection handling
- Room auth and room-bound posting constraints
- Dispatch forwarding and worker result fanout
- Optional Kafka publish/consume integration

## Entry Points

- Main app/events: `socket-hub/app/main.py`
- Room auth/state: `socket-hub/app/sockets/manager.py`
- API routes: `socket-hub/app/api/routes.py`

## Dependencies

- FastAPI + python-socketio
- Redis
- aiokafka

## Canonical Docs

- `docs/system-overview.md`
- `docs/request-lifecycle.md`
- `docs/interfaces-and-contracts.md`
- `docs/testing-and-validation.md`

Last verified against code: February 13, 2026
