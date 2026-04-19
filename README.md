# carry_sprint

CarrySprint is a local sprint API server implemented in Go with SQLite storage.

## Design-aligned process structure

This repository now includes the detailed-design aligned split process structure:

- `p1/`: Web Gateway Process (HTTP -> ZeroMQ REQ)
- `p2/`: Application Process (ZeroMQ REP -> use case -> SQLite)

Run in separate terminals:

```bash
go run ./p2
```

```bash
go run ./p1
```

Environment variables:

- `CARRY_SPRINT_ZMQ_ENDPOINT` (default: `tcp://127.0.0.1:5557`)
- `CARRY_SPRINT_ADDR` for P1 HTTP bind (default: `:8080`)
- `CARRY_SPRINT_DATA_DIR` for P2 SQLite directory (default: `data`)

## Quick start

1. Install Go 1.23 or later.
2. Run the server:

```bash
go run ./cmd/carrysprint
```

By default, the server listens on `:8080` and creates SQLite files under `./data`.

Environment variables:

- `CARRY_SPRINT_ADDR`: bind address (default `:8080`)
- `CARRY_SPRINT_DATA_DIR`: data directory path (default `data`)

## Troubleshooting: port already in use

If startup fails with an error like `listen tcp :8080: bind: Only one usage of each socket address ...`, another process is already using port `8080`.

Use either approach:

- stop the process currently using `8080`
- run CarrySprint on a different port

PowerShell example:

```powershell
$env:CARRY_SPRINT_ADDR=':18080'
go run ./cmd/carrysprint
```

To check the process using port `8080` on Windows:

```powershell
Get-NetTCPConnection -LocalPort 8080 -State Listen |
  Select-Object LocalAddress, LocalPort, OwningProcess
```

## Implemented APIs

- `GET /api/projects`
- `GET /api/projects/{project_id}/summary`
- `GET /api/projects/{project_id}/sprints/{sprint_id}/workspace`
- `PATCH /api/projects/{project_id}/tasks/{task_id}`
- `GET /api/projects/{project_id}/resources`
- `PUT /api/projects/{project_id}/resources`
- `GET /api/projects/{project_id}/calendar`
- `PUT /api/projects/{project_id}/calendar`
- `POST /api/projects/{project_id}/sprints/{sprint_id}/carryover/apply`
- `GET /api/users`
- `POST /api/users`
- `PATCH /api/users/{user_id}`
- `DELETE /api/users/{user_id}`
- `GET /api/users/{user_id}/locale`
- `PUT /api/users/{user_id}/locale`
- `GET /api/projects/{project_id}/roles`
- `PUT /api/projects/{project_id}/roles`
- `GET /api/locales/default`
- `GET /api/top/menu`
- `GET /api/users/{user_id}/menu-visibility`
- `PUT /api/users/{user_id}/menu-visibility`

All requests require `X-Request-Id` header.

`GET /api/top/menu` additionally requires `X-User-Id` header.

Write APIs require `Content-Type: application/json`.

## Seed data

On first startup, the app seeds:

- project `demo`
- sprint `sp-001`
- locales `ja`, `de`, `zh`, `it`, `fr`
- tasks `task-001` to `task-003`
- user `u001`
- user `admin` (initial password value: `admin`)

At startup, `admin` is created only when it does not already exist. Credential data is stored as a one-way hash in `user_credentials`.

This allows immediate testing of workspace and role APIs.

## Example

```bash
curl -H "X-Request-Id: req-1" http://localhost:8080/api/projects
```
