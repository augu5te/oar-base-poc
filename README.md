# OAR base system (Rust)

Base distributed system for the new [OAR](https://github.com/oar-team/oar3/), a
versatile resource and job manager for clusters.

Three kinds of machines, **one persistent process each**, plus a CLI. They talk
**gRPC** (tonic); the CLI talks **JSON over a unix domain socket** (UDS) to its
local daemon. The database, the scheduler and the job execution are
**placeholders behind traits**, so the real implementations can be dropped in
without touching the transport, the protocol or the automaton.

```
        client machine                 server machine            compute nodes
  ┌───────────────────────┐      ┌────────────────────────┐    ┌────────────────┐
  │ oarsub / oarstat      │      │ oar-server (unique)    │    │ oar-node       │
  │      │ JSON lines      │      │                        │    │                │
  │      ▼  over UDS       │      │  SchedulerService  ◄───┼────┼─ no             │
  │ oar-client            │      │  NodeCallbackService ◄─┼────┼─ RegisterNode  │
  │   ├─ Database (stub)  │      │  AdminService          │    │  Heartbeat     │
  │   └─ gRPC client ─────┼─────►│  automaton + engine    │    │  JobFinished   │
  └───────────────────────┘      │   ├─ Database (stub)   │    │  ───────────   │
                                 │   ├─ Scheduler (stub)  │    │  ComputeNode   │
                                 │   └─ NodeLauncher ─────┼───►│  Service       │
                                 └────────────────────────┘    │  ├─ FakeExecutor
                                                               └────────────────┘
```

## Crates

| crate | binary | role |
| --- | --- | --- |
| `crates/oar-proto` | — | the whole wire protocol: `proto/oar.proto` + tonic/prost codegen |
| `crates/oar-common` | — | shared code: config, domain types, `Database` trait, resources parsing, gRPC helpers, CLI⇄daemon JSON protocol |
| `crates/oar-server` | `oar-server` | the unique server: automaton + engine + `SchedulerService`, `NodeCallbackService`, `AdminService` |
| `crates/oar-node` | `oar-node` | the compute node daemon: `ComputeNodeService`, simulated execution, registration/heartbeat |
| `crates/oar-client` | `oar-client` | the client daemon: JSON over UDS, local job store, notifies the server |
| `crates/oar-cli` | `oarsub`, `oarstat` | the user commands, talking JSON to the client daemon |

## Quick start

Everything below is automated by [`scripts/smoke_test.sh`](scripts/smoke_test.sh)
(`make smoke`), which starts the real daemons as separate processes and drives
them with the real CLI.

```bash
# 1. build (see "Cargo home" below: `make` keeps the registry in ./.cargo)
make build

# 2. configuration (OAR3 dotenv syntax); copy and edit etc/oar.conf
export OAR_CONFIG="$PWD/etc/oar.conf"

# 3. the unique server: gRPC on SERVER_HOSTNAME:SERVER_PORT
oar-server &

# 4. the client daemon: JSON on $OAR_CLIENT_SOCKET (default /run/oar/client.sock)
oar-client &

# 5. one daemon per compute node
OAR_NODE_ID=node-1 OAR_NODE_ADDRESS=127.0.0.1:7001 oar-node &
OAR_NODE_ID=node-2 OAR_NODE_ADDRESS=127.0.0.1:7002 oar-node &

# 6. submit / inspect (the command is whatever follows the options)
oarsub -l nodes=2,walltime=1:00:00 -- ./my_program.sh
oarsub -l nodes=1 "exit=3"
oarstat
oarstat -j 1
oarstat -s running
```

## Configuration

`Config::load` reads, in order: the file given by `OAR_CONFIG` (else
`/etc/oar/oar.conf`), then the environment. The file is parsed by `dotenvy`
("OAR3 compatible": `KEY="VALUE"`, `#` comments) and the environment accepts
either the OAR3 key (`SERVER_PORT`) or the daemon-prefixed one
(`OAR_SERVER_PORT`). Unknown keys are ignored with a warning, and every setting
falls back to a sane default, so a partial file is fine.

| key (file) | env | meaning | default |
| --- | --- | --- | --- |
| `SERVER_HOSTNAME`, `SERVER_PORT` | `OAR_SERVER_PORT` | server gRPC endpoint | `localhost:6666` |
| `NODES` | `OAR_NODES` | optional static node list `id=address,...` (or `@file`) | empty |
| `OARSUB_DEFAULT_RESOURCES` | — | `-l` used by `oarsub` without `-l` | `/resource_id=1` |
| `DEFAULT_WALLTIME` | — | walltime used when none is given | `2:00:00` |
| `CLIENT_SOCKET` | `OAR_CLIENT_SOCKET` | client daemon UDS | `/run/oar/client.sock` |
| `NODE_ID`, `NODE_ADDRESS` | `OAR_NODE_ID`, `OAR_NODE_ADDRESS` | node identity and gRPC endpoint | hostname / `127.0.0.1:7001` |
| `NODE_HEARTBEAT` | — | heartbeat/registration period (s) | `10` |
| `SIMULATED_JOB_DURATION` | — | duration of a simulated job (s) | `2` |
| `AUTOMATON_WAIT` | — | automaton timeout when the event queue is empty (s) | `1` |
| `SCHEDULER_TIMEOUT` | — | scheduler timeout (s) | `30` |
| `FINAUD_FREQUENCY` | — | periodic node check (s) | `300` |
| `DB_HOSTNAME`, `DB_PORT`, `DB_BASE_NAME`, `DB_BASE_LOGIN`, `DB_BASE_PASSWD` | — | placeholder database settings | local PostgreSQL defaults |
| `LOG_LEVEL`, `LOG_CATEGORIES` | — | tracing level/categories (OAR3 `LOG_LEVEL` 1..4) | `2`, `all` |

`-l` accepts the OAR3 syntax (`nodes=2,walltime=1:30:00`, `/nodes=n1+n2/core=4`,
several `-l` options joined with `/`). Walltime follows `oarsub(1)`:
`[hour:mn:sec|hour:mn|hour]`, so `2:00` means two hours.

## Protocols

**gRPC** (`crates/oar-proto/proto/oar.proto`) — four services:

| service | direction | RPCs |
| --- | --- | --- |
| `SchedulerService` | client daemon → server | `NotifyNewJob` (the server allocates the job id) |
| `NodeCallbackService` | node → server | `RegisterNode`, `Heartbeat`, `JobFinished` |
| `ComputeNodeService` | server → node | `StartJob`, `Ping` |
| `AdminService` | operator/CLI → server | `GetJob`, `ListJobs`, `ClusterStatus` |

Every RPC works over TCP (`http://host:port`) and over unix domain sockets
(`unix:/path`); the transport is chosen from the address by the helpers in
`crates/oar-common/src/grpc.rs`.

**JSON over UDS** (CLI ⇄ client daemon, `crates/oar-common/src/ipc.rs`) — one
JSON document per line:

```json
{"type":"submit","command":"echo hi","nodes":2,"walltime_secs":3600}
{"type":"submitted","job_id":1,"warning":null}
{"type":"list","state":"running"}
{"type":"jobs","jobs":[{"job_id":1,"state":"running","nodes":["n1"],"exit_code":null}]}
```

The submitter's identity is **not** sent by the CLI: the daemon takes it from the
socket credentials (`SO_PEERCRED`).

Job states (stable wire spelling, `snake_case`): `waiting`, `to_launch`,
`launching`, `running`, `finishing`, `terminated`, `error`. Transitions are
validated by `JobState::can_transition_to`, so a late or duplicated node
notification can never corrupt a job.

## The placeholders

Everything that needs a real implementation sits behind a trait:

| trait | placeholder | where |
| --- | --- | --- |
| `Database` | `MemoryDatabase` | `oar-common/src/db.rs` |
| `Scheduler` | `FifoScheduler` | `oar-server/src/scheduler.rs` |
| `NodeLauncher` | `GrpcNodeLauncher` (real transport, fake resource manager) | `oar-server/src/node_client.rs` |
| `Executor` | `FakeExecutor` (sleeps, derives an exit code from the command) | `oar-node/src/executor.rs` |
| `ServerClient` | `GrpcServerClient` | `oar-client/src/backend.rs` |
| `StepExecutor` | `EngineSteps` | `oar-server/src/engine.rs` |

The automaton cycle is *scheduling → launching jobs → checking nodes → finishing
jobs → waiting*, driven by an event queue and a timeout. Scheduling and
launching wake dedicated worker tasks (the automaton waits for their reply);
checking nodes and finishing jobs are spawned tasks the automaton waits for.

## Tests

```bash
make test          # everything (120 tests)
make test-unit     # unit tests only
make test-e2e      # in-process end-to-end tests only
make clippy        # lint (-D warnings)
make smoke         # live multi-process smoke test with the real binaries
```

* **unit tests** live next to the code (configuration parsing incl. the shipped
  `etc/oar.conf`, resource/walltime parsing, job state machine, database,
  registry, automaton, scheduler, engine, gRPC services, node executor/service,
  client daemon, CLI argument handling);
* **end-to-end tests**:
  * `crates/oar-server/tests/cluster_e2e.rs` — server + node + client started
    in-process, talking over real TCP/UDS sockets: submission → scheduling →
    launch → simulated execution → termination, exit codes, job queueing, node
    liveness;
  * `crates/oar-server/tests/services_e2e.rs` — every RPC of all four services,
    over TCP *and* over a unix domain socket, including the error paths;
  * `crates/oar-client/tests/uds_e2e.rs` — the JSON/UDS protocol: multiple and
    malformed messages, stale socket files, error answers;
  * `crates/oar-cli/tests/cli_e2e.rs` — `oarsub`/`oarstat` against a real daemon.

## Cargo home

The design requires the Cargo registry cache and index to live in the
in-workspace `.cargo` directory (git-ignored). Use `make`, which exports
`CARGO_HOME=$(CURDIR)/.cargo` for every target:

```bash
make build                 # CARGO_HOME=./.cargo
make CARGO_HOME=$HOME/.cargo build   # reuse the user-level cache instead
```

Plain `cargo` keeps using your usual `CARGO_HOME`; `make cargo-home` prints the
one currently in use.

## Layout

```
crates/oar-proto           tonic/prost codegen
crates/oar-common          config, types, Database trait, -l parsing, gRPC/IPC helpers
crates/oar-server          automaton + engine + Scheduler/NodeCallback/Admin services
crates/oar-node            ComputeNode service, simulated execution, registration
crates/oar-client          JSON over UDS, job store, notify new job submission
crates/oar-cli             oarsub / oarstat
docs/design.md             the original direction document
etc/oar.conf               OAR3 example configuration
scripts/smoke_test.sh      live multi-process smoke test
```
