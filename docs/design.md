# Distributed foundation for new OAR in Rust
Base distributed system for the new OAR  [OAR](https://github.com/oar-team/oar3/), a versatile resource and job manager for clusters: three kinds of machines, one persistent process each,
talking **gRPC** (tonic). Database, scheduling, job execution; scheduling are
**placeholders** behind traits.



# Roles 

## client (frontend):   
- One persistent process per client machine; a **Unix domain socket** for the CLI (json to communicate), stores submissions in the (placeholder) database, and notify server of new job submission 
- binary name: `oar-client`

## server (unique):
- the unique server; its core is an **automaton** that cycles over the states *scheduling,   launching jobs, checking nodes, finishing jobs, waiting*. Switch between steps depends on one queue events and timeout if there is no events. Events are pushed in queue when clients or nodes notify server via **gRPC**. Scheduling and launching jobs steps are a wake-up signal to respective thread. Checking nodes and finishing jobs steps spawn respective threads. A each steps automaton waits that the concerned thread finished before to consider to switch to the next step.   

- binary name: `oar-server`

## compute node  | `oar-node`    
one persistent process per compute node; receives `StartJob` calls from the server, *simulates* running the application on its compute nodes, and signals the server when the job is finished |
- binary name: `oar-node`

## CLI
Users interact with the system through CLI commands (`oarsub`, `oarstat`)
that speak json to the client daemon over the Unix domain socket.





## Layout

```
crates/oar-proto           tonic/prost codegen 

crates/oar-common          shared helpers: node registry, Database trait,
                           -l resources parsing, TCP/UDS gRPC connectors
crates/oar-server          the unique server daemon: automaton + engine +
                           Scheduler / NodeCallback gRPC services
crates/oar-node            the head compute node daemon: ComputeNode gRPC
                           service, simulated execution, node registration
crates/oar-client          the client daemon: json over Unix domain socket (UDS)
                            oar-cli, DB store, notify new job submission server
scripts/smoke_test.sh      live multi-process smoke test of the binaries
```
 
### others file details
crates/oar-proto/proto/oar.proto the whole wire protocol (messages + 4 services) 
crates/oar-common/src/ipc.rs (CLI ⇄ client protocol, JSON lines) ·
crates/oar-common/src/types.rs

/etc/oar.conf file is an example of configuration of OAR3

# Important Notes

## crates to uses
dotenvy : to load configuration to from OAR3 compatible format 
config : to manage and distribute configuration settings
tracing : for logging and tracing
tonic

## Misc
Cargo must .cargo directory for registry cache and index
