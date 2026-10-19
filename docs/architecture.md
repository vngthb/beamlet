# Beamlet architecture

Beamlet is a native Erlang/OTP 27+ library. It talks to Docker's HTTP API through
Gun 2.7.0 and uses OTP JSON. The supported Docker API negotiation range is
1.41–1.45. Unix sockets and explicit HTTP/TCP endpoints are supported; TLS,
Windows named pipes, and Docker CLI contexts are outside the current scope.

Start with [How Beamlet works](guides/how-beamlet-works.md) for a short walkthrough
with diagrams. See [usage](guides/using-beamlet.md) for fixtures/endpoints,
[configuration](guides/configuration.md) for properties/recipes, and
[adding containers](guides/adding-containers.md) for extension work.

## Module boundaries

| Module / group | Responsibility |
| --- | --- |
| `beamlet` | Public sessions, container lifecycle, endpoint/diagnostic accessors, and fixture wrapper. Handles are opaque. |
| `beamlet_config` | Pure schema validation, defaults/recipe merging, session snapshots, and connection-property resolution. |
| `beamlet_container` | Input `spec/0`, resolved `config/0`, context-free validation/expansion, and Docker create-body encoding. |
| Service/UI modules | Sparse constructors, internal pure `defaults/1` builders, and client settings or URL accessors. |
| `beamlet_session` | Owner/caller monitors, resolved starts, resource/dependency tracking, operation coordination, and cleanup. |
| Session/job supervisors and `beamlet_job` | Temporary session subtrees and one-shot supervised `proc_lib` operation workers. |
| `beamlet_runtime` | Shared pull/create/start/inspect/readiness pipeline and preset-specific initialization. |
| `beamlet_wait` / `beamlet_deadline` | Bounded public waits, internal protocol probes, and monotonic deadlines. |
| `beamlet_docker` / `beamlet_http` | Docker operations, API negotiation, HTTP transport, and bounded response handling. |
| `beamlet_ryuk` | Reaper creation, listener readiness, filter registration, and socket lifetime. |

Presets use explicit kind branches. There is no callback/plugin framework.
Brod 4.6.3, epgsql 4.8.0, eredis 1.7.1, and erlcloud 3.8.3 are integration-only clients; runtime
has no service-client dependency.

## Sessions and ownership

Each session is a temporary supervision subtree containing a control process and
an operation-worker supervisor. The session monitors its owner, defaulting to the
process calling `start_session`. An explicit `owner` supports fixtures whose setup
process exits before their tests run. Startup requests also monitor their callers;
caller exit cancels startup and initiates cleanup independently of that caller.

Workers acknowledge startup before executing once, send one `job_done` result,
and exit. Session/worker children do not automatically restart external effects.
The session stays responsive during I/O and admits at most sixteen concurrent
container starts. Cancellation immediately terminates the relevant worker.

Handles contain identity, session ownership, mapped host/ports, and preset
settings. They contain no Docker connection. Inspection, exec, and logs use the
owning session. Internal handle/settings helpers are not public extension APIs.

## Configuration resolution

The OTP application environment key `config` defaults to `#{}` and contains
`session`, `container`, `presets`, and `recipes` maps. Consumers load ordinary
`sys.config`; Beamlet does not discover or parse configuration files.

`start_session` loads the application specification, captures config and supported
environment once, validates application/session-local layers and effective
recipes before Docker requests, then starts the tree. Bootstrap negotiates the
resolved Docker connection without rereading environment. Every supplied config
layer is validated, including invalid values hidden by later overrides.

Constructors return explicit input specs and perform no I/O/global config reads.
Container resolution happens inside the session before target checks, resource
reservation, and operation workers. Internal `defaults/1` builders expand managed
fields from the merged typed options. `beamlet_container:validate/1` remains
context-free: it expands built-ins only and rejects recipes without a session.

Container precedence is explicit input > selected recipe > local preset > local
common > application preset > application common > built-ins. Option values
replace in full; namespace/kind maps merge structurally, while a local recipe
replaces the entire same-named definition. Session precedence is explicit outer
option > supported environment > local session defaults > application session
defaults > built-in/derived values. Supported variables are `DOCKER_HOST` and
`TESTCONTAINERS_HOST_OVERRIDE`. Full schema and clearing/migration examples are in
the [configuration guide](guides/configuration.md).

Owners and live UI targets remain call-site inputs. Configured defaults/recipes
cannot override managed preset fields. Configuration errors return paths/reasons
without credential values. Supervisor specs carry an opaque options closure and
session status omits config, jobs, and messages from ordinary diagnostics. Client
settings and privileged VM introspection can expose configured test credentials.
Existing handles/settings/cleanup never consult later global config changes.

## Ports and private networks

Docker assigns every published host port. Public specs accept container ports;
fixed host bindings and unknown keys are rejected. The daemon address and
service-client address are distinct. Use returned endpoints and URLs; set `host`
or `TESTCONTAINERS_HOST_OVERRIDE` when clients need a different reachable host.
The `docker_socket` property selects the daemon-side socket mounted into Ryuk.
All published ports, including Ryuk's, explicitly bind to `127.0.0.1` by default.
Unix-socket Docker connections also return `127.0.0.1` as the default client host
to avoid clients resolving `localhost` to an unpublished IPv6 address.
The session `bind_ip` property accepts a literal IPv4 daemon-side address;
remote access requires an explicit override and a reachable client `host`.
Changing the client host alone never widens the bind address. Non-loopback
bindings expose test services and the cleanup listener and require a trusted
network. Private container-to-container traffic still uses the bridge network.

Every session reserves a unique labeled bridge-network name before creation.
Docker connection and Ryuk registration precede network creation; the session
becomes ready only after the network exists. All workloads join that network
using their generated names as private DNS aliases. Kafka advertises alias:9093
on INTERNAL and the Docker-assigned host endpoint on EXTERNAL. Ryuk stays outside
the workload network; its session label filter covers networks and containers.
This network is session infrastructure, without a public network-selection API.

## Presets and image pins

Built-in defaults live in the preset modules' internal `defaults/1` builders;
Ryuk's image lives in `beamlet_ryuk`. Explicit/default/recipe image overrides must
remain compatible with each preset's managed startup contract.

| Kind | Built-in image | Container TCP port | Readiness / initialization |
| --- | --- | --- | --- |
| Kafka | `apache/kafka:3.9.1` | 9092 published; 9093 private | Single-node KRaft; gated startup receives the assigned external listener, checks broker API, and creates requested topics. |
| DynamoDB Local | `amazon/dynamodb-local:3.3.1` | 8000 | In-memory shared-database mode; signed API probe with explicit dummy region/credentials, without ambient AWS configuration. |
| PostgreSQL | `postgres:17.6-alpine` | 5432 | Typed username/password/database generate managed environment; `psql` executes `SELECT 1` over TCP against the final server, excluding the temporary Unix-socket initialization server. |
| Redis | `redis:7.4.11-alpine` | 6379 | Snapshots/append-only storage disabled; host RESP PING requires a complete PONG, including fragmented responses. |
| Moto Server | `motoserver/moto:5.2.3` | 5000 | Host S3 ListBuckets returns HTTP 200 and the S3 XML result envelope; path-style bucket interpretation is enforced. |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.4.2` | 8080 | Browser page plus `/api/clusters/beamlet/topics` against the private Kafka target. |
| DynamoDB UI | `aaronshaf/dynamodb-admin:5.3.4` | 8001 | Browser page plus `/api/tables` against the private DynamoDB target with explicit dummy AWS settings. |
| Ryuk | `testcontainers/ryuk:0.12.0` | 8080 | `Started` log before connecting, then acknowledged session-label registration. |

Kafka starts a gated shell, inspects the assigned mapping, uploads a tar archive
with the advertised-listener script, then releases the broker. This retains one
port-allocation scheme. Preset ports, command, and environment are managed;
image, timeout, pull policy, labels, additional waits, and typed service options
remain configurable. Generic containers have no implicit service readiness.
Built-in readiness always precedes additional waits under one startup deadline.

PostgreSQL's built-in username/password/database are each `beamlet`;
empty/NUL-containing values fail validation. DynamoDB supplies dummy test
credentials without an AWS account or general AWS SDK behavior. These services
are ephemeral; separate containers isolate data. Kafbat uses static cluster name
`beamlet`; Kafka Connect, Schema Registry, and dynamic UI configuration are absent.

Moto exposes one endpoint for its supported AWS APIs. Its accessor returns the
resolved region and explicit test credentials plus an S3 path-style flag. These
options configure clients; Moto retains its normal mock account/region behavior
and does not gain IAM enforcement from a secret key. No ambient AWS configuration
is read. Separate containers isolate mock state. S3 bucket/object operations and
a second API on the same endpoint are covered by acceptance tests; other Moto
APIs are subject to upstream coverage. See the [Moto guide](guides/moto.md).

## Companion lifecycle

Services start headless. `beamlet_kafka_ui:new(Target, Options)` and
`beamlet_dynamodb_ui:new(Target, Options)` build optional specs; `new(Target)`
uses empty explicit options. Each UI exposes `url/1` after startup.

Targets must be running, belong to the same session, and have the expected kind.
The session validates ownership/state before allocation, and runtime checks the
live Docker identity before pull/create. Starting UIs are tracked dependencies.
A UI stop/failure leaves its target running. Target stop prevents new companions,
cancels pending UI starts, and removes companions before the target. Failed
companion removal blocks direct target removal and reports failure for retry.
Concurrent stop requests are serialized; successful removal is idempotent.

UIs connect through private aliases; host clients/browsers use returned mapped
endpoints. Backend acceptance checks seeded messages/items and two-stack
isolation, beyond landing-page HTTP success. These are development UIs without
an added login layer.

## Cleanup and failure handling

Reserve resource names before issuing create requests. Never blindly retry an
ambiguous create: the daemon may finish after a timeout. Keep starting/failed
names for the final sweep even if initial removal reports success. Remove UIs
before targets, then the network; another session is never a cleanup candidate.

Ryuk is enabled by default. Register its cryptographically generated session
label filter before creating network/workload resources, and retain its socket
for the session lifetime. Before connecting/registering, wait for the pinned
image's `Started` log within the bootstrap deadline: a Docker-published port can
accept and reset connections before the reaper listens. Failure reports
`ryuk_ready` and attempts reaper removal. Registration failure closes the socket
and removes the unsuccessful reaper while preserving the original error.

Normal teardown force-removes tracked workloads and anonymous volumes. Closing
the Ryuk socket lets the reaper sweep remaining labeled resources and itself,
including after VM termination. Leave it available after direct removal failures
so it can retry. Unreachable Docker can prevent immediate removal; teardown
reports cleanup failures. `ryuk => false` leaves crash recovery to the environment.

Expected failures return tagged errors with stage/reason; workers convert
unexpected exceptions separately. Preserve the startup cause if cleanup also
fails. Requests share monotonic deadlines and an 8 MiB response limit. Log reads
request the last 1,000 lines and decode Docker's non-TTY frames; malformed frames
are rejected. Readiness detects early container exit. Session cleanup has a
separate ten-second budget. Internal caps/budgets and the Ryuk image are not
configurable properties in this phase.

## Verification and limitations

EUnit uses local sockets and a fake Docker boundary. Docker Common Test covers
service host-client round trips/isolation, UI backend visibility/dependencies,
configuration recipes, and normal/abrupt-VM cleanup with a surviving second
session. Docker acceptance is opt-in locally and enabled in CI; missing Docker
fails an enabled run. The CI workflow targets OTP 27/28/29.

Additional presets, service SASL/TLS, multi-broker Kafka, registry credentials,
Docker TLS/named pipes/CLI contexts, custom network selection,
persistence/reuse, Compose, AWS orchestration beyond Moto's upstream mocks, and
YAML/JSON loaders remain outside the implemented scope.

## Upstream references

These explain the selected contracts; links to moving branches are not image pins.

- [PostgreSQL entrypoint](https://github.com/docker-library/postgres/blob/master/docker-entrypoint.sh)
- [Kafbat 1.4.2 API](https://github.com/kafbat/kafka-ui/blob/v1.4.2/contract/src/main/resources/swagger/kafbat-ui-api.yaml)
- [DynamoDB Admin 5.3.4 routes](https://github.com/aaronshaf/dynamodb-admin/blob/v5.3.4/lib/routes.ts)
- [DynamoDB Admin 5.3.4 settings](https://github.com/aaronshaf/dynamodb-admin/blob/v5.3.4/README.md)

- [Moto server mode](https://docs.getmoto.org/en/latest/docs/server_mode.html)
- [Moto S3 options and coverage](https://docs.getmoto.org/en/latest/docs/services/s3.html)
