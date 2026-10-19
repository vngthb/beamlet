# beamlet

Disposable Docker containers for Erlang/OTP, with Kafka, DynamoDB Local, PostgreSQL,
Redis, and Moto Server (S3/AWS mock) presets, plus optional Kafka and DynamoDB
development UIs.

Requires OTP 27+, Rebar3, and access to a Docker daemon. Beamlet talks to Docker's
HTTP API directly without invoking the Docker CLI.

## Guides

- [How Beamlet works](docs/guides/how-beamlet-works.md): a short implementation
  walkthrough with diagrams, from startup to cleanup.
- [Using Beamlet](docs/guides/using-beamlet.md): local dependency setup, sessions,
  endpoints and names, service options, EUnit/Common Test fixtures, and diagnostics.
- [S3 and AWS mocks with Moto](docs/guides/moto.md): S3 clients, test credentials,
  shared configuration, and mock limitations.
- [Shared configuration and recipes](docs/guides/configuration.md): OTP `sys.config`,
  session overrides, precedence, and constructor migration.
- [Adding container support](docs/guides/adding-containers.md): reusable generic
  configs and the workflow for contributing a built-in preset.
- [Architecture and limitations](docs/architecture.md): implementation contracts.

Examples using `with_session/2` must perform client operations inside the callback;
containers are removed when it returns. For a dependency setup walkthrough, start
with the usage guide above.

## Kafka

```erlang
beamlet:with_session(#{}, fun(Session) ->
    Config = beamlet_kafka:new(#{topics => [<<"events">>]}),
    {ok, Kafka} = beamlet:start_container(Session, Config),
    BootstrapServers = beamlet_kafka:bootstrap_servers(Kafka),
    %% Pass BootstrapServers to your Kafka client and run your test.
    io:format("Kafka bootstrap servers: ~s~n", [BootstrapServers])
end).
```

Kafka runs in single-node KRaft mode. Docker assigns its host port; Beamlet
configures the advertised listener before starting the broker. Use the returned
bootstrap endpoint. There is one port-allocation strategy and no host-port option.

## DynamoDB Local

```erlang
{ok, Session} = beamlet:start_session(),
try
    {ok, DynamoDB} = beamlet:start_container(Session, beamlet_dynamodb:new()),
    ClientConfig = beamlet_dynamodb:client_config(DynamoDB)
    %% Configure your client with its endpoint, region, and dummy credentials.
after
    ok = beamlet:stop_session(Session)
end.
```

`client_config/1` returns `endpoint`, `region`, `access_key_id`, and
`secret_access_key`. Translate these keys to your client's configuration format.
Data is in memory and isolated per container. No AWS account or real credentials
are needed. DynamoDB Local has [differences from hosted DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html).

## PostgreSQL and Redis

```erlang
beamlet:with_session(#{}, fun(Session) ->
    {ok, Postgres} = beamlet:start_container(Session, beamlet_postgres:new(#{
        username => <<"tester">>, password => <<"test-secret">>, database => <<"app_test">>
    })),
    PostgresClient = beamlet_postgres:client_config(Postgres),
    %% #{host := binary(), port := integer(), username := binary(),
    %%   password := binary(), database := binary()}
    {ok, Redis} = beamlet:start_container(Session, beamlet_redis:new()),
    RedisClient = beamlet_redis:client_config(Redis),
    %% #{host := binary(), port := integer()}
    {PostgresClient, RedisClient}
end).
```

PostgreSQL defaults its test database, username, and password to `beamlet`. It is
ready after the final server executes a SQL query over TCP. Redis requires a PONG
through its mapped host endpoint and disables snapshots and append-only storage.
Both are ephemeral and use Docker-assigned ports. Translate the returned binary
strings to your client's expected format. epgsql and eredis are test-only clients;
Beamlet adds no service-client runtime dependency.

## S3 with Moto Server

```erlang
beamlet:with_session(#{}, fun(Session) ->
    {ok, Moto} = beamlet:start_container(Session, beamlet_moto:new()),
    ClientConfig = beamlet_moto:client_config(Moto)
    %% Configure your S3 client and perform bucket/object operations here.
end).
```

The returned map contains `endpoint`, `region`, `access_key_id`,
`secret_access_key`, and `force_path_style => true`. Moto is an Apache-2.0 AWS mock
and needs no AWS account or vendor token. The same endpoint serves its other
supported AWS APIs. See the [Moto guide](docs/guides/moto.md) for client setup,
`sys.config` properties, and limits.

## Optional development UIs

```erlang
{ok, Session} = beamlet:start_session(),
{ok, Kafka} = beamlet:start_container(Session, beamlet_kafka:new()),
{ok, KafkaUI} = beamlet:start_container(Session, beamlet_kafka_ui:new(Kafka)),
KafkaURL = beamlet_kafka_ui:url(KafkaUI),
{ok, Dynamo} = beamlet:start_container(Session, beamlet_dynamodb:new()),
{ok, DynamoUI} = beamlet:start_container(Session, beamlet_dynamodb_ui:new(Dynamo)),
DynamoURL = beamlet_dynamodb_ui:url(DynamoUI).
%% Keep this owner process alive while using the UIs.
%% Stop the whole stack with beamlet:stop_session(Session).
```

Services start headless; companions are explicit. Kafka uses Kafbat UI and DynamoDB
uses DynamoDB Admin. Readiness checks both a browser page and a backend route that
queries the target. The UIs use private aliases on a session-owned bridge network;
clients and browsers use the returned host endpoints and URLs.

A companion must target a running service of the correct kind in the same session.
Stopping a UI leaves its service running. Failed UI startup preserves the service.
Stopping a service cancels pending UI starts and removes its UIs first. Session
teardown removes containers before the network. Each session has its own network,
including headless sessions. These are local development UIs with no added login.

For a complete stack with an owner that stays alive until explicit shutdown:

```sh
rebar3 compile
escript examples/dev_stack.escript
```

The example prints endpoints and browser URLs. Press Enter to remove the stack;
Ryuk also handles an abrupt VM exit. Shared properties and named recipes can be
stored in OTP application configuration; see the [configuration guide](docs/guides/configuration.md)
and [example `sys.config`](examples/sys.config).

## Shared properties and recipes

Load the [example configuration](examples/sys.config) through OTP, then select a
recipe explicitly:

```erlang
ok = beamlet:with_session(#{}, fun(Session) ->
    {ok, DB} = beamlet:start_container(Session, #{recipe => <<"app_db">>}),
    #{host := Host, port := Port} = beamlet_postgres:client_config(DB),
    io:format("PostgreSQL is reachable at ~s:~B~n", [Host, Port])
end).
```

Defaults are captured per session; explicit options override selected recipes and
shared defaults. Constructors now return sparse input specs, with defaults filled
in at startup. See the [configuration guide](docs/guides/configuration.md) for
precedence, local overrides, and migration from populated constructor maps.

## Generic containers

```erlang
Config = beamlet_container:new(<<"redis:7.4.11-alpine">>, #{
    ports => [6379],
    wait => [{tcp, 6379}],
    timeout => 30000
}),
{ok, Container} = beamlet:start_container(Session, Config),
{ok, {Host, Port}} = beamlet:endpoint(Container, 6379),
ok = beamlet:stop_container(Container).
```

Configuration keys are `image`, `ports`, `environment`, `command`, `labels`,
`timeout` (milliseconds), `pull_policy` (`missing`, `always`, or `never`), and
`wait`. Strings are binaries. Image pulls default to `missing`.

Readiness checks run in order under one startup deadline:

- `{tcp, ContainerPort}`
- `{log, <<"ready message">>}`
- `{http, ContainerPort, <<"/health">>, 200}`
- `{command, [<<"executable">>, <<"argument">>]}` — requires exit code zero

Presets manage their ports, command, and environment. Configure Kafka topics with
`topics => [<<"name">>]`. All service and UI presets accept `image`, `timeout`, `pull_policy`,
`labels`, and additional `wait` checks.

Use `beamlet:id/1`, `beamlet:inspect/1`, `beamlet:logs/1`, and
`beamlet:exec(Container, [<<"command">>])` for inspection and diagnostics.
`exec/2` returns `{ok, ExitCode, Output}`. Lifecycle operations return tagged
errors; startup failures include the failing stage and reason.

## Sessions and test fixtures

A session belongs to its creating process. Owner exit, failed startup, and
explicit teardown trigger cleanup. `with_session/2` cleans up after both normal
returns and exceptions; cleanup failure is returned on a normal path, while a test
exception is preserved and any cleanup failure is logged.

For EUnit, keep `with_session/2` inside the test function. For a shared Common
Test fixture, choose an explicit owner that outlives setup, such as
`whereis(ct_util_server)`. The
[usage guide](docs/guides/using-beamlet.md#eunit-own-the-session-inside-the-test)
has complete EUnit/Common Test modules and teardown examples.

Ryuk provides cleanup after an abrupt VM exit. It needs permission to mount the
Docker socket. Explicitly disabling it with `ryuk => false` leaves crash recovery
to your environment. Direct teardown failures are reported; reaper removal is
eventual and depends on daemon availability.

## Docker configuration

The default is `unix:///var/run/docker.sock`. Set `DOCKER_HOST` or the session
`docker_host` option for another Unix socket or an explicit HTTP/TCP endpoint.
TLS and Windows named pipes are outside the initial supported scope.

Published ports bind to `127.0.0.1` by default, including Ryuk's cleanup port.
The session `bind_ip` option accepts a literal IPv4 address on the Docker
host. Use a non-loopback address only when remote access is intentional and the
network is trusted; this exposes test services and the cleanup listener.

The session `host` option or `TESTCONTAINERS_HOST_OVERRIDE` sets the host reachable
by service clients; it does not change the bind address. The `docker_socket`
option sets the daemon-side socket path
mounted into Ryuk. Beamlet does not read Docker CLI contexts or global AWS settings.
Explicit session options override supported environment values, then local and
application session defaults, then built-ins. See the
[configuration guide](docs/guides/configuration.md#precedence-and-replacement)
for precedence and [architecture](docs/architecture.md) for the full contract.

## Development

```sh
rebar3 fmt
rebar3 fmt --check
rebar3 lint
rebar3 compile
rebar3 eunit
rebar3 dialyzer
```

`fmt` applies [erlfmt](https://github.com/WhatsApp/erlfmt) to source, tests, and
Erlang configuration; `fmt --check` verifies formatting without writing files.
`lint` runs [Elvis](https://github.com/project-fifo/rebar3_lint) with
`elvis.config`, using the standard source/test rulesets with no disabled rules or
module exceptions. Elvis enforces the 100-character source-line limit alongside
erlfmt formatting. CI requires both checks to pass.

EUnit uses local sockets without Docker. Full acceptance tests require a working
Docker daemon, network access to pull pinned images, and a C compiler, Make, and
CMake to build Brod's integration-only dependency:

```sh
BEAMLET_INTEGRATION=1 rebar3 as integration ct
```

Without `BEAMLET_INTEGRATION=1`, Common Test explicitly skips Docker acceptance.
CI enables it and fails if Docker or a required service is unavailable.

CI targets OTP 27/28/29.
