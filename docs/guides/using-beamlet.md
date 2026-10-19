# Using Beamlet

Create a session, start containers, use their returned endpoints, then stop the
session. Docker assigns every published host port. See also
[adding container support](adding-containers.md), [architecture](../architecture.md),
[shared configuration](configuration.md), and the [README](../../README.md).

## Set up a consumer project

Requirements: Erlang/OTP 27+, Rebar3, and a reachable Docker daemon. Ryuk needs
permission to mount the daemon's Docker socket. Beamlet calls Docker's HTTP API
directly; it does not require the Docker CLI at runtime.

No package publication is recorded for this checkout. To use the local source,
add this dependency to your consumer project's `rebar.config`:

```erlang
{deps, [{beamlet, "0.1.0"}]}.
```

Before compiling, link the source into Rebar's checkout directory. Replace the
absolute path below with the path to this repository:

```sh
mkdir -p _checkouts
ln -s /absolute/path/to/beamlet _checkouts/beamlet
rebar3 compile
rebar3 shell
```

`0.1.0` matches local application metadata; it is not a published-release claim.
Keep the checkout link with this dependency configuration. Rebar resolves runtime
dependencies. `start_session` starts Beamlet and its OTP dependencies automatically.

## Start and clean up a session

In the Erlang shell:

```erlang
ok = beamlet:with_session(#{}, fun(Session) ->
    {ok, Redis} = beamlet:start_container(Session, beamlet_redis:new()),
    #{host := Host, port := Port} = beamlet_redis:client_config(Redis),
    io:format("Redis is reachable at ~s:~B~n", [Host, Port]),
    {ok, 0, <<"PONG\n">>} = beamlet:exec(Redis, [<<"redis-cli">>, <<"PING">>]),
    ok
end).
```

Run client operations inside the callback: the service is removed before
`with_session/2` returns. It returns the callback result after successful cleanup.
Cleanup failure returns an error; a callback exception is re-raised, with any
cleanup failure logged. Match the result so a test cannot ignore a cleanup error.

For a longer-lived development session:

```erlang
{ok, Session} = beamlet:start_session(),
{ok, Redis} = beamlet:start_container(Session, beamlet_redis:new()),
RedisConfig = beamlet_redis:client_config(Redis).
%% Keep this owner process alive while using Redis.
%% When finished:
ok = beamlet:stop_session(Session).
```

The creating process is the default owner; its exit triggers cleanup. Stop one
container with `beamlet:stop_container(Redis)`. Use `try ... after` around manually
managed sessions when subsequent code can raise.

## Get endpoints, IDs, and names

With `Redis` from an active session:

```erlang
{ok, {Host, HostPort}} = beamlet:endpoint(Redis, 6379),
ContainerId = beamlet:id(Redis),
{ok, Info} = beamlet:inspect(Redis),
DockerName = maps:get(<<"Name">>, Info).
```

The input `6379` is the **container** port; `HostPort` is the assigned host port.
`Host` is a binary and `HostPort` an integer. An unpublished port returns
`{error, port_not_exposed}`. Endpoints are usable only while the service is alive.
Use the returned host rather than assuming localhost.

Containers have unique generated names such as `beamlet-<32 hex characters>`.
Docker inspection returns the name, normally with a leading slash. There is no
public name override or name getter. Treat handles as opaque; use accessors.

## Choose a service preset

| Constructor | Container port | Connection accessor | Service options |
| --- | --- | --- | --- |
| `beamlet_kafka:new/0,1` | 9092 | `bootstrap_servers/1` → binary authority | `topics => [<<"events">>]` |
| `beamlet_dynamodb:new/0,1` | 8000 | `client_config/1` → endpoint, region, dummy credentials | No additional service options |
| `beamlet_postgres:new/0,1` | 5432 | `client_config/1` → host, port, username, password, database | Binary `username`, `password`, `database`; each defaults to `<<"beamlet">>` |
| `beamlet_moto:new/0,1` | 5000 | `client_config/1` → endpoint, region, test credentials, S3 path-style flag | Binary `region`, `access_key_id`, `secret_access_key`; see [Moto guide](moto.md) |
| `beamlet_redis:new/0,1` | 6379 | `client_config/1` → host, port | No additional service options |

Translate accessor maps to your client's format. Kafka returns a bootstrap
address matching its advertised listener. DynamoDB Local supplies explicit dummy
credentials without reading global AWS settings. PostgreSQL, Redis, and DynamoDB
Local use ephemeral storage; separate containers isolate data.

The table shows built-in defaults before shared configuration/recipes and
explicit overrides. All presets accept `image`, `timeout`, `pull_policy`, `labels`,
and extra `wait` checks. Ports, environment, and commands are managed. Image overrides must remain
compatible with the startup/readiness contract. Defaults are pinned in the
[preset modules](../../src) and described in [architecture](../architecture.md).

## Use a generic container

Within an active session:

```erlang
Config = beamlet_container:new(<<"redis:7.4.11-alpine">>, #{
    ports => [6379],
    command => [<<"redis-server">>, <<"--save">>, <<>>, <<"--appendonly">>, <<"no">>],
    wait => [{command, [<<"redis-cli">>, <<"PING">>]}],
    timeout => 30000
}),
{ok, Container} = beamlet:start_container(Session, Config),
{ok, {Host, Port}} = beamlet:endpoint(Container, 6379).
```

| Generic option | Value and default |
| --- | --- |
| `image` | Required after resolution; nonempty binary from `new/1,2`, a raw map, defaults, or recipe |
| `ports` | Container TCP ports, integers 1–65535; default `[]` |
| `environment` | Binary key/value map; default `#{}` |
| `command` | Binary executable/arguments list; default `[]` uses image command |
| `labels` | Binary key/value map; default `#{}`; session labels remain managed |
| `timeout` | Positive startup milliseconds; default `120000` |
| `pull_policy` | `missing` (default), `always`, or `never` |
| `wait` | Sequential readiness checks; default `[]` |

These are built-in values before shared defaults and explicit overrides.
Unknown keys and fixed-host-port bindings are rejected. Strings are binaries;
empty command arguments are valid, but an empty executable is rejected.
Constructors build sparse input specs; `start_container/2` resolves defaults and
validates before creation. Use [shared configuration and recipes](configuration.md)
for defaults across calls.

| Public readiness strategy | Success condition |
| --- | --- |
| `{tcp, ContainerPort}` | Connect through the mapped host endpoint |
| `{log, <<"ready message">>}` | Container logs contain the bytes |
| `{http, ContainerPort, <<"/health">>, 200}` | Host HTTP request returns the selected status |
| `{command, [<<"executable">>, <<"argument">>]}` | Command in the container exits with zero |

Pull, create, start, preset readiness, and extra waits share one startup deadline.
An open TCP port alone does not prove protocol readiness. Presets add their own
checks before extra waits; generic containers have no implicit service check.
There are no public custom callback or startup-hook options.

## EUnit: own the session inside the test

Save as `test/beamlet_usage_tests.erl` in the consumer project. It connects through
the returned host endpoint and verifies a Redis protocol response:

```erlang
-module(beamlet_usage_tests).
-include_lib("eunit/include/eunit.hrl").

redis_ping_test_() ->
    {timeout, 300, fun() ->
        ok = beamlet:with_session(#{}, fun(Session) ->
            {ok, Redis} = beamlet:start_container(Session, beamlet_redis:new()),
            {ok, {Host, Port}} = beamlet:endpoint(Redis, 6379),
            {ok, Socket} = gen_tcp:connect(binary_to_list(Host), Port,
                [binary, {active, false}, {packet, line}], 5000),
            try
                ok = gen_tcp:send(Socket, <<"*1\r\n$4\r\nPING\r\n">>),
                ?assertEqual({ok, <<"+PONG\r\n">>}, gen_tcp:recv(Socket, 0, 5000))
            after
                gen_tcp:close(Socket)
            end
        end)
    end}.
```

Run `rebar3 eunit --module=beamlet_usage_tests`. This consumer example needs Docker,
unlike Beamlet's fake-daemon EUnit suite. The test timeout allows initial pulls;
startup remains bounded separately. Avoid starting a session under the default
owner in a short-lived fixture setup process.

## Common Test: choose a longer-lived owner

Save as `test/beamlet_usage_SUITE.erl`. CT's utility process owns a shared session;
each case gets a fresh container, removed by its teardown:

```erlang
-module(beamlet_usage_SUITE).
-export([suite/0, all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2, redis_ping/1]).

suite() -> [{timetrap, {minutes, 5}}].
all() -> [redis_ping].

init_per_suite(Config) ->
    {ok, Session} = beamlet:start_session(#{owner => whereis(ct_util_server)}),
    [{beamlet_session, Session} | Config].

end_per_suite(Config) ->
    ok = beamlet:stop_session(proplists:get_value(beamlet_session, Config)).

init_per_testcase(_, Config) ->
    Session = proplists:get_value(beamlet_session, Config),
    {ok, Redis} = beamlet:start_container(Session, beamlet_redis:new()),
    [{redis, Redis} | Config].

end_per_testcase(_, Config) ->
    ok = beamlet:stop_container(proplists:get_value(redis, Config)).

redis_ping(Config) ->
    Redis = proplists:get_value(redis, Config),
    {ok, 0, <<"PONG\n">>} = beamlet:exec(Redis, [<<"redis-cli">>, <<"PING">>]),
    ok.
```

Run `rebar3 ct --suite test/beamlet_usage_SUITE`. This consumer suite runs Docker
directly; Beamlet's repository acceptance opt-in does not gate it. Explicitly stop
the shared session even with a longer-lived owner.

## Add development UIs

```erlang
{ok, Session} = beamlet:start_session(),
{ok, Kafka} = beamlet:start_container(Session, beamlet_kafka:new()),
{ok, UI} = beamlet:start_container(Session, beamlet_kafka_ui:new(Kafka)),
URL = beamlet_kafka_ui:url(UI).
%% Keep this owner alive; call beamlet:stop_session(Session) when finished.
```

DynamoDB uses `beamlet_dynamodb_ui:new(DynamoDB)` and
`beamlet_dynamodb_ui:url(UI)`. Companions need a running target of the correct kind
in the same session. UI stop/failure preserves the target. Target stop removes
UIs first; failed UI removal blocks direct target removal for retry. Private
aliases connect UIs to services; browsers use returned host URLs.

For a complete stack, from the Beamlet repository run `rebar3 compile`, then
`escript examples/dev_stack.escript`. The [example](../../examples/dev_stack.escript)
keeps the owner alive, prints endpoints/URLs, and removes the stack on Enter.

## Configure Docker and diagnose failures

| Session option | Purpose / default |
| --- | --- |
| `config` | Session-local defaults/recipes layer; see [configuration](configuration.md) |
| `owner` | Process whose exit triggers cleanup; default `self()` |
| `timeout` | Session bootstrap milliseconds; default `120000`; independent of container timeout |
| `ryuk` | Crash-recovery reaper; default `true` |
| `docker_host` | Docker API endpoint; built-in `unix:///var/run/docker.sock` |
| `host` | Host reachable by service clients; derived from Docker connection when absent |
| `bind_ip` | Daemon-side IP for all published service/UI and Ryuk ports; default `<<"127.0.0.1">>`; literal IPv4 only |
| `docker_socket` | Daemon-side socket path mounted into Ryuk; defaults to the Unix connection path, or `/var/run/docker.sock` for TCP/HTTP |

Session properties resolve from explicit outer option, then supported environment,
local `config.session`, application `config.session`, and built-in/derived values.
Environment applies only to `DOCKER_HOST` and `TESTCONTAINERS_HOST_OVERRIDE`.
See [configuration precedence](configuration.md#precedence-and-replacement).

For another local socket, for example:

```erlang
beamlet:start_session(#{
    docker_host => <<"unix:///run/user/1000/docker.sock">>,
    docker_socket => <<"/run/user/1000/docker.sock">>
}).
```

Ports now bind to loopback by default. Remote Docker clients must explicitly
choose a `bind_ip` reachable on the Docker host and a corresponding client `host`.
For example, if the daemon has interface address `192.0.2.10`, use
`bind_ip => <<"192.0.2.10">>` with the appropriate `docker_host` and `host` values.
`bind_ip => <<"0.0.0.0">>` deliberately publishes on all IPv4 interfaces. This also
exposes Ryuk's cleanup listener and services with test credentials or no login;
use non-loopback bindings only on trusted, isolated networks. The `host` option
and host-override environment variable never change what interfaces Docker binds.
The same property can be set under `config.session` in `sys.config`.

TCP/HTTP daemon endpoints are supported; TLS and named pipes are not. Distinguish
the daemon endpoint, service-client host, and daemon-side mounted socket. Beamlet
does not read Docker CLI contexts. Shared defaults and recipes use OTP
[`sys.config`](configuration.md); explicit options override configured defaults.
JSON/YAML loaders are outside the supported scope.

Use `beamlet:logs(Container)` → `{ok, Binary}` and `beamlet:exec(Container, Args)`
→ `{ok, ExitCode, Output}` while the container is alive. Startup errors identify
their stage and reason; failed containers are subject to cleanup, so retain the
error rather than expecting a handle. `invalid_config` often means a typo,
nonbinary string, or managed-field override. For connection errors, check daemon
permissions and the service-client host. For readiness timeouts or early exits,
check image compatibility and startup arguments.

If session startup fails at `ryuk_ready`, check the reaper's logs and Docker socket
permissions. Beamlet waits for Ryuk's listener-ready message before registering
the cleanup filter, since Docker can accept connections before Ryuk is listening.
A later `ryuk_register` error means the filter handshake failed. See the
[cleanup architecture](../architecture.md#cleanup-and-failure-handling).

Teardown reports cleanup failures. Ryuk can eventually sweep remaining labeled
resources after teardown or VM termination, subject to daemon availability.
With `ryuk => false`, VM-crash recovery is your environment's responsibility.

Errors with `stage => config` and a `path` identify invalid shared/session
configuration or recipe selection; fix the referenced keys before retrying.
Existing sessions keep their snapshots, so create a new session after changing
shared properties. See [configuration](configuration.md#independent-session-snapshots).
