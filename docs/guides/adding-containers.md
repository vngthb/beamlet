# Adding support for another container

Start with a generic config and wrap it in your application's helper for reuse.
A built-in preset is useful for managed configuration, service-specific readiness,
or connection accessors maintained by Beamlet. The current library has explicit
preset branches, not a plugin registry or callback behaviour.

Read the [usage guide](using-beamlet.md) for configuration fields, waits, and
fixtures, and [architecture](../architecture.md) for ownership/cleanup contracts.
For reusable definitions without a helper module, use a generic
[recipe in `sys.config`](configuration.md).

## Try a generic configuration

For an image supported by environment, command, published TCP ports, and existing
wait strategies, use `beamlet_container:new/2` in an active session:

```erlang
Config = beamlet_container:new(<<"redis:7.4.11-alpine">>, #{
    ports => [6379],
    command => [<<"redis-server">>, <<"--save">>, <<>>, <<"--appendonly">>, <<"no">>],
    wait => [{command, [<<"redis-cli">>, <<"PING">>]}]
}),
{ok, Container} = beamlet:start_container(Session, Config),
{ok, {Host, Port}} = beamlet:endpoint(Container, 6379).
```

Redis already has a built-in preset; it demonstrates a complete configuration
against a known image here. For another image, inspect its startup contract and
select its command, environment, container ports, and readiness check. Pin a
tested version. Do not invent a `kind` atom: validation accepts implemented kinds.

Generic containers have no implicit service readiness. Choose an observable
success condition: a protocol command or health route is stronger than an open
port. A command wait checks exit status, not its output. Checks run sequentially
under the shared startup deadline.

## Package it as a reusable helper

Save this complete example as `src/my_test_redis.erl` in the consumer project:

```erlang
-module(my_test_redis).
-export([new/0, new/1, client_config/1]).

new() -> new(#{}).

new(Options) ->
    Defaults = #{
        image => <<"redis:7.4.11-alpine">>,
        ports => [6379],
        command => [<<"redis-server">>, <<"--save">>, <<>>, <<"--appendonly">>, <<"no">>],
        wait => [{command, [<<"redis-cli">>, <<"PING">>]}]
    },
    Config = maps:merge(Defaults, Options),
    beamlet_container:new(maps:get(image, Config), Config).

client_config(Container) ->
    {ok, {Host, Port}} = beamlet:endpoint(Container, 6379),
    #{host => Host, port => Port}.
```

Use it through the usual session API:

```erlang
ok = beamlet:with_session(#{}, fun(Session) ->
    {ok, Redis} = beamlet:start_container(Session, my_test_redis:new(#{timeout => 30000})),
    #{host := Host, port := Port} = my_test_redis:client_config(Redis),
    io:format("~s:~B~n", [Host, Port]),
    ok
end).
```

This remains a generic container. `maps:merge/2` replaces whole option values,
including lists and nested maps; it is not recursive. This helper allows callers
to replace defaults, so its accessor assumes port 6379 remains exposed. Define
a narrower helper API if that assumption must be enforced. Beamlet still
validates supported fields at startup. No registration or handle construction
is needed.

Options materialized by this helper count as explicit values and override shared
defaults. Use a named recipe when consumers should edit these properties through
`sys.config`. Built-in constructors instead return sparse specs.

The generic API has no volumes, custom network selection, fixed host ports,
custom entrypoint, arbitrary Docker request body, or startup callbacks. An image
requiring one of these needs a separately designed library change; a helper
cannot make an unsupported key work.

## Contribute a built-in preset

Use [Redis](../../src/beamlet_redis.erl) and
[PostgreSQL](../../src/beamlet_postgres.erl) as small examples. Kafka's mapped
advertised-listener startup is more involved and is not the default template.
[Moto](../../src/beamlet_moto.erl) demonstrates a protocol probe and AWS client
settings.

Specify the image pin, container port, typed options, ephemeral storage behaviour,
readiness success/failure, connection accessor, and supported image overrides.
Identify library-owned fields. Keep service-client dependencies integration-only
unless runtime behaviour needs them.

| Location | Work for a new built-in service |
| --- | --- |
| New `src/beamlet_<service>.erl` | Sparse constructor `new/0,1`, internal pure `defaults/1` builder, and connection accessor using `beamlet:endpoint/2`. No Docker I/O or global config reads in constructors. |
| [`beamlet_container`](../../src/beamlet_container.erl) | Extend `spec/0`, `config/0`, `kinds/0`, partial `validate_options/2`, the `defaults/2` dispatch, typed `service_options/1`, and `managed_config/1` guards. Reject invalid options before Docker effects; compare managed fields against defaults generated from typed options. |
| [`beamlet_config`](../../src/beamlet_config.erl) | Review kind defaults/recipe resolution and validation; new kinds use the existing namespace and precedence rules. |
| [`beamlet_runtime`](../../src/beamlet_runtime.erl) | Add a `ready/6` branch. Reuse `start/7` and existing waits where sufficient; wrap expected failures with `stage/2`. |
| [`beamlet_wait`](../../src/beamlet_wait.erl) | Add an internal protocol probe only if existing waits are insufficient. Honour the deadline and mapped endpoint, handle fragmented responses, and preserve early-exit/error handling. |
| `rebar.config` and tests | Add integration-only clients if needed, defaults/recipe/readiness failure unit tests, and a Docker `test/beamlet_<service>_SUITE.erl` proving host-client behaviour. |
| README, guides, architecture | Document options, accessor values, readiness, image pin, limits, and ownership. Rebar populates the application's module list during build. |

A new `kind` in a constructor alone fails validation. Adding it only to validation
leaves runtime without a matching readiness branch. Read the full path first.

### Typed options and connection data

Keep host ports dynamic and use the host from `beamlet:endpoint/2`. Return a small
client-facing map; do not expose Docker connection data or require consumers to
pattern-match opaque handles.

Most services need only host/port. If new settings must survive in the handle,
`beamlet_runtime:start/7` copies PostgreSQL `username`, `password`, and
`database`, plus Moto `region`, `access_key_id`, and `secret_access_key`. Extend
that copy and validation deliberately. Internal
`make_container`, `settings`, `with_settings`, `reference`, and `private_host`
helpers are not public extension APIs; keep their use inside Beamlet.

Derive managed command/environment/ports from typed options and enforce them in
`managed_config/1`. Unrestricted environment overrides can break listeners,
readiness, isolation, and cleanup. Avoid credentials/full configs in diagnostics.

### Readiness and errors

Constructors do not wait. Runtime invokes built-in readiness, then extra waits.
Preserve one deadline across pull, create, start, initialization, and polling.
Do not reset timeouts on each attempt or retry an ambiguous resource creation.
Return expected errors through the staged result path. Workers handle unexpected
exceptions. Preserve the original startup cause even if cleanup also fails.

Private probe atoms in `beamlet_wait` are not public strategies. A new public
strategy additionally needs `valid/1` validation, documentation, and tests.
There is no public custom function/callback wait contract today.

### Companions require lifecycle integration

A UI config with `target` is not enough. Existing integration also checks target
kind in `beamlet_session:target/2`, verifies live Docker identity at runtime,
builds managed private-endpoint configuration, and tracks dependencies for
cancellation/removal. Read [Kafka UI](../../src/beamlet_kafka_ui.erl),
[DynamoDB UI](../../src/beamlet_dynamodb_ui.erl), and
[session](../../src/beamlet_session.erl) before extending them.

Preserve same-session running targets, UI failure/stop preserving its target,
and target stop cancelling pending starts and removing dependents first. Failed
dependent removal blocks direct target removal. Remove containers before the
session network; leave Ryuk available for failed removals. Peers use private
aliases; host clients use published endpoints. Kafka must retain its advertised
external endpoint alongside INTERNAL alias:9093.

## Verify support

For a consumer helper, compile it and test a real container through its returned
host endpoint. Verify declared behaviour, failures, and cleanup rather than just
the constructed map.

For built-in changes, start with failing config/readiness tests. Use the fake
Docker boundary for errors/deadlines. Use real Docker for host-client round trips,
two-container data isolation, failed-start cleanup, and teardown. Protocol probes
need partial-response and timeout coverage. Companion tests need target-backed
seeded data visibility and lifecycle checks; a landing page HTTP 200 is insufficient.

After implementation and review, from the repository root run:

```sh
rebar3 fmt --check
rebar3 lint
rebar3 compile
rebar3 eunit
rebar3 dialyzer
BEAMLET_INTEGRATION=1 rebar3 as integration ct
```

Keep Elvis rules enabled. The integration profile defines `BEAMLET_INTEGRATION` for SDK-dependent test
headers; keep those includes conditional so default EUnit/CT compile without the
SDK. Integration needs Docker, image access, a C compiler,
Make, and CMake for client dependencies. Without `BEAMLET_INTEGRATION=1`, repository
CT skips Docker cases. Record actual results and environment limits; compile
success alone is not evidence of connectivity or cleanup.
