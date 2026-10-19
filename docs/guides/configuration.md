# Shared configuration and recipes

Put shared properties in your consumer's OTP `sys.config`. Beamlet reads the
application environment key `config` when a session starts. The complete
[example configuration](../../examples/sys.config) covers all four namespaces:

```erlang
[
    {beamlet, [
        {config, #{
            session => #{timeout => 120000},
            container => #{pull_policy => missing},
            presets => #{postgres => #{username => <<"tester">>, password => <<"test-secret">>}},
            recipes => #{
                <<"app_db">> => #{kind => postgres, database => <<"orders_test">>},
                <<"cache">> => #{kind => redis}
            }
        }}
    ]}
].
```

Load it through your release configuration or start the VM with
`erl -config path/to/sys`. [OTP](https://www.erlang.org/doc/apps/kernel/config.html)
loads `sys.config`; Beamlet reads its application environment. Beamlet has no
file-discovery or YAML/JSON loader API. An absent `config` means `#{}`.

For a [Rebar consumer](using-beamlet.md#set-up-a-consumer-project), copy the example
to `config/sys.config`, compile, then start a shell:

```sh
rebar3 compile
rebar3 shell --config config/sys.config
```

Rebar also supports `rebar3 ct --sys_config config/sys.config` for tests. These
options are described in its [command reference](https://www.rebar3.org/docs/commands/).

## Start named recipes

With the example loaded:

```erlang
ok = beamlet:with_session(#{}, fun(Session) ->
    {ok, DB} = beamlet:start_container(Session, #{recipe => <<"app_db">>}),
    #{username := <<"tester">>, database := <<"orders_test">>} =
        beamlet_postgres:client_config(DB),
    {ok, Cache} = beamlet:start_container(Session, #{recipe => <<"cache">>}),
    {ok, 0, <<"PONG\n">>} = beamlet:exec(Cache, [<<"redis-cli">>, <<"PING">>]),
    ok
end).
```

A recipe defines one container and requires a `kind`. Its name is a nonempty
binary. Select it explicitly with `recipe`; no recipe runs automatically. Its
kind supplies an omitted input kind. An explicitly different kind is an error.

Override recipe values at the call site:

```erlang
beamlet:start_container(Session, beamlet_postgres:new(#{
    recipe => <<"app_db">>, database => <<"one_test">>
})).
```

Companions still need a running target in the same session. With the full example
configuration, use `beamlet_kafka_ui:new(Kafka, #{recipe => <<"kafka_browser">>})`
or `beamlet_dynamodb_ui:new(Dynamo, #{recipe => <<"dynamo_browser">>})`.
Recipes cannot store live targets or start a stack. The existing target checks
and [companion cleanup rules](using-beamlet.md#add-development-uis) apply.

## Supported properties

| Namespace | Accepted keys |
| --- | --- |
| `session` | `timeout`, `ryuk`, `docker_host`, `host`, `docker_socket`, `bind_ip`; see [session options](using-beamlet.md#configure-docker-and-diagnose-failures). `owner` is supplied only to `start_session/1`. |
| `container` | Common defaults for every workload: `timeout`, `pull_policy`, `labels`, `wait`. |
| `presets` | Kind maps keyed by `generic`, `kafka`, `dynamodb`, `postgres`, `redis`, `moto`, `kafka_ui`, or `dynamodb_ui`. Every kind accepts the common keys plus `image`. |
| `recipes` | Binary names mapped to `#{kind => Kind, ...Options}`. Options follow the corresponding preset rules. |

Additional kind options are `ports`, `environment`, and `command` for `generic`;
`topics` for `kafka`; `username`, `password`, `database` for `postgres`; and
`region`, `access_key_id`, `secret_access_key` for `moto`. See the
[Moto guide](moto.md) for defaults and S3 client setup.
See [generic values and readiness strategies](using-beamlet.md#use-a-generic-container).
Strings are binaries; option keys and kinds are existing atoms. Unknown keys fail.

Generic preset defaults may omit `image`. A generic recipe needs an effective
image from either its definition or generic preset defaults. Use a raw map such
as `#{recipe => <<"helper">>}` for a generic recipe; `beamlet_container:new(Image)`
always supplies an explicit image. Presets keep their ports, command, and
environment managed and derive them from the resolved typed options.

`session.timeout` bounds bootstrap; `container.timeout` bounds each workload's
startup. Common defaults do not configure Ryuk. The Ryuk image and internal
operation/cleanup limits remain internal. Ports are always Docker-assigned.
`session.bind_ip` defaults to `<<"127.0.0.1">>` and applies to every published
service/UI port and Ryuk. It follows session precedence and must be a literal
IPv4 address. `host` and the host-override environment variable change client
addresses only. See [remote access](using-beamlet.md#configure-docker-and-diagnose-failures).

## Precedence and replacement

Container options resolve from highest to lowest priority:

1. Explicit start/constructor options.
2. The selected recipe.
3. Session `config.presets[Kind]`.
4. Session `config.container`.
5. Application `config.presets[Kind]`.
6. Application `config.container`.
7. Built-in defaults.

An explicit value wins even when it equals an old built-in default. Each option
replaces the whole earlier value. Labels and environment maps do not merge by
entry; lists do not append. Use `labels => #{}` or `environment => #{}` to clear
inherited entries, and `topics => []` or `wait => []` to clear inherited lists.
Preset readiness still runs when additional waits are empty. Beamlet reapplies
its required cleanup labels.

Structural namespace maps merge by option and kind. A local recipe with an
existing name replaces the entire recipe definition and must include its kind.
Empty structural namespaces add no overrides; there are no deletion markers or
recipe inheritance.

Session properties resolve in this order: explicit outer option, supported
environment variable, local `config.session`, application `config.session`, then
built-in/derived defaults. Environment applies only to `DOCKER_HOST` and
`TESTCONTAINERS_HOST_OVERRIDE`. The client host and Ryuk socket path derive from
the selected Docker endpoint when absent.

## Independent session snapshots

Provide a local layer using the same schema without changing global properties:

```erlang
ok = beamlet:with_session(#{
    config => #{presets => #{postgres => #{database => <<"isolated_test">>}}}
}, fun(Session) ->
    {ok, DB} = beamlet:start_container(Session, beamlet_postgres:new()),
    #{database := <<"isolated_test">>} = beamlet_postgres:client_config(DB),
    %% Use the database here while the session is alive.
    ok
end).
```

Application config and supported environment values are captured once per
session. Later changes affect new sessions only. Constructors do not read global
configuration. Concurrent sessions can resolve the same spec differently.
Owners remain explicit PIDs, defaulting to the process calling `start_session`.

All supplied layers are validated, including values hidden by later overrides.
Invalid defaults or unused recipes fail before Docker requests. Unknown recipes,
kind mismatches, and invalid container inputs fail before workload allocation.
Configuration errors identify a path and reason without dumping credential values
or complete configuration maps.

## Constructor migration

Constructors now return input specifications containing kind, explicit options,
and a UI target when applicable. For example, `beamlet_postgres:new()` returns
`#{kind => postgres}`. Existing start call patterns work unchanged.

Code inspecting fully populated constructor maps must change. For built-in-only
expansion, call `beamlet_container:validate(Spec)` and match `{ok, Config}`. That
function is context-free: it ignores application/session defaults and rejects
recipe references. `start_container/2` resolves recipes against its session
snapshot. For a running service, use its connection accessor or
`beamlet:inspect/1`; treat handles as opaque.

Previously constructed complete maps are explicit values, so their stored
defaults override configured defaults. Prefer the sparse constructors when
shared properties should apply.
