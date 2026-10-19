-module(beamlet_config_tests).
-include_lib("eunit/include/eunit.hrl").

unconfigured_defaults_test() ->
    {ok, Snapshot} = beamlet_config:snapshot(#{}, #{}),
    {ok, C} = beamlet_config:container(Snapshot, beamlet_postgres:new()),
    ?assertEqual(<<"postgres:17.6-alpine">>, maps:get(image, C)),
    ?assertEqual(<<"beamlet">>, maps:get(username, C)),
    ?assertEqual(120000, maps:get(timeout, C)).

constructor_defaults_resolve_at_startup_test() ->
    ?assertEqual(#{kind => postgres}, beamlet_postgres:new()),
    ?assertEqual(#{kind => kafka}, beamlet_kafka:new()),
    ?assertEqual(#{kind => redis, timeout => 120000}, beamlet_redis:new(#{timeout => 120000})),
    {ok, Snapshot} = beamlet_config:snapshot(#{container => #{timeout => 60000}}, #{}),
    {ok, C} = beamlet_config:container(Snapshot, beamlet_redis:new(#{timeout => 120000})),
    ?assertEqual(120000, maps:get(timeout, C)).

container_precedence_test() ->
    App = #{
        container => #{timeout => 1000, labels => #{<<"app">> => <<"common">>}},
        presets => #{postgres => #{timeout => 2000, database => <<"application">>}},
        recipes => #{<<"db">> => #{kind => postgres, timeout => 5000, database => <<"recipe">>}}
    },
    Local = #{
        container => #{timeout => 3000},
        presets => #{postgres => #{timeout => 4000, database => <<"session">>}}
    },
    {ok, Snapshot} = beamlet_config:snapshot(App, Local),
    {ok, Default} = beamlet_config:container(Snapshot, beamlet_postgres:new()),
    ?assertEqual(4000, maps:get(timeout, Default)),
    {ok, Recipe} = beamlet_config:container(Snapshot, #{recipe => <<"db">>}),
    ?assertEqual(5000, maps:get(timeout, Recipe)),
    ?assertEqual(<<"recipe">>, maps:get(database, Recipe)),
    {ok, Explicit} = beamlet_config:container(
        Snapshot,
        beamlet_postgres:new(#{
            recipe => <<"db">>, timeout => 120000, database => <<"explicit">>
        })
    ),
    ?assertEqual(120000, maps:get(timeout, Explicit)),
    ?assertEqual(<<"explicit">>, maps:get(<<"POSTGRES_DB">>, maps:get(environment, Explicit))).

option_values_replace_in_full_test() ->
    App = #{
        container => #{labels => #{<<"inherited">> => <<"yes">>}, wait => [{tcp, 6379}]},
        presets => #{kafka => #{topics => [<<"inherited">>]}}
    },
    {ok, Snapshot} = beamlet_config:snapshot(App, #{}),
    {ok, C} = beamlet_config:container(
        Snapshot,
        beamlet_kafka:new(#{
            labels => #{}, wait => [], topics => []
        })
    ),
    ?assertEqual(#{}, maps:get(labels, C)),
    ?assertEqual([], maps:get(wait, C)),
    ?assertEqual([], maps:get(topics, C)),
    {ok, GenericDefaults} = beamlet_config:snapshot(
        #{
            presets => #{
                generic => #{
                    image => <<"example:1">>, environment => #{<<"INHERITED">> => <<"yes">>}
                }
            }
        },
        #{}
    ),
    {ok, Generic} = beamlet_config:container(GenericDefaults, #{environment => #{}}),
    ?assertEqual(#{}, maps:get(environment, Generic)).

session_recipe_replaces_definition_test() ->
    App = #{recipes => #{<<"service">> => #{kind => postgres, database => <<"old">>}}},
    Local = #{recipes => #{<<"service">> => #{kind => redis}}},
    {ok, Snapshot} = beamlet_config:snapshot(App, Local),
    {ok, C} = beamlet_config:container(Snapshot, #{recipe => <<"service">>}),
    ?assertEqual(redis, maps:get(kind, C)),
    ?assertNot(maps:is_key(database, C)).

recipe_selection_and_kind_errors_test() ->
    {ok, Snapshot} = beamlet_config:snapshot(
        #{
            recipes => #{<<"cache">> => #{kind => redis}}
        },
        #{}
    ),
    ?assertMatch(
        {error, #{reason := unknown_recipe}},
        beamlet_config:container(Snapshot, #{recipe => <<"missing">>})
    ),
    ?assertMatch(
        {error, #{reason := kind_mismatch}},
        beamlet_config:container(Snapshot, beamlet_postgres:new(#{recipe => <<"cache">>}))
    ),
    ?assertMatch({error, _}, beamlet_container:validate(#{recipe => <<"cache">>})).

all_kinds_receive_defaults_test() ->
    App = #{
        container => #{timeout => 45000},
        presets => #{
            generic => #{image => <<"example:1">>}, postgres => #{password => <<"test-secret">>}
        }
    },
    {ok, Snapshot} = beamlet_config:snapshot(App, #{}),
    Target = beamlet:make_container(<<"id">>, <<"target">>, self(), <<"localhost">>, #{}),
    Inputs = [
        #{},
        beamlet_kafka:new(),
        beamlet_dynamodb:new(),
        beamlet_postgres:new(),
        beamlet_redis:new(),
        beamlet_moto:new(),
        beamlet_kafka_ui:new(Target),
        beamlet_dynamodb_ui:new(Target)
    ],
    lists:foreach(
        fun(Input) ->
            {ok, C} = beamlet_config:container(Snapshot, Input),
            ?assertEqual(45000, maps:get(timeout, C))
        end,
        Inputs
    ),
    {ok, PG} = beamlet_config:container(Snapshot, beamlet_postgres:new()),
    ?assertEqual(<<"test-secret">>, maps:get(password, PG)).

invalid_layers_are_not_hidden_by_overrides_test() ->
    ?assertMatch(
        {error, #{stage := config, path := [application, container, timeout]}},
        beamlet_config:snapshot(#{container => #{timeout => 0}}, #{container => #{timeout => 1000}})
    ),
    Invalid = [
        #{unknown => #{}},
        #{session => #{owner => self()}},
        #{presets => #{redis => #{ports => [6379]}}},
        #{presets => #{postgres => #{password => <<>>}}},
        #{recipes => #{bad_atom_name => #{kind => redis}}},
        #{recipes => #{<<"bad">> => #{kind => future_kind}}},
        #{recipes => #{<<"bad">> => #{kind => generic}}},
        #{recipes => #{<<"bad">> => #{kind => kafka_ui, target => self()}}}
    ],
    lists:foreach(
        fun(Config) ->
            ?assertMatch({error, #{stage := config}}, beamlet_config:snapshot(Config, #{}))
        end,
        Invalid
    ).

configuration_errors_do_not_include_values_test() ->
    Secret = <<"credential-must-not-appear">>,
    Error = beamlet_config:snapshot(#{presets => #{postgres => #{password => [Secret]}}}, #{}),
    ?assertMatch({error, #{path := [application, presets, postgres, password]}}, Error),
    ?assertEqual(nomatch, binary:match(term_to_binary(Error), Secret)).

session_connection_precedence_test() ->
    App = #{session => #{docker_host => <<"tcp://app:2375">>, host => <<"app-client">>}},
    Explicit = #{config => #{session => #{host => <<"session-client">>}}},
    Env = #{docker_host => <<"tcp://environment:2376">>, host => <<"env-client">>},
    {ok, Options, _} = beamlet_config:session(App, Explicit, Env),
    ?assertEqual(<<"env-client">>, maps:get(host, maps:get(connection, Options))),
    ?assertEqual({tcp, "environment", 2376}, maps:get(transport, maps:get(connection, Options))),
    {ok, Local, _} = beamlet_config:session(App, Explicit#{host => <<"explicit-client">>}, Env),
    ?assertEqual(<<"explicit-client">>, maps:get(host, maps:get(connection, Local))),
    ?assertEqual(self(), maps:get(owner, Local)),
    {ok, Unix, _} = beamlet_config:session(
        #{}, #{docker_host => <<"unix:///tmp/docker.sock">>}, #{}
    ),
    ?assertEqual("/tmp/docker.sock", maps:get(docker_socket, Unix)).

invalid_connection_defaults_fail_purely_test() ->
    lists:foreach(
        fun(Session) ->
            ?assertMatch(
                {error, #{stage := config}}, beamlet_config:snapshot(#{session => Session}, #{})
            )
        end,
        [
            #{host => <<"invalid host">>},
            #{docker_host => <<"https://unsupported:2376">>},
            #{docker_socket => 42},
            #{ryuk => yes},
            #{timeout => 0}
        ]
    ).

malformed_lists_return_configuration_errors_test() ->
    Invalid = [
        #{presets => #{generic => #{ports => [80 | bad]}}},
        #{container => #{wait => [{tcp, 80} | bad]}},
        #{session => #{docker_socket => [47 | bad]}},
        #{session => #{docker_host => <<"unix:///tmp/docker", 0, ".sock">>}}
    ],
    lists:foreach(
        fun(Config) ->
            ?assertMatch({error, #{stage := config}}, beamlet_config:snapshot(Config, #{}))
        end,
        Invalid
    ).
