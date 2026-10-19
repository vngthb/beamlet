-module(beamlet_config_session_tests).
-include_lib("eunit/include/eunit.hrl").

application_defaults_reach_docker_test() ->
    fixture(
        #{presets => #{generic => #{image => <<"configured:1">>, ports => [8000]}}},
        fun(D, Options) ->
            ok = beamlet:with_session(Options, fun(S) ->
                {ok, C} = beamlet:start_container(S, #{}),
                ?assertMatch({ok, {_, _}}, beamlet:endpoint(C, 8000)),
                [Body] = creates(D),
                ?assertEqual(<<"configured:1">>, maps:get(<<"Image">>, Body))
            end)
        end
    ).

live_sessions_keep_independent_snapshots_test() ->
    fixture(generic(<<"old:1">>), fun(D, Options) ->
        ok = beamlet:with_session(Options, fun(A) ->
            ok = application:set_env(beamlet, config, generic(<<"new:1">>)),
            ok = beamlet:with_session(Options, fun(B) ->
                parallel_starts([A, B]),
                ?assertEqual(
                    [<<"new:1">>, <<"old:1">>],
                    lists:sort([maps:get(<<"Image">>, Body) || Body <- creates(D)])
                )
            end)
        end)
    end).

session_overrides_and_recipes_reach_client_settings_test() ->
    App = #{
        presets => #{postgres => #{username => <<"app-user">>}},
        recipes => #{<<"db">> => #{kind => postgres, database => <<"recipe-db">>}}
    },
    fixture(App, fun(D, Options) ->
        Local = #{presets => #{postgres => #{username => <<"session-user">>}}},
        ok = beamlet:with_session(Options#{config => Local}, fun(S) ->
            {ok, C} = beamlet:start_container(S, beamlet_postgres:new(#{recipe => <<"db">>})),
            Client = beamlet_postgres:client_config(C),
            ?assertEqual(<<"session-user">>, maps:get(username, Client)),
            ?assertEqual(<<"recipe-db">>, maps:get(database, Client)),
            [Body] = creates(D),
            ?assert(lists:member(<<"POSTGRES_USER=session-user">>, maps:get(<<"Env">>, Body)))
        end)
    end).

invalid_unused_defaults_fail_before_docker_test() ->
    fixture(#{recipes => #{<<"unused">> => #{kind => generic}}}, fun(D, Options) ->
        ?assertMatch({error, #{stage := config}}, beamlet:start_session(Options)),
        ?assertEqual([], beamlet_test_daemon:events(D))
    end).

invalid_selected_recipe_does_not_start_a_workload_test() ->
    fixture(#{recipes => #{<<"db">> => #{kind => postgres}}}, fun(D, Options) ->
        ok = beamlet:with_session(Options, fun(S) ->
            Before = beamlet_test_daemon:events(D),
            ?assertMatch(
                {error, #{reason := unknown_recipe}},
                beamlet:start_container(S, #{recipe => <<"missing">>})
            ),
            ?assertMatch(
                {error, #{reason := kind_mismatch}},
                beamlet:start_container(S, beamlet_kafka:new(#{recipe => <<"db">>}))
            ),
            ?assertEqual(Before, beamlet_test_daemon:events(D))
        end)
    end).

context_free_validation_does_not_read_application_config_test() ->
    fixture(#{container => #{timeout => 1}}, fun(_, _) ->
        {ok, C} = beamlet_container:validate(beamlet_kafka:new()),
        ?assertEqual(120000, maps:get(timeout, C))
    end).

environment_changes_affect_new_sessions_only_test() ->
    Previous = os:getenv("TESTCONTAINERS_HOST_OVERRIDE"),
    try
        fixture(generic(<<"snapshot:1">>), fun(_, Options) ->
            WithoutHost = maps:remove(host, Options),
            true = os:putenv("TESTCONTAINERS_HOST_OVERRIDE", "old-client"),
            ok = beamlet:with_session(WithoutHost, fun(A) ->
                true = os:putenv("TESTCONTAINERS_HOST_OVERRIDE", "new-client"),
                ok = beamlet:with_session(WithoutHost, fun(B) ->
                    {ok, CA} = beamlet:start_container(A, #{ports => [8000]}),
                    {ok, CB} = beamlet:start_container(B, #{ports => [8000]}),
                    ?assertMatch({ok, {<<"old-client">>, _}}, beamlet:endpoint(CA, 8000)),
                    ?assertMatch({ok, {<<"new-client">>, _}}, beamlet:endpoint(CB, 8000))
                end)
            end)
        end)
    after
        restore_env("TESTCONTAINERS_HOST_OVERRIDE", Previous)
    end.

diagnostic_child_specs_do_not_print_credentials_test() ->
    Secret = <<"private-config-credential">>,
    fixture(#{presets => #{postgres => #{password => Secret}}}, fun(_, Options) ->
        ok = beamlet:with_session(Options, fun(S) ->
            [{Id, Tree}] = [
                {Key, T}
             || {Key, T, _, _} <- supervisor:which_children(beamlet_sup),
                {session, Pid, _, _} <- supervisor:which_children(T),
                Pid =:= S
            ],
            {ok, TreeSpec} = supervisor:get_childspec(beamlet_sup, Id),
            {ok, SessionSpec} = supervisor:get_childspec(Tree, session),
            Printed = iolist_to_binary(io_lib:format("~tp", [{TreeSpec, SessionSpec}])),
            ?assertEqual(nomatch, binary:match(Printed, Secret))
        end)
    end).

session_status_does_not_print_credentials_test() ->
    Secret = <<"private-status-credential">>,
    fixture(#{presets => #{postgres => #{password => Secret}}}, fun(_, Options) ->
        ok = beamlet:with_session(Options, fun(S) ->
            {ok, _} = beamlet:start_container(S, beamlet_postgres:new()),
            Printed = iolist_to_binary(io_lib:format("~tp", [sys:get_status(S)])),
            ?assertEqual(nomatch, binary:match(Printed, Secret))
        end)
    end).

ui_recipe_requires_a_live_target_in_its_session_test() ->
    fixture(#{recipes => #{<<"browser">> => #{kind => kafka_ui}}}, fun(D, Options) ->
        #{port := Port} = uri_string:parse(maps:get(docker_host, Options)),
        ok = gen_server:call(D, {set_ports, #{<<"8080/tcp">> => Port}}),
        ok = beamlet:with_session(Options, fun(S) ->
            {ok, Kafka} = beamlet:start_container(S, beamlet_kafka:new()),
            Before = beamlet_test_daemon:events(D),
            ?assertMatch({error, _}, beamlet:start_container(S, #{recipe => <<"browser">>})),
            ?assertEqual(Before, beamlet_test_daemon:events(D)),
            {ok, UI} = beamlet:start_container(
                S,
                beamlet_kafka_ui:new(Kafka, #{
                    recipe => <<"browser">>
                })
            ),
            ?assert(is_binary(beamlet_kafka_ui:url(UI))),
            ok = beamlet:stop_container(Kafka),
            ?assertEqual({error, container_not_ready}, beamlet:inspect(UI))
        end)
    end).

invalid_explicit_session_options_fail_before_docker_test() ->
    fixture(#{}, fun(D, Options) ->
        lists:foreach(
            fun(Extra) ->
                ?assertMatch({error, _}, beamlet:start_session(maps:merge(Options, Extra)))
            end,
            [
                #{owner => invalid},
                #{config => not_a_map},
                #{host => <<"bad host">>},
                #{unknown => true},
                #{timeout => 0}
            ]
        ),
        ?assertEqual([], beamlet_test_daemon:events(D))
    end).

parallel_starts(Sessions) ->
    Parent = self(),
    Ref = make_ref(),
    lists:foreach(
        fun(S) ->
            spawn(fun() -> Parent ! {Ref, beamlet:start_container(S, #{})} end)
        end,
        Sessions
    ),
    lists:foreach(
        fun(_) ->
            receive
                {Ref, Result} -> ?assertMatch({ok, _}, Result)
            after 2000 -> error(start_timeout)
            end
        end,
        Sessions
    ).

restore_env(Name, false) -> os:unsetenv(Name);
restore_env(Name, Value) -> os:putenv(Name, Value).

generic(Image) -> #{presets => #{generic => #{image => Image}}}.

creates(D) ->
    [
        json:decode(Body)
     || {<<"POST">>, <<"/containers/create?", _/binary>>, Body} <-
            beamlet_test_daemon:events(D)
    ].

fixture(Config, Fun) ->
    {ok, _} = application:ensure_all_started(beamlet),
    Previous = application:get_env(beamlet, config),
    {ok, D} = beamlet_test_daemon:start_link(#{}),
    try
        ok = application:set_env(beamlet, config, Config),
        Fun(D, beamlet_test_daemon:options(D))
    after
        case Previous of
            undefined -> application:unset_env(beamlet, config);
            {ok, Value} -> application:set_env(beamlet, config, Value)
        end,
        beamlet_test_daemon:stop(D)
    end.
