-module(beamlet_companion_tests).
-include_lib("eunit/include/eunit.hrl").

companion_rejects_wrong_session_kind_and_stopped_target_test() ->
    with_daemon(#{}, fun(D, Options) ->
        {ok, A} = beamlet:start_session(Options),
        {ok, B} = beamlet:start_session(Options),
        try
            {ok, K} = beamlet:start_container(A, beamlet_kafka:new()),
            Before = beamlet_test_daemon:events(D),
            ?assertEqual(
                {error, {invalid_target, wrong_session}},
                beamlet:start_container(B, beamlet_kafka_ui:new(K))
            ),
            ?assertEqual(
                {error, {invalid_target, wrong_kind}},
                beamlet:start_container(A, beamlet_dynamodb_ui:new(K))
            ),
            ?assertEqual(Before, beamlet_test_daemon:events(D)),
            ok = beamlet:stop_container(K),
            Stopped = beamlet_test_daemon:events(D),
            ?assertEqual(
                {error, {invalid_target, not_ready}},
                beamlet:start_container(A, beamlet_kafka_ui:new(K))
            ),
            ?assertEqual(Stopped, beamlet_test_daemon:events(D))
        after
            beamlet:stop_session(A),
            beamlet:stop_session(B)
        end
    end).

companion_configuration_preserves_managed_fields_test() ->
    with_daemon(#{}, fun(_, Options) ->
        beamlet:with_session(Options, fun(S) ->
            {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
            {ok, Config} = beamlet_container:validate(beamlet_kafka_ui:new(K)),
            Env = maps:get(environment, Config),
            ?assertMatch(
                <<"beamlet-", _/binary>>, maps:get(<<"KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS">>, Env)
            ),
            ?assertMatch(
                {error, _}, beamlet_container:validate(beamlet_kafka_ui:new(K, #{ports => [9000]}))
            ),
            ?assertMatch(
                {error, _},
                beamlet_container:validate(beamlet_dynamodb_ui:new(K, #{environment => #{}}))
            ),
            ?assertMatch(
                {error, _}, beamlet_container:validate(beamlet_kafka_ui:new(#{bad => target}))
            )
        end)
    end).

with_daemon(Opts, Fun) ->
    {ok, D} = beamlet_test_daemon:start_link(Opts),
    try
        Fun(D, beamlet_test_daemon:options(D))
    after
        beamlet_test_daemon:stop(D)
    end.

ui_stop_preserves_target_and_target_stop_removes_ui_first_test() ->
    with_ui_daemon(#{}, fun(D, S) ->
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        {ok, UI} = beamlet:start_container(S, beamlet_kafka_ui:new(K)),
        ?assertMatch(<<"http://127.0.0.1:", _/binary>>, beamlet_kafka_ui:url(UI)),
        ok = beamlet:stop_container(UI),
        ?assertMatch({ok, _}, beamlet:inspect(K)),
        {ok, UI2} = beamlet:start_container(S, beamlet_kafka_ui:new(K)),
        ok = beamlet:stop_container(K),
        ?assertMatch({error, _}, beamlet:inspect(UI2)),
        Deletes = [
            P
         || {<<"DELETE">>, <<"/containers/", _/binary>> = P, _} <- beamlet_test_daemon:events(D)
        ],
        ?assertEqual([delete_path(UI), delete_path(UI2), delete_path(K)], Deletes)
    end).

ui_backend_failure_preserves_target_test() ->
    with_ui_daemon(#{backend_ready => false}, fun(_, S) ->
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        ?assertMatch(
            {error, _}, beamlet:start_container(S, beamlet_kafka_ui:new(K, #{timeout => 150}))
        ),
        ?assertMatch({ok, _}, beamlet:inspect(K))
    end).

target_stop_cancels_starting_companion_test() ->
    with_ui_daemon(#{backend_ready => false, notify => self()}, fun(D, S) ->
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        Parent = self(),
        {Pid, Ref} = spawn_monitor(fun() ->
            Parent ! {ui_result, beamlet:start_container(S, beamlet_kafka_ui:new(K))}
        end),
        receive
            {docker_request, <<"GET">>, <<"/api/clusters/beamlet/topics">>} -> ok
        after 1000 -> error(ui_did_not_start)
        end,
        ok = beamlet:stop_container(K),
        receive
            {ui_result, {error, _}} -> ok
        after 1000 -> error(start_not_cancelled)
        end,
        receive
            {'DOWN', Ref, process, Pid, normal} -> ok
        after 1000 -> error(caller_not_done)
        end,
        ?assertEqual(
            delete_path(K), lists:last([P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)])
        ),
        [CancelledUIPath, _] = [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)],
        ok = beamlet:stop_session(S),
        ?assertEqual(
            2,
            length([
                ok
             || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D),
                P =:= CancelledUIPath
            ])
        )
    end).

session_stop_removes_ui_before_target_test() ->
    with_ui_daemon(#{}, fun(D, S) ->
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        {ok, UI} = beamlet:start_container(S, beamlet_kafka_ui:new(K)),
        ok = beamlet:stop_session(S),
        [UIPath, KPath, NetworkPath] = [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)],
        ?assertEqual(delete_path(UI), UIPath),
        ?assertEqual(delete_path(K), KPath),
        ?assertMatch(<<"/networks/", _/binary>>, NetworkPath)
    end).

failed_ui_removal_keeps_target_until_retry_test() ->
    with_ui_daemon(#{}, fun(D, S) ->
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        {ok, UI} = beamlet:start_container(S, beamlet_kafka_ui:new(K)),
        ok = gen_server:call(D, {set_options, #{fail_remove => true}}),
        ?assertMatch({error, #{stage := cleanup}}, beamlet:stop_container(K)),
        ?assertEqual([delete_path(UI)], [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)]),
        ok = gen_server:call(D, {set_options, #{fail_remove => false}}),
        ok = beamlet:stop_container(K),
        ?assertEqual(
            [delete_path(UI), delete_path(UI), delete_path(K)],
            [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)]
        )
    end).

simultaneous_target_and_ui_stop_are_idempotent_test() ->
    with_ui_daemon(#{}, fun(D, S) ->
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        {ok, UI} = beamlet:start_container(S, beamlet_kafka_ui:new(K)),
        Parent = self(),
        Starts = [
            spawn_monitor(fun() -> Parent ! {self(), beamlet:stop_container(C)} end)
         || C <- [K, UI]
        ],
        [
            receive
                {Pid, ok} -> demonitor(Ref, [flush]);
                {'DOWN', Ref, process, Pid, Reason} -> error({stop_failed, Reason})
            after 2000 -> error(stop_timeout)
            end
         || {Pid, Ref} <- Starts
        ],
        ?assertEqual(
            [delete_path(UI), delete_path(K)],
            [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)]
        )
    end).

html_response_does_not_satisfy_backend_readiness_test() ->
    with_ui_daemon(#{backend_body => <<"<html>error page</html>">>}, fun(_, S) ->
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        ?assertMatch(
            {error, _}, beamlet:start_container(S, beamlet_kafka_ui:new(K, #{timeout => 150}))
        ),
        ?assertMatch({ok, _}, beamlet:inspect(K))
    end).

stop_caller_exit_does_not_cancel_dependency_cleanup_test() ->
    with_ui_daemon(#{notify => self(), remove_delay => 100}, fun(D, S) ->
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        {ok, UI} = beamlet:start_container(S, beamlet_kafka_ui:new(K)),
        Caller = spawn(fun() -> beamlet:stop_container(K) end),
        UIPath = delete_path(UI),
        KPath = delete_path(K),
        receive
            {docker_request, <<"DELETE">>, UIPath} -> ok
        after 1000 -> error(ui_not_removed)
        end,
        exit(Caller, kill),
        receive
            {docker_request, <<"DELETE">>, KPath} -> ok
        after 1000 -> error(target_cleanup_cancelled)
        end,
        ok = beamlet:stop_container(K),
        ?assertEqual([UIPath, KPath], [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)])
    end).

owner_exit_removes_companion_target_and_network_test() ->
    with_ui_daemon(#{}, fun(D, _) ->
        Options = beamlet_test_daemon:options(D),
        Parent = self(),
        Owner = spawn(fun() ->
            {ok, S} = beamlet:start_session(Options),
            {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
            {ok, UI} = beamlet:start_container(S, beamlet_kafka_ui:new(K)),
            Parent ! {owned_stack, S, K, UI},
            receive
                stop -> ok
            after 2000 -> error(owner_timeout)
            end
        end),
        receive
            {owned_stack, S, K, UI} ->
                Ref = monitor(process, S),
                Owner ! stop,
                receive
                    {'DOWN', Ref, process, S, normal} -> ok
                after 1000 -> error(owner_cleanup_timeout)
                end,
                [UIPath, KPath, NetworkPath] = [
                    P
                 || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)
                ],
                ?assertEqual(delete_path(UI), UIPath),
                ?assertEqual(delete_path(K), KPath),
                ?assertMatch(<<"/networks/", _/binary>>, NetworkPath)
        after 1000 -> error(stack_start_timeout)
        end
    end).

delete_path(C) -> <<"/containers/", (beamlet:private_host(C))/binary, "?force=true&v=true">>.

with_ui_daemon(Opts, Fun) ->
    with_daemon(Opts, fun(D, Options) ->
        #{port := Port} = uri_string:parse(maps:get(docker_host, Options)),
        ok = gen_server:call(D, {set_ports, #{<<"8080/tcp">> => Port, <<"8001/tcp">> => Port}}),
        beamlet:with_session(Options, fun(S) -> Fun(D, S) end)
    end).
