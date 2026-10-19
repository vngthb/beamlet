-module(beamlet_session_tests).
-include_lib("eunit/include/eunit.hrl").

session_lifecycle_test() ->
    with_daemon(#{}, fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        Config = beamlet_container:new(<<"example:1">>, #{ports => [8000]}),
        {ok, A} = beamlet:start_container(S, Config),
        {ok, B} = beamlet:start_container(S, Config),
        {ok, {<<"127.0.0.1">>, PA}} = beamlet:endpoint(A, 8000),
        {ok, {_, PB}} = beamlet:endpoint(B, 8000),
        ?assertNotEqual(PA, PB),
        ?assertEqual({error, port_not_exposed}, beamlet:endpoint(A, 1234)),
        ?assertEqual(ok, beamlet:stop_container(A)),
        ?assertEqual(ok, beamlet:stop_container(A)),
        ?assertEqual(ok, beamlet:stop_session(S)),
        Deletes = [
            P
         || {<<"DELETE">>, <<"/containers/", _/binary>> = P, _} <- beamlet_test_daemon:events(D)
        ],
        ?assertEqual(2, length(lists:usort(Deletes)))
    end).

failed_start_is_removed_test() ->
    with_daemon(#{fail_start => true}, fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        ?assertMatch(
            {error, #{stage := start, reason := {http, 500, _}}},
            beamlet:start_container(S, beamlet_container:new(<<"bad:1">>))
        ),
        ?assert(lists:any(fun({M, _, _}) -> M =:= <<"DELETE">> end, beamlet_test_daemon:events(D))),
        ok = beamlet:stop_session(S)
    end).

container_exiting_before_inspection_is_not_ready_test() ->
    with_daemon(#{exit_on_start => true}, fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        ?assertMatch(
            {error, #{stage := inspect, reason := {container_exited, _}}},
            beamlet:start_container(S, beamlet_container:new(<<"example:1">>))
        ),
        ?assert(lists:any(fun({M, _, _}) -> M =:= <<"DELETE">> end, beamlet_test_daemon:events(D))),
        ok = beamlet:stop_session(S)
    end).

owner_exit_cleans_session_test() ->
    with_daemon(#{}, fun(D, Options) ->
        Parent = self(),
        Owner = spawn(fun() ->
            {ok, S} = beamlet:start_session(Options),
            {ok, _} = beamlet:start_container(S, beamlet_container:new(<<"example:1">>)),
            Parent ! {session, S},
            receive
                finish -> ok
            after 2000 -> error(owner_wait_timeout)
            end
        end),
        receive
            {session, S} ->
                Ref = monitor(process, S),
                Owner ! finish,
                receive
                    {'DOWN', Ref, process, S, _} -> ok
                after 2000 -> error(owner_cleanup_timeout)
                end
        after 2000 -> error(start_timeout)
        end,
        ?assert(lists:any(fun({M, _, _}) -> M =:= <<"DELETE">> end, beamlet_test_daemon:events(D)))
    end).

invalid_config_does_not_contact_docker_test() ->
    with_daemon(#{}, fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        Before = beamlet_test_daemon:events(D),
        ?assertMatch(
            {error, _}, beamlet:start_container(S, #{image => <<"x:1">>, host_port => 19092})
        ),
        ?assertEqual(Before, beamlet_test_daemon:events(D)),
        ok = beamlet:stop_session(S)
    end).

caller_exit_cancels_and_cleans_start_test() ->
    with_daemon(#{start_delay => 1000, notify => self()}, fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        Caller = spawn(fun() ->
            beamlet:start_container(S, beamlet_container:new(<<"example:1">>))
        end),
        receive
            {docker_request, <<"POST">>, Path} ->
                case binary:match(Path, <<"/start">>) of
                    nomatch -> wait_for_start();
                    _ -> ok
                end
        after 1000 -> error(no_create)
        end,
        exit(Caller, kill),
        receive
            {docker_request, <<"DELETE">>, _} -> ok
        after 1000 -> error(no_cleanup)
        end,
        ?assert(lists:any(fun({M, _, _}) -> M =:= <<"DELETE">> end, beamlet_test_daemon:events(D))),
        ok = beamlet:stop_session(S)
    end).

startup_deadline_cleans_resource_test() ->
    with_daemon(#{start_delay => 1000}, fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        ?assertMatch(
            {error, _},
            beamlet:start_container(
                S,
                beamlet_container:new(<<"example:1">>, #{timeout => 30})
            )
        ),
        ?assert(lists:any(fun({M, _, _}) -> M =:= <<"DELETE">> end, beamlet_test_daemon:events(D))),
        ok = beamlet:stop_session(S)
    end).

with_session_reports_cleanup_failure_test() ->
    with_daemon(#{fail_remove => true}, fun(_, Options) ->
        Result = beamlet:with_session(Options, fun(S) ->
            {ok, _} = beamlet:start_container(S, beamlet_container:new(<<"example:1">>)),
            completed
        end),
        ?assertMatch({error, #{stage := cleanup}}, Result)
    end).

with_session_preserves_test_failure_and_cleans_up_test() ->
    with_daemon(#{}, fun(D, Options) ->
        ?assertError(
            test_failed,
            beamlet:with_session(Options, fun(S) ->
                {ok, _} = beamlet:start_container(S, beamlet_container:new(<<"example:1">>)),
                error(test_failed)
            end)
        ),
        ?assert(lists:any(fun({M, _, _}) -> M =:= <<"DELETE">> end, beamlet_test_daemon:events(D)))
    end).

unknown_request_does_not_crash_session_test() ->
    with_daemon(#{}, fun(_, Options) ->
        {ok, S} = beamlet:start_session(Options),
        ?assertEqual({error, unknown_request}, gen_server:call(S, unexpected)),
        ?assertEqual(ok, beamlet:stop_session(S))
    end).

failed_start_keeps_original_error_when_cleanup_fails_test() ->
    with_daemon(#{fail_start => true, fail_remove => true}, fun(_, Options) ->
        {ok, S} = beamlet:start_session(Options),
        try
            ?assertMatch(
                {error, #{
                    failure :=
                        {error, #{
                            stage := start,
                            reason := {http, 500, _}
                        }},
                    cleanup := {error, {http, 500, _}}
                }},
                beamlet:start_container(S, beamlet_container:new(<<"bad:1">>))
            )
        after
            beamlet:stop_session(S)
        end
    end).

wait_for_start() ->
    receive
        {docker_request, <<"POST">>, Path} ->
            case binary:match(Path, <<"/start">>) of
                nomatch -> wait_for_start();
                _ -> ok
            end
    after 1000 -> error(no_start)
    end.

with_daemon(Opts, Fun) ->
    {ok, D} = beamlet_test_daemon:start_link(Opts),
    try
        Fun(D, beamlet_test_daemon:options(D))
    after
        beamlet_test_daemon:stop(D)
    end.
