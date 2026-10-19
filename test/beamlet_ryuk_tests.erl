-module(beamlet_ryuk_tests).
-include_lib("eunit/include/eunit.hrl").

session_holds_reaper_connection_test() ->
    reaper_fixture(fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        receive
            {registered, Filter} -> ?assertMatch(<<"label=org.beamlet.session=", _/binary>>, Filter)
        after 1000 -> error(no_registration)
        end,
        {ok, _} = beamlet:start_container(S, beamlet_container:new(<<"example:1">>)),
        ok = beamlet:stop_session(S),
        receive
            reaper_disconnected -> ok
        after 1000 -> error(socket_leaked)
        end,
        ?assertEqual(
            1,
            length([
                ok
             || {<<"DELETE">>, <<"/containers/", _/binary>>, _} <- beamlet_test_daemon:events(D)
            ])
        )
    end).

cleanup_failure_leaves_reaper_to_retry_test() ->
    reaper_fixture(#{fail_remove => true}, fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        receive
            {registered, _} -> ok
        after 1000 -> error(no_registration)
        end,
        {ok, _} = beamlet:start_container(S, beamlet_container:new(<<"example:1">>)),
        ?assertMatch({error, #{stage := cleanup}}, beamlet:stop_session(S)),
        receive
            reaper_disconnected -> ok
        after 1000 -> error(socket_leaked)
        end,
        Deletes = [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)],
        ?assertEqual([], [P || P <- Deletes, binary:match(P, <<"beamlet-ryuk-">>) =/= nomatch])
    end).

session_kill_closes_reaper_connection_test() ->
    reaper_fixture(fun(_, Options) ->
        {ok, S} = beamlet:start_session(Options),
        receive
            {registered, _} -> ok
        after 1000 -> error(no_registration)
        end,
        Ref = monitor(process, S),
        exit(S, kill),
        receive
            {'DOWN', Ref, process, S, killed} -> ok
        after 1000 -> error(not_killed)
        end,
        receive
            reaper_disconnected -> ok
        after 1000 -> error(socket_leaked)
        end
    end).

rejected_registration_closes_socket_and_preserves_error_test() ->
    reaper_fixture(#{reaper_ack => <<"NACK\n">>, fail_remove => true}, fun(D, Options) ->
        ?assertEqual(
            {error, #{stage => ryuk_register, reason => {ok, <<"NACK\n">>}}},
            beamlet:start_session(Options)
        ),
        receive
            reaper_disconnected -> ok
        after 1000 -> error(socket_leaked)
        end,
        Deletes = [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)],
        ?assertMatch([<<"/containers/beamlet-ryuk-", _/binary>>], Deletes)
    end).

registration_waits_for_reaper_readiness_test() ->
    reaper_fixture(#{reject_early_registration => true}, fun(_, Options) ->
        {ok, S} = beamlet:start_session(Options),
        try
            receive
                {registered, _} -> ok
            after 1000 -> error(no_registration)
            end
        after
            ok = beamlet:stop_session(S)
        end,
        receive
            reaper_disconnected -> ok
        after 1000 -> error(socket_leaked)
        end
    end).

reaper_readiness_timeout_removes_reaper_test() ->
    reaper_fixture(#{logs => <<"starting">>}, fun(D, Options) ->
        {ok, Conn} = beamlet_docker:connect(Options, beamlet_deadline:new(1000)),
        Name = <<"beamlet-ryuk-not-ready">>,
        ?assertMatch(
            {error, #{stage := ryuk_ready}},
            beamlet_ryuk:start(
                Conn, Name, <<"not-ready">>, Options, self(), beamlet_deadline:new(200)
            )
        ),
        Events = beamlet_test_daemon:events(D),
        ?assertEqual(
            [<<"/containers/", Name/binary, "?force=true&v=true">>],
            [P || {<<"DELETE">>, P, _} <- Events]
        )
    end).

exited_reaper_reports_readiness_failure_test() ->
    reaper_fixture(#{exit_on_start => true}, fun(D, Options) ->
        ?assertMatch(
            {error, #{stage := ryuk_ready, reason := {container_exited, _}}},
            beamlet:start_session(Options)
        ),
        ?assertMatch(
            [_],
            [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)]
        ),
        ?assertEqual(
            [],
            [P || {<<"POST">>, <<"/networks/create">> = P, _} <- beamlet_test_daemon:events(D)]
        )
    end).

reaper_fixture(Fun) ->
    reaper_fixture(#{}, Fun).
reaper_fixture(Options, Fun) ->
    {ok, L} = gen_tcp:listen(0, [binary, {active, false}, {packet, line}, {ip, {127, 0, 0, 1}}]),
    {ok, {_, Port}} = inet:sockname(L),
    DaemonOptions = maps:merge(#{logs => <<"Started">>}, Options),
    {ok, D} = beamlet_test_daemon:start_link(DaemonOptions#{port_map => #{<<"8080/tcp">> => Port}}),
    Parent = self(),
    Server = spawn_link(fun() ->
        {ok, S} = gen_tcp:accept(L),
        {ok, Filter} = gen_tcp:recv(S, 0, 2000),
        case maps:get(reject_early_registration, Options, false) andalso not logs_read(D) of
            true ->
                gen_tcp:close(S),
                Parent ! reaper_disconnected;
            false ->
                ok = gen_tcp:send(S, maps:get(reaper_ack, Options, <<"ACK\n">>)),
                Parent ! {registered, Filter},
                {error, closed} = gen_tcp:recv(S, 0, 2000),
                Parent ! reaper_disconnected
        end
    end),
    try
        Fun(D, (beamlet_test_daemon:options(D))#{ryuk => true})
    after
        unlink(Server),
        exit(Server, kill),
        gen_tcp:close(L),
        beamlet_test_daemon:stop(D)
    end.

logs_read(Daemon) ->
    lists:any(
        fun
            ({<<"GET">>, Path, _}) -> binary:match(Path, <<"/logs?">>) =/= nomatch;
            (_) -> false
        end,
        beamlet_test_daemon:events(Daemon)
    ).

reaper_and_workload_use_session_bind_address_test_() ->
    [
        {binary_to_list(IP), fun() -> check_bind_address(IP) end}
     || IP <- [<<"127.0.0.1">>, <<"0.0.0.0">>]
    ].

check_bind_address(IP) ->
    reaper_fixture(fun(D, Options) ->
        Extra =
            case IP of
                <<"127.0.0.1">> -> #{};
                _ -> #{bind_ip => IP}
            end,
        ok = beamlet:with_session(maps:merge(Options, Extra), fun(S) ->
            {ok, _} = beamlet:start_container(
                S, beamlet_container:new(<<"example:1">>, #{ports => [8000]})
            ),
            Bodies = [
                json:decode(B)
             || {<<"POST">>, <<"/containers/create?name=", _/binary>>, B} <- beamlet_test_daemon:events(
                    D
                )
            ],
            ?assertEqual(2, length(Bodies)),
            lists:foreach(
                fun(Body) ->
                    Bindings = maps:get(<<"PortBindings">>, maps:get(<<"HostConfig">>, Body)),
                    ?assertEqual(
                        [[#{<<"HostIp">> => IP, <<"HostPort">> => <<>>}]], maps:values(Bindings)
                    )
                end,
                Bodies
            )
        end)
    end).
