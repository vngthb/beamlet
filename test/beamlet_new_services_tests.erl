-module(beamlet_new_services_tests).
-include_lib("eunit/include/eunit.hrl").

postgres_queries_final_tcp_server_test() ->
    with_daemon(#{}, fun(D, S) ->
        Options = #{username => <<"tester">>, database => <<"app_test">>, password => <<"secret">>},
        {ok, C} = beamlet:start_container(S, beamlet_postgres:new(Options)),
        ?assertMatch(
            #{
                host := <<"127.0.0.1">>,
                port := _,
                username := <<"tester">>,
                database := <<"app_test">>,
                password := <<"secret">>
            },
            beamlet_postgres:client_config(C)
        ),
        Commands = [
            maps:get(<<"Cmd">>, json:decode(B))
         || {<<"POST">>, P, B} <- beamlet_test_daemon:events(D),
            binary:match(P, <<"/exec">>) =/= nomatch,
            binary:match(P, <<"/containers/">>) =/= nomatch
        ],
        ?assert(
            lists:any(
                fun(Args) ->
                    Text = iolist_to_binary(Args),
                    binary:match(Text, <<"--host 127.0.0.1">>) =/= nomatch andalso
                        binary:match(Text, <<"SELECT 1">>) =/= nomatch
                end,
                Commands
            )
        )
    end).

redis_waits_for_fragmented_pong_test() ->
    with_redis(
        fun(Socket) ->
            {ok, <<"*1\r\n$4\r\nPING\r\n">>} = gen_tcp:recv(Socket, 14, 1000),
            [ok = gen_tcp:send(Socket, <<B>>) || <<B>> <= <<"+PONG\r\n">>]
        end,
        fun(Port) ->
            with_daemon(#{port_map => #{<<"6379/tcp">> => Port}}, fun(_, S) ->
                {ok, C} = beamlet:start_container(S, beamlet_redis:new(#{timeout => 1000})),
                ?assertEqual(
                    #{host => <<"127.0.0.1">>, port => Port}, beamlet_redis:client_config(C)
                )
            end)
        end
    ).

redis_wrong_response_fails_and_cleans_up_test() ->
    with_redis(
        fun(Socket) ->
            {ok, _} = gen_tcp:recv(Socket, 0, 1000),
            ok = gen_tcp:send(Socket, <<"-ERR not ready\r\n">>)
        end,
        fun(Port) ->
            with_daemon(#{port_map => #{<<"6379/tcp">> => Port}}, fun(D, S) ->
                ?assertMatch(
                    {error, _}, beamlet:start_container(S, beamlet_redis:new(#{timeout => 150}))
                ),
                ?assert(
                    lists:any(
                        fun({M, _, _}) -> M =:= <<"DELETE">> end, beamlet_test_daemon:events(D)
                    )
                )
            end)
        end
    ).

with_daemon(Options, Fun) ->
    {ok, D} = beamlet_test_daemon:start_link(Options),
    try
        beamlet:with_session(beamlet_test_daemon:options(D), fun(S) -> Fun(D, S) end)
    after
        beamlet_test_daemon:stop(D)
    end.

with_redis(Serve, Fun) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
    {ok, {_, Port}} = inet:sockname(Listen),
    {Pid, Ref} = spawn_monitor(fun() ->
        {ok, Socket} = gen_tcp:accept(Listen, 2000),
        try
            Serve(Socket)
        after
            gen_tcp:close(Socket)
        end
    end),
    try
        Fun(Port),
        receive
            {'DOWN', Ref, process, Pid, normal} -> ok;
            {'DOWN', Ref, process, Pid, Reason} -> error({server_failed, Reason})
        after 2000 -> error(server_timeout)
        end
    after
        gen_tcp:close(Listen),
        exit(Pid, kill),
        demonitor(Ref, [flush])
    end.
