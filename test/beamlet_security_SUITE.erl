-module(beamlet_security_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    default_bindings_are_loopback/1,
    explicit_bind_address/1
]).

suite() -> [{timetrap, {minutes, 3}}].
all() -> [default_bindings_are_loopback, explicit_bind_address].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok -> Config;
        Skip -> Skip
    end.
end_per_suite(_) -> ok.

default_bindings_are_loopback(_) ->
    check_session(#{}, <<"127.0.0.1">>).

explicit_bind_address(_) ->
    %% Exercise a non-default binding without exposing this test to the LAN.
    IP = <<"127.0.0.2">>,
    check_session(#{bind_ip => IP, host => IP}, IP).

check_session(Options, IP) ->
    beamlet:with_session(Options, fun(S) ->
        {ok, C} = beamlet:start_container(S, beamlet_redis:new()),
        {ok, Info} = beamlet:inspect(C),
        assert_bindings(Info, IP),
        Token = maps:get(
            <<"org.beamlet.session">>, maps:get(<<"Labels">>, maps:get(<<"Config">>, Info))
        ),
        Reaper = <<"beamlet-ryuk-", Token/binary>>,
        D = beamlet_deadline:new(5000),
        {ok, Conn} = beamlet_docker:connect(#{}, D),
        {ok, ReaperInfo} = beamlet_docker:inspect(Conn, Reaper, D),
        assert_bindings(ReaperInfo, IP),
        {ok, 0, Version} = beamlet:exec(C, [<<"redis-cli">>, <<"--raw">>, <<"INFO">>, <<"server">>]),
        ?assertNotEqual(nomatch, binary:match(Version, <<"redis_version:7.4.11\r\n">>)),
        {ok, {Host, Port}} = beamlet:endpoint(C, 6379),
        {ok, Socket} = gen_tcp:connect(binary_to_list(Host), Port, [binary, {active, false}], 5000),
        try
            ok = gen_tcp:send(Socket, <<"*1\r\n$4\r\nPING\r\n">>),
            ?assertEqual({ok, <<"+PONG\r\n">>}, gen_tcp:recv(Socket, 0, 5000))
        after
            gen_tcp:close(Socket)
        end
    end).

assert_bindings(Info, IP) ->
    Ports = maps:get(<<"Ports">>, maps:get(<<"NetworkSettings">>, Info)),
    ?assert(map_size(Ports) > 0),
    lists:foreach(
        fun(Bindings) ->
            ?assertMatch([#{<<"HostIp">> := IP, <<"HostPort">> := _}], Bindings)
        end,
        maps:values(Ports)
    ).
