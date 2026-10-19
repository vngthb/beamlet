-module(beamlet_redis_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    round_trip_and_isolation/1,
    failed_start_cleanup/1
]).

suite() -> [{timetrap, {minutes, 3}}].
all() -> [round_trip_and_isolation, failed_start_cleanup].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok ->
            {ok, _} = application:ensure_all_started(eredis),
            Config;
        Skip ->
            Skip
    end.
end_per_suite(_) -> ok.

round_trip_and_isolation(_) ->
    beamlet:with_session(#{}, fun(S) ->
        {ok, A} = beamlet:start_container(S, beamlet_redis:new()),
        {ok, B} = beamlet:start_container(S, beamlet_redis:new()),
        ?assertNotEqual(beamlet_redis:client_config(A), beamlet_redis:client_config(B)),
        {ok, ClientA} = connect(A),
        {ok, ClientB} = connect(B),
        try
            {ok, <<"PONG">>} = eredis:q(ClientA, [<<"PING">>]),
            {ok, <<"OK">>} = eredis:q(ClientA, [<<"SET">>, <<"key">>, <<"hello">>]),
            {ok, <<"hello">>} = eredis:q(ClientA, [<<"GET">>, <<"key">>]),
            {ok, undefined} = eredis:q(ClientB, [<<"GET">>, <<"key">>])
        after
            eredis:stop(ClientA),
            eredis:stop(ClientB)
        end
    end).
failed_start_cleanup(_) ->
    beamlet:with_session(#{}, fun(S) ->
        ?assertMatch(
            {error, _},
            beamlet:start_container(
                S,
                beamlet_redis:new(#{image => <<"alpine:3.21.3">>, timeout => 1000})
            )
        )
    end).
connect(C) ->
    #{host := Host, port := Port} = beamlet_redis:client_config(C),
    eredis:start_link(binary_to_list(Host), Port).
