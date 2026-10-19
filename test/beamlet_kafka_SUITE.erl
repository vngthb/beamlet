-module(beamlet_kafka_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([suite/0, all/0, init_per_suite/1, end_per_suite/1, round_trip/1, independent_brokers/1]).

suite() -> [{timetrap, {minutes, 5}}].
all() -> [round_trip, independent_brokers].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok ->
            case application:ensure_all_started(brod) of
                {ok, _} -> Config;
                Error -> ct:fail({kafka_test_client_unavailable, use_integration_profile, Error})
            end;
        Skip ->
            Skip
    end.
end_per_suite(_) -> ok.
round_trip(_) ->
    beamlet:with_session(#{}, fun(S) ->
        {ok, C} = beamlet:start_container(S, beamlet_kafka:new(#{topics => [<<"events">>]})),
        verify(C, beamlet_kafka_roundtrip, <<"hello">>)
    end).
independent_brokers(_) ->
    beamlet:with_session(#{}, fun(S) ->
        Parent = self(),
        Config = beamlet_kafka:new(#{topics => [<<"events">>]}),
        Starts = [
            spawn_monitor(fun() -> Parent ! {self(), beamlet:start_container(S, Config)} end)
         || _ <- [1, 2]
        ],
        Containers = [
            receive
                {Pid, {ok, C}} ->
                    demonitor(Ref, [flush]),
                    C;
                {'DOWN', Ref, process, Pid, Reason} ->
                    ct:fail({start_failed, Reason})
            after 180000 -> ct:fail(start_timeout)
            end
         || {Pid, Ref} <- Starts
        ],
        [A, B] = Containers,
        ?assertNotEqual(beamlet_kafka:bootstrap_servers(A), beamlet_kafka:bootstrap_servers(B)),
        verify(A, beamlet_kafka_a, <<"one">>),
        verify(B, beamlet_kafka_b, <<"two">>)
    end).
verify(C, ClientId, Value) ->
    {ok, {Host, Port}} = beamlet:endpoint(C, 9092),
    Endpoints = [{binary_to_list(Host), Port}],
    {ok, _} = brod:get_metadata(Endpoints, [<<"events">>]),
    {ok, Client} = brod:start_link_client(Endpoints, ClientId, [{auto_start_producers, true}]),
    try
        {ok, Offset} = brod:produce_sync_offset(Client, <<"events">>, 0, <<"key">>, Value),
        {ok, {_, Messages}} = brod:fetch(Endpoints, <<"events">>, 0, Offset),
        ?assertMatch([{kafka_message, _, <<"key">>, Value, _, _, _} | _], Messages)
    after
        brod:stop_client(Client)
    end.
