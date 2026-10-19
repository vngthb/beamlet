-module(beamlet_kafka_ui_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([suite/0, all/0, init_per_suite/1, end_per_suite/1, backend_message_and_isolation/1]).

suite() -> [{timetrap, {minutes, 5}}].
all() -> [backend_message_and_isolation].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok ->
            {ok, _} = application:ensure_all_started(brod),
            Config;
        Skip ->
            Skip
    end.
end_per_suite(_) -> ok.

backend_message_and_isolation(_) ->
    beamlet:with_session(#{}, fun(A) ->
        beamlet:with_session(#{}, fun(B) ->
            {ok, KA} = beamlet:start_container(A, beamlet_kafka:new(#{topics => [<<"events-a">>]})),
            {ok, KB} = beamlet:start_container(B, beamlet_kafka:new(#{topics => [<<"events-b">>]})),
            seed(KA),
            {ok, UIA} = beamlet:start_container(A, beamlet_kafka_ui:new(KA)),
            {ok, UIB} = beamlet:start_container(B, beamlet_kafka_ui:new(KB)),
            ?assertNotEqual(beamlet_kafka_ui:url(UIA), beamlet_kafka_ui:url(UIB)),
            ?assertMatch(
                {error, _},
                beamlet:start_container(
                    A,
                    beamlet_kafka_ui:new(KA, #{image => <<"alpine:3.21.3">>, timeout => 1000})
                )
            ),
            ?assertMatch({ok, _}, beamlet:inspect(KA)),
            {ok, 200, _} = beamlet_test_support:http_get(UIA, 8080, <<"/">>),
            ?assert(lists:member(<<"events-a">>, topics(UIA))),
            ?assertNot(lists:member(<<"events-b">>, topics(UIA))),
            ?assert(lists:member(<<"events-b">>, topics(UIB))),
            ?assertNot(lists:member(<<"events-a">>, topics(UIB))),
            {ok, 200, Messages} = beamlet_test_support:http_get(
                UIA,
                8080,
                <<"/api/clusters/beamlet/topics/events-a/messages/v2?mode=EARLIEST&limit=1">>
            ),
            ?assertNotEqual(nomatch, binary:match(Messages, <<"hello-from-host">>)),
            ok = beamlet:stop_container(UIA),
            ?assertMatch({ok, _}, beamlet:inspect(KA)),
            {ok, UIA2} = beamlet:start_container(A, beamlet_kafka_ui:new(KA)),
            ok = beamlet:stop_container(KA),
            ?assertMatch({error, _}, beamlet:inspect(UIA2)),
            ?assert(lists:member(<<"events-b">>, topics(UIB)))
        end)
    end).

topics(UI) ->
    {ok, 200, Body} = beamlet_test_support:http_get(UI, 8080, <<"/api/clusters/beamlet/topics">>),
    #{<<"topics">> := Topics} = json:decode(Body),
    [maps:get(<<"name">>, T) || T <- Topics].

seed(Kafka) ->
    {ok, {Host, Port}} = beamlet:endpoint(Kafka, 9092),
    {ok, Client} = brod:start_link_client(
        [{binary_to_list(Host), Port}],
        beamlet_kafka_ui_test,
        [{auto_start_producers, true}]
    ),
    try
        ok = brod:produce_sync(Client, <<"events-a">>, 0, <<"key">>, <<"hello-from-host">>)
    after
        brod:stop_client(Client)
    end.
