-module(beamlet_dynamodb_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([suite/0, all/0, init_per_suite/1, end_per_suite/1, round_trip/1, isolated_databases/1]).

suite() -> [{timetrap, {minutes, 3}}].
all() -> [round_trip, isolated_databases].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok -> Config;
        Skip -> Skip
    end.
end_per_suite(_) -> ok.
round_trip(_) ->
    beamlet:with_session(#{}, fun(S) ->
        {ok, C} = beamlet:start_container(S, beamlet_dynamodb:new()),
        Client = beamlet_dynamodb:client_config(C),
        Table = <<"beamlet_test">>,
        {ok, _} = api(Client, <<"CreateTable">>, #{
            <<"TableName">> => Table,
            <<"KeySchema">> => [#{<<"AttributeName">> => <<"id">>, <<"KeyType">> => <<"HASH">>}],
            <<"AttributeDefinitions">> => [
                #{<<"AttributeName">> => <<"id">>, <<"AttributeType">> => <<"S">>}
            ],
            <<"BillingMode">> => <<"PAY_PER_REQUEST">>
        }),
        Item = #{<<"id">> => #{<<"S">> => <<"one">>}, <<"value">> => #{<<"S">> => <<"hello">>}},
        {ok, _} = api(Client, <<"PutItem">>, #{<<"TableName">> => Table, <<"Item">> => Item}),
        {ok, Read} = api(Client, <<"GetItem">>, #{
            <<"TableName">> => Table,
            <<"Key">> => #{<<"id">> => #{<<"S">> => <<"one">>}},
            <<"ConsistentRead">> => true
        }),
        ?assertEqual(Item, maps:get(<<"Item">>, Read)),
        {ok, _} = api(Client, <<"DeleteTable">>, #{<<"TableName">> => Table}),
        ok
    end).
isolated_databases(_) ->
    beamlet:with_session(#{}, fun(S) ->
        {ok, A} = beamlet:start_container(S, beamlet_dynamodb:new()),
        {ok, B} = beamlet:start_container(S, beamlet_dynamodb:new()),
        CA = beamlet_dynamodb:client_config(A),
        CB = beamlet_dynamodb:client_config(B),
        ?assertNotEqual(maps:get(endpoint, CA), maps:get(endpoint, CB)),
        {ok, _} = api(CA, <<"CreateTable">>, #{
            <<"TableName">> => <<"isolated">>,
            <<"KeySchema">> => [#{<<"AttributeName">> => <<"id">>, <<"KeyType">> => <<"HASH">>}],
            <<"AttributeDefinitions">> => [
                #{<<"AttributeName">> => <<"id">>, <<"AttributeType">> => <<"S">>}
            ],
            <<"BillingMode">> => <<"PAY_PER_REQUEST">>
        }),
        ?assertEqual({ok, #{<<"TableNames">> => []}}, api(CB, <<"ListTables">>, #{})),
        ok
    end).
api(Client, Op, Payload) ->
    beamlet_dynamodb_api:request(Client, Op, Payload, beamlet_deadline:new(10000)).
