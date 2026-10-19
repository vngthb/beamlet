-module(beamlet_dynamodb_ui_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([suite/0, all/0, init_per_suite/1, end_per_suite/1, backend_item_and_isolation/1]).

suite() -> [{timetrap, {minutes, 3}}].
all() -> [backend_item_and_isolation].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok -> Config;
        Skip -> Skip
    end.
end_per_suite(_) -> ok.

backend_item_and_isolation(_) ->
    beamlet:with_session(#{}, fun(A) ->
        beamlet:with_session(#{}, fun(B) ->
            {ok, DA} = beamlet:start_container(A, beamlet_dynamodb:new()),
            {ok, DB} = beamlet:start_container(B, beamlet_dynamodb:new()),
            seed(DA),
            {ok, UIA} = beamlet:start_container(A, beamlet_dynamodb_ui:new(DA)),
            {ok, UIB} = beamlet:start_container(B, beamlet_dynamodb_ui:new(DB)),
            ?assertNotEqual(beamlet_dynamodb_ui:url(UIA), beamlet_dynamodb_ui:url(UIB)),
            ?assertMatch(
                {error, _},
                beamlet:start_container(
                    A,
                    beamlet_dynamodb_ui:new(DA, #{image => <<"alpine:3.21.3">>, timeout => 1000})
                )
            ),
            ?assertMatch({ok, _}, beamlet:inspect(DA)),
            {ok, 200, Page} = beamlet_test_support:http_get(UIA, 8001, <<"/">>),
            ?assertNotEqual(nomatch, binary:match(Page, <<"ui_items">>)),
            {ok, 200, Tables} = beamlet_test_support:http_get(UIA, 8001, <<"/api/tables">>),
            ?assertNotEqual(nomatch, binary:match(Tables, <<"ui_items">>)),
            {ok, 200, Empty} = beamlet_test_support:http_get(UIB, 8001, <<"/api/tables">>),
            ?assertEqual([], json:decode(Empty)),
            {ok, 200, Items} = beamlet_test_support:http_get(
                UIA, 8001, <<"/tables/ui_items/items">>
            ),
            ?assertNotEqual(nomatch, binary:match(Items, <<"hello-from-host">>)),
            ok = beamlet:stop_container(DA),
            ?assertMatch({error, _}, beamlet:inspect(UIA)),
            ?assertMatch({ok, _}, beamlet:inspect(DB))
        end)
    end).

seed(Container) ->
    Client = beamlet_dynamodb:client_config(Container),
    {ok, _} = api(Client, <<"CreateTable">>, #{
        <<"TableName">> => <<"ui_items">>,
        <<"KeySchema">> => [#{<<"AttributeName">> => <<"id">>, <<"KeyType">> => <<"HASH">>}],
        <<"AttributeDefinitions">> => [
            #{<<"AttributeName">> => <<"id">>, <<"AttributeType">> => <<"S">>}
        ],
        <<"BillingMode">> => <<"PAY_PER_REQUEST">>
    }),
    {ok, _} = api(Client, <<"PutItem">>, #{
        <<"TableName">> => <<"ui_items">>,
        <<"Item">> => #{
            <<"id">> => #{<<"S">> => <<"one">>},
            <<"value">> => #{<<"S">> => <<"hello-from-host">>}
        }
    }),
    ok.
api(Client, Operation, Payload) ->
    beamlet_dynamodb_api:request(Client, Operation, Payload, beamlet_deadline:new(10000)).
