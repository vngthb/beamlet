-module(beamlet_config_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    configured_services/1,
    configured_companions/1
]).

suite() -> [{timetrap, {minutes, 5}}].
all() -> [configured_services, configured_companions].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok ->
            {ok, _} = application:ensure_all_started(epgsql),
            {ok, _} = application:ensure_all_started(eredis),
            Config;
        Skip ->
            Skip
    end.
end_per_suite(_) -> ok.

configured_services(_) ->
    with_config(
        #{
            container => #{labels => #{<<"configured">> => <<"yes">>}},
            presets => #{
                postgres => #{username => <<"configured_user">>, password => <<"shared">>}
            },
            recipes => #{
                <<"db">> => #{kind => postgres, database => <<"recipe_db">>},
                <<"generic_cache">> => #{
                    kind => generic,
                    image => <<"redis:7.4.11-alpine">>,
                    ports => [6379],
                    command => [
                        <<"redis-server">>, <<"--save">>, <<>>, <<"--appendonly">>, <<"no">>
                    ],
                    wait => [{command, [<<"redis-cli">>, <<"PING">>]}]
                }
            }
        },
        fun(S) ->
            {ok, DB} = beamlet:start_container(
                S,
                beamlet_postgres:new(#{
                    recipe => <<"db">>, password => <<"beamlet">>
                })
            ),
            postgres_round_trip(beamlet_postgres:client_config(DB)),
            {ok, #{<<"Config">> := #{<<"Labels">> := Labels}}} = beamlet:inspect(DB),
            ?assertEqual(<<"yes">>, maps:get(<<"configured">>, Labels)),
            {ok, Cache} = beamlet:start_container(S, #{recipe => <<"generic_cache">>}),
            {ok, {Host, Port}} = beamlet:endpoint(Cache, 6379),
            {ok, Client} = eredis:start_link(binary_to_list(Host), Port),
            try
                {ok, <<"OK">>} = eredis:q(Client, [<<"SET">>, <<"key">>, <<"configured">>]),
                {ok, <<"configured">>} = eredis:q(Client, [<<"GET">>, <<"key">>]),
                ok
            after
                eredis:stop(Client)
            end
        end
    ).

configured_companions(_) ->
    with_config(
        #{
            recipes => #{
                <<"broker">> => #{kind => kafka, topics => [<<"recipe_events">>]},
                <<"kafka_browser">> => #{kind => kafka_ui},
                <<"dynamo">> => #{kind => dynamodb},
                <<"dynamo_browser">> => #{kind => dynamodb_ui}
            }
        },
        fun(S) ->
            {ok, Kafka} = beamlet:start_container(S, #{recipe => <<"broker">>}),
            {ok, KafkaUI} = beamlet:start_container(
                S,
                beamlet_kafka_ui:new(Kafka, #{
                    recipe => <<"kafka_browser">>
                })
            ),
            {ok, 200, Topics} = beamlet_test_support:http_get(
                KafkaUI, 8080, <<"/api/clusters/beamlet/topics">>
            ),
            ?assertNotEqual(nomatch, binary:match(Topics, <<"recipe_events">>)),
            {ok, Dynamo} = beamlet:start_container(S, #{recipe => <<"dynamo">>}),
            seed_table(Dynamo),
            {ok, DynamoUI} = beamlet:start_container(
                S,
                beamlet_dynamodb_ui:new(Dynamo, #{
                    recipe => <<"dynamo_browser">>
                })
            ),
            {ok, 200, Tables} = beamlet_test_support:http_get(DynamoUI, 8001, <<"/api/tables">>),
            ?assertNotEqual(nomatch, binary:match(Tables, <<"recipe_items">>)),
            ok = beamlet:stop_container(Dynamo),
            ?assertMatch({error, _}, beamlet:inspect(DynamoUI)),
            ok
        end
    ).

postgres_round_trip(#{
    host := Host, port := Port, username := User, password := Password, database := Database
}) ->
    ?assertEqual(<<"beamlet">>, Password),
    {ok, Client} = epgsql:connect(
        binary_to_list(Host),
        binary_to_list(User),
        binary_to_list(Password),
        [{port, Port}, {database, binary_to_list(Database)}, {timeout, 5000}]
    ),
    try
        {ok, _, [{<<"configured_user">>, <<"recipe_db">>}]} = epgsql:squery(
            Client, "SELECT current_user, current_database()"
        ),
        ok
    after
        epgsql:close(Client)
    end.

seed_table(Dynamo) ->
    {ok, _} = beamlet_dynamodb_api:request(
        beamlet_dynamodb:client_config(Dynamo),
        <<"CreateTable">>,
        #{
            <<"TableName">> => <<"recipe_items">>,
            <<"KeySchema">> => [#{<<"AttributeName">> => <<"id">>, <<"KeyType">> => <<"HASH">>}],
            <<"AttributeDefinitions">> => [
                #{<<"AttributeName">> => <<"id">>, <<"AttributeType">> => <<"S">>}
            ],
            <<"BillingMode">> => <<"PAY_PER_REQUEST">>
        },
        beamlet_deadline:new(10000)
    ),
    ok.

with_config(Config, Fun) ->
    Previous = application:get_env(beamlet, config),
    try
        ok = application:set_env(beamlet, config, Config),
        ok = beamlet:with_session(#{}, Fun)
    after
        case Previous of
            undefined -> application:unset_env(beamlet, config);
            {ok, Value} -> application:set_env(beamlet, config, Value)
        end
    end.
