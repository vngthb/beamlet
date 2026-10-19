-module(beamlet_container_tests).
-include_lib("eunit/include/eunit.hrl").

docker_allocates_host_ports_test() ->
    Config = beamlet_container:new(<<"example:1">>, #{ports => [9092, 8000]}),
    {ok, Valid} = beamlet_container:validate(Config),
    Body = beamlet_container:docker_config(Valid, #{<<"session">> => <<"one">>}),
    Bindings = maps:get(<<"PortBindings">>, maps:get(<<"HostConfig">>, Body)),
    ?assertEqual(
        [#{<<"HostIp">> => <<"127.0.0.1">>, <<"HostPort">> => <<>>}],
        maps:get(<<"9092/tcp">>, Bindings)
    ),
    ?assertEqual(#{<<"session">> => <<"one">>}, maps:get(<<"Labels">>, Body)).

reject_host_ports_test() ->
    lists:foreach(
        fun(Options) ->
            ?assertMatch(
                {error, _},
                beamlet_container:validate(
                    beamlet_container:new(<<"example:1">>, Options)
                )
            )
        end,
        [
            #{host_port => 1234},
            #{ports => [{9092, 19092}]},
            #{host_config => #{port_bindings => #{}}}
        ]
    ).

reject_invalid_configuration_test() ->
    lists:foreach(
        fun(Options) ->
            ?assertMatch(
                {error, _},
                beamlet_container:validate(
                    beamlet_container:new(<<"example:1">>, Options)
                )
            )
        end,
        [
            #{ports => [0]},
            #{timeout => 0},
            #{command => [<<>>]},
            #{pull_policy => sometimes},
            #{environment => #{<<"BAD=KEY">> => <<"value">>}}
        ]
    ).

preset_contracts_test() ->
    {ok, Kafka} = beamlet_container:validate(beamlet_kafka:new()),
    ?assertEqual([9092], maps:get(ports, Kafka)),
    ?assertEqual(kafka, maps:get(kind, Kafka)),
    {ok, Dynamo} = beamlet_container:validate(beamlet_dynamodb:new()),
    ?assertEqual([8000], maps:get(ports, Dynamo)),
    ?assert(lists:member(<<"-inMemory">>, maps:get(command, Dynamo))).

managed_preset_configuration_cannot_be_overridden_test() ->
    ?assertMatch({error, _}, beamlet_container:validate(beamlet_kafka:new(#{ports => [19092]}))),
    ?assertMatch(
        {error, _},
        beamlet_container:validate(
            beamlet_kafka:new(
                #{
                    environment => #{
                        <<"KAFKA_ADVERTISED_LISTENERS">> => <<"EXTERNAL://localhost:19092">>
                    }
                }
            )
        )
    ),
    ?assertMatch(
        {error, _}, beamlet_container:validate(beamlet_dynamodb:new(#{command => [<<"other">>]}))
    ).

postgres_credentials_and_managed_fields_test() ->
    Options = #{username => <<"tester">>, password => <<"secret">>, database => <<"app_test">>},
    {ok, C} = beamlet_container:validate(beamlet_postgres:new(Options)),
    ?assertEqual([5432], maps:get(ports, C)),
    ?assertEqual(<<"tester">>, maps:get(<<"POSTGRES_USER">>, maps:get(environment, C))),
    ?assertMatch({error, _}, beamlet_container:validate(beamlet_postgres:new(#{password => <<>>}))),
    ?assertMatch({error, _}, beamlet_container:validate(beamlet_postgres:new(#{ports => [1234]}))),
    ?assertMatch(
        {error, _}, beamlet_container:validate(beamlet_postgres:new(#{environment => #{}}))
    ).

redis_ephemeral_managed_configuration_test() ->
    {ok, C} = beamlet_container:validate(beamlet_redis:new()),
    ?assertEqual([6379], maps:get(ports, C)),
    ?assert(lists:member(<<"--save">>, maps:get(command, C))),
    ?assertMatch({error, _}, beamlet_container:validate(beamlet_redis:new(#{command => []}))).
