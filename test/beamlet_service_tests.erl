-module(beamlet_service_tests).
-include_lib("eunit/include/eunit.hrl").

kafka_uses_assigned_port_in_uploaded_script_test() ->
    {ok, D} = beamlet_test_daemon:start_link(#{}),
    try
        ok = beamlet:with_session(beamlet_test_daemon:options(D), fun(S) ->
            {ok, C} = beamlet:start_container(S, beamlet_kafka:new(#{topics => [<<"events">>]})),
            Bootstrap = beamlet_kafka:bootstrap_servers(C),
            Uploads = [
                B
             || {<<"PUT">>, P, B} <- beamlet_test_daemon:events(D),
                binary:match(P, <<"/archive">>) =/= nomatch
            ],
            [Tar] = Uploads,
            {ok, [{"beamlet-start.sh", Script}]} = erl_tar:extract({binary, Tar}, [memory]),
            ?assertMatch(<<"#!/bin/bash\n", _/binary>>, Script),
            ?assertNotEqual(nomatch, binary:match(Script, <<"EXTERNAL://", Bootstrap/binary>>))
        end)
    after
        beamlet_test_daemon:stop(D)
    end.

dynamodb_api_probe_test() ->
    {ok, D} = beamlet_test_daemon:start_link(#{}),
    try
        Options = beamlet_test_daemon:options(D),
        Client = #{
            endpoint => maps:get(docker_host, Options),
            region => <<"us-east-1">>,
            access_key_id => <<"beamlet">>,
            secret_access_key => <<"beamlet">>
        },
        ?assertEqual(
            {ok, #{<<"TableNames">> => []}},
            beamlet_dynamodb_api:request(
                Client, <<"ListTables">>, #{}, beamlet_deadline:new(1000)
            )
        )
    after
        beamlet_test_daemon:stop(D)
    end.
