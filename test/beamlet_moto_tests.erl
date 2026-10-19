-module(beamlet_moto_tests).
-include_lib("eunit/include/eunit.hrl").

defaults_and_managed_fields_test() ->
    ?assertEqual(#{kind => moto}, beamlet_moto:new()),
    {ok, C} = beamlet_container:validate(beamlet_moto:new()),
    ?assertEqual(<<"motoserver/moto:5.2.3">>, maps:get(image, C)),
    ?assertEqual([5000], maps:get(ports, C)),
    ?assertEqual(<<"us-east-1">>, maps:get(region, C)),
    ?assertEqual(<<"test">>, maps:get(access_key_id, C)),
    ?assertEqual(<<"test">>, maps:get(secret_access_key, C)),
    lists:foreach(
        fun(Override) ->
            ?assertMatch({error, _}, beamlet_container:validate(beamlet_moto:new(Override)))
        end,
        [#{ports => [80]}, #{command => [<<"other">>]}, #{environment => #{}}]
    ).

typed_options_and_recipe_precedence_test() ->
    App = #{
        presets => #{moto => #{region => <<"eu-west-1">>, access_key_id => <<"configured">>}},
        recipes => #{<<"s3">> => #{kind => moto, secret_access_key => <<"recipe-secret">>}}
    },
    {ok, Snapshot} = beamlet_config:snapshot(App, #{}),
    {ok, C} = beamlet_config:container(
        Snapshot, beamlet_moto:new(#{recipe => <<"s3">>, region => <<"us-east-1">>})
    ),
    ?assertEqual(<<"us-east-1">>, maps:get(region, C)),
    ?assertEqual(<<"configured">>, maps:get(access_key_id, C)),
    ?assertEqual(<<"recipe-secret">>, maps:get(secret_access_key, C)),
    lists:foreach(
        fun(Key) ->
            ?assertMatch(
                {error, _}, beamlet_container:validate(beamlet_moto:new(#{Key => <<>>}))
            ),
            ?assertMatch(
                {error, _},
                beamlet_config:snapshot(#{presets => #{moto => #{Key => <<0>>}}}, #{})
            ),
            lists:foreach(
                fun(Kind) ->
                    ?assertMatch(
                        {error, _},
                        beamlet_container:validate(#{
                            kind => Kind, image => <<"test:1">>, Key => <<"x">>
                        })
                    )
                end,
                [generic, kafka, dynamodb, postgres, redis]
            )
        end,
        [region, access_key_id, secret_access_key]
    ).

s3_readiness_and_client_settings_test() ->
    with_daemon(s3_body(), fun(D, S, Port) ->
        {ok, C} = beamlet:start_container(
            S,
            beamlet_moto:new(#{region => <<"eu-west-1">>, secret_access_key => <<"local-secret">>})
        ),
        ?assertEqual(
            #{
                endpoint => <<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>,
                region => <<"eu-west-1">>,
                access_key_id => <<"test">>,
                secret_access_key => <<"local-secret">>,
                force_path_style => true
            },
            beamlet_moto:client_config(C)
        ),
        [Body] = [
            json:decode(B)
         || {<<"POST">>, <<"/containers/create?name=", _/binary>>, B} <- beamlet_test_daemon:events(
                D
            )
        ],
        ?assert(lists:member(<<"S3_IGNORE_SUBDOMAIN_BUCKETNAME=true">>, maps:get(<<"Env">>, Body))),
        ?assertEqual(false, maps:is_key(<<"Cmd">>, Body))
    end).

http_success_without_s3_response_fails_and_cleans_up_test() ->
    with_daemon(<<"<html>not an S3 server</html>">>, fun(D, S, _) ->
        ?assertMatch(
            {error, _},
            beamlet:start_container(S, beamlet_moto:new(#{timeout => 200}))
        ),
        Events = beamlet_test_daemon:events(D),
        ?assert(lists:any(fun({M, P, _}) -> M =:= <<"GET">> andalso P =:= <<"/">> end, Events)),
        ?assert(lists:any(fun({M, _, _}) -> M =:= <<"DELETE">> end, Events))
    end).

s3_body() ->
    <<
        "<?xml version=\"1.0\"?><ListAllMyBucketsResult "
        "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Buckets/></ListAllMyBucketsResult>"
    >>.

with_daemon(Body, Fun) ->
    {ok, D} = beamlet_test_daemon:start_link(#{root_body => Body}),
    try
        Options = beamlet_test_daemon:options(D),
        #{port := Port} = uri_string:parse(maps:get(docker_host, Options)),
        ok = gen_server:call(D, {set_ports, #{<<"5000/tcp">> => Port}}),
        beamlet:with_session(Options, fun(S) -> Fun(D, S, Port) end)
    after
        beamlet_test_daemon:stop(D)
    end.
