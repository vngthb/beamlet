-module(beamlet_binding_tests).
-include_lib("eunit/include/eunit.hrl").

presets_publish_only_on_loopback_test() ->
    Target = beamlet:make_container(<<"id">>, <<"target">>, self(), <<"localhost">>, #{}),
    Specs = [
        beamlet_container:new(<<"example:1">>, #{ports => [8080]}),
        beamlet_kafka:new(),
        beamlet_dynamodb:new(),
        beamlet_postgres:new(),
        beamlet_redis:new(),
        beamlet_moto:new(),
        beamlet_kafka_ui:new(Target),
        beamlet_dynamodb_ui:new(Target)
    ],
    lists:foreach(
        fun(Spec) ->
            {ok, Config} = beamlet_container:validate(Spec),
            Body = beamlet_container:docker_config(Config, #{}),
            Bindings = maps:get(<<"PortBindings">>, maps:get(<<"HostConfig">>, Body)),
            ?assert(map_size(Bindings) > 0),
            lists:foreach(
                fun(B) ->
                    ?assertEqual([#{<<"HostIp">> => <<"127.0.0.1">>, <<"HostPort">> => <<>>}], B)
                end,
                maps:values(Bindings)
            )
        end,
        Specs
    ).

bind_address_is_explicit_and_independent_of_client_host_test() ->
    {ok, LocalDefault, _} = beamlet_config:session(#{}, #{}, #{}),
    ?assertEqual(<<"127.0.0.1">>, maps:get(host, maps:get(connection, LocalDefault))),
    {ok, Default, _} = beamlet_config:session(#{}, #{host => <<"client-host">>}, #{}),
    ?assertEqual(<<"127.0.0.1">>, maps:get(bind_ip, maps:get(connection, Default))),
    App = #{session => #{bind_ip => <<"127.0.0.2">>}},
    Local = #{session => #{bind_ip => <<"0.0.0.0">>}},
    {ok, Session, _} = beamlet_config:session(App, #{config => Local}, #{}),
    ?assertEqual(<<"0.0.0.0">>, maps:get(bind_ip, maps:get(connection, Session))),
    {ok, Explicit, _} = beamlet_config:session(
        App, #{config => Local, bind_ip => <<"127.0.0.3">>}, #{host => <<"environment-host">>}
    ),
    ?assertEqual(<<"127.0.0.3">>, maps:get(bind_ip, maps:get(connection, Explicit))),
    ?assertEqual(<<"environment-host">>, maps:get(host, maps:get(connection, Explicit))).

invalid_bind_addresses_fail_configuration_test() ->
    lists:foreach(
        fun(IP) ->
            ?assertMatch(
                {error, #{stage := config, path := [options, bind_ip]}},
                beamlet_config:session(#{}, #{bind_ip => IP}, #{})
            ),
            ?assertMatch(
                {error, #{stage := config, path := [application, session, bind_ip]}},
                beamlet_config:session(
                    #{session => #{bind_ip => IP}}, #{bind_ip => <<"127.0.0.1">>}, #{}
                )
            )
        end,
        [
            <<>>,
            <<"localhost">>,
            <<"::1">>,
            <<"127.1">>,
            <<"999.1.2.3">>,
            <<"127.0.0.1", 0>>,
            "127.0.0.1",
            42
        ]
    ).

explicit_bind_address_keeps_docker_assigned_ports_test() ->
    {ok, Config} = beamlet_container:validate(beamlet_redis:new()),
    Body = beamlet_container:docker_config(Config, #{}, <<"0.0.0.0">>),
    Bindings = maps:get(<<"PortBindings">>, maps:get(<<"HostConfig">>, Body)),
    ?assertEqual(
        [#{<<"HostIp">> => <<"0.0.0.0">>, <<"HostPort">> => <<>>}],
        maps:get(<<"6379/tcp">>, Bindings)
    ).
