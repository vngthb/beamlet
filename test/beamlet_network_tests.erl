-module(beamlet_network_tests).
-include_lib("eunit/include/eunit.hrl").

session_network_is_labeled_and_used_by_kafka_test() ->
    with_daemon(#{}, fun(D, Options) ->
        {ok, S} = beamlet:start_session(Options),
        {ok, K} = beamlet:start_container(S, beamlet_kafka:new()),
        Events = beamlet_test_daemon:events(D),
        [Net] = [json:decode(B) || {<<"POST">>, <<"/networks/create">>, B} <- Events],
        ?assert(maps:is_key(<<"org.beamlet.session">>, maps:get(<<"Labels">>, Net))),
        [Config] = [
            json:decode(B)
         || {<<"POST">>, P, B} <- Events,
            binary:match(P, <<"/containers/create">>) =/= nomatch
        ],
        [Alias] = maps:get(
            <<"Aliases">>,
            maps:get(
                maps:get(<<"Name">>, Net),
                maps:get(<<"EndpointsConfig">>, maps:get(<<"NetworkingConfig">>, Config))
            )
        ),
        [Tar] = [B || {<<"PUT">>, _, B} <- Events],
        {ok, [{_, Script}]} = erl_tar:extract({binary, Tar}, [memory]),
        ?assertNotEqual(nomatch, binary:match(Script, <<"INTERNAL://", Alias/binary, ":9093">>)),
        ?assert(is_binary(beamlet_kafka:bootstrap_servers(K))),
        ok = beamlet:stop_session(S),
        Deletes = [P || {<<"DELETE">>, P, _} <- beamlet_test_daemon:events(D)],
        ?assertMatch(<<"/networks/", _/binary>>, lists:last(Deletes))
    end).

sessions_have_independent_networks_test() ->
    with_daemon(#{}, fun(D, Options) ->
        {ok, A} = beamlet:start_session(Options),
        {ok, B} = beamlet:start_session(Options),
        try
            Nets = [
                maps:get(<<"Name">>, json:decode(Body))
             || {<<"POST">>, <<"/networks/create">>, Body} <- beamlet_test_daemon:events(D)
            ],
            ?assertEqual(2, length(lists:usort(Nets)))
        after
            beamlet:stop_session(A),
            beamlet:stop_session(B)
        end
    end).

ambiguous_network_creation_is_cleaned_test() ->
    with_daemon(#{fail_network_create => true}, fun(D, Options) ->
        ?assertMatch({error, _}, beamlet:start_session(Options)),
        Events = beamlet_test_daemon:events(D),
        [Net] = [
            maps:get(<<"Name">>, json:decode(B))
         || {<<"POST">>, <<"/networks/create">>, B} <- Events
        ],
        ?assert(lists:member({<<"DELETE">>, <<"/networks/", Net/binary>>, <<>>}, Events))
    end).

network_cleanup_failure_is_reported_test() ->
    with_daemon(#{fail_network_remove => true}, fun(_, Options) ->
        {ok, S} = beamlet:start_session(Options),
        ?assertMatch({error, #{stage := cleanup}}, beamlet:stop_session(S))
    end).

with_daemon(Opts, Fun) ->
    {ok, D} = beamlet_test_daemon:start_link(Opts),
    try
        Fun(D, beamlet_test_daemon:options(D))
    after
        beamlet_test_daemon:stop(D)
    end.
