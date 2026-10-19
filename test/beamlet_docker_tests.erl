-module(beamlet_docker_tests).
-include_lib("eunit/include/eunit.hrl").

fragmented_http_test() ->
    {ok, D} = beamlet_test_daemon:start_link(#{fragment => true}),
    try
        {ok, Conn} = beamlet_docker:connect(
            beamlet_test_daemon:options(D), beamlet_deadline:new(1000)
        ),
        ?assertEqual(<<"1.45">>, maps:get(version, Conn))
    after
        beamlet_test_daemon:stop(D)
    end.

http_deadline_test() ->
    {ok, D} = beamlet_test_daemon:start_link(#{delay => 100}),
    try
        ?assertMatch(
            {error, _},
            beamlet_docker:connect(beamlet_test_daemon:options(D), beamlet_deadline:new(10))
        )
    after
        beamlet_test_daemon:stop(D)
    end.

docker_lifecycle_test() ->
    {ok, D} = beamlet_test_daemon:start_link(#{}),
    try
        Deadline = beamlet_deadline:new(2000),
        {ok, Conn} = beamlet_docker:connect(beamlet_test_daemon:options(D), Deadline),
        {ok, C} = beamlet_container:validate(
            beamlet_container:new(<<"image:1">>, #{ports => [8000]})
        ),
        {ok, Id} = beamlet_docker:create(
            Conn, <<"test">>, beamlet_container:docker_config(C, #{}), Deadline
        ),
        ok = beamlet_docker:start(Conn, Id, Deadline),
        {ok, Inspect} = beamlet_docker:inspect(Conn, Id, Deadline),
        ?assertEqual(true, maps:get(<<"Running">>, maps:get(<<"State">>, Inspect))),
        ?assertEqual({ok, <<"ready">>}, beamlet_docker:logs(Conn, Id, Deadline)),
        ok = beamlet_docker:remove(Conn, <<"test">>, Deadline),
        ok = beamlet_docker:remove(Conn, <<"test">>, Deadline)
    after
        beamlet_test_daemon:stop(D)
    end.

exec_responses_test_() ->
    Frame = <<1, 0, 0, 0, 3:32, "out", 2, 0, 0, 0, 3:32, "err">>,
    Cases = [
        {"nonzero exit with stdout and stderr",
            #{
                exec_output => Frame,
                exec_info => #{<<"Running">> => false, <<"ExitCode">> => 7}
            },
            {ok, 7, <<"outerr">>}},
        {"empty output", #{}, {ok, 0, <<>>}},
        {"malformed output", #{exec_output => <<"bad frame">>}, {error, invalid_docker_stream}},
        {"running exec", #{exec_info => #{<<"Running">> => true}}, {error, exec_still_running}},
        {"invalid create response", #{exec_create_response => #{}}, {error, invalid_exec_response}},
        {"start failure", #{exec_start_status => 500, exec_output => <<"failed">>},
            {error, {http, 500, <<"failed">>}}},
        {"inspect failure", #{exec_inspect_status => 500, exec_info => <<"failed">>},
            {error, {http, 500, <<"failed">>}}}
    ],
    [{Name, fun() -> verify_exec(Options, Expected) end} || {Name, Options, Expected} <- Cases].

verify_exec(Options, Expected) ->
    {ok, D} = beamlet_test_daemon:start_link(Options),
    try
        ok = beamlet:with_session(beamlet_test_daemon:options(D), fun(S) ->
            {ok, C} = beamlet:start_container(S, beamlet_container:new(<<"example:1">>)),
            ?assertEqual(Expected, beamlet:exec(C, [<<"example">>]))
        end)
    after
        beamlet_test_daemon:stop(D)
    end.
