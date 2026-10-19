-module(beamlet_cleanup_SUITE).
-export([
    suite/0, all/0, init_per_suite/1, end_per_suite/1, abrupt_vm_exit/1, normal_network_cleanup/1
]).

suite() -> [{timetrap, {minutes, 3}}].
all() -> [abrupt_vm_exit, normal_network_cleanup].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok -> Config;
        Skip -> Skip
    end.
end_per_suite(_) -> ok.
abrupt_vm_exit(_) ->
    beamlet:with_session(#{}, fun(SurvivorSession) ->
        {ok, Survivor} = beamlet:start_container(
            SurvivorSession,
            beamlet_container:new(<<"alpine:3.21.3">>, #{command => [<<"sleep">>, <<"300">>]})
        ),
        abrupt_exit(Survivor)
    end).

abrupt_exit(Survivor) ->
    Paths = [
        P
     || P <- code:get_path(),
        filelib:is_dir(P),
        (filelib:is_file(filename:join(P, "beamlet.beam")) orelse
            filelib:is_file(filename:join(P, "gun.beam")) orelse
            filelib:is_file(filename:join(P, "cow_http.beam")))
    ],
    Eval =
        "{ok,S}=beamlet:start_session(),"
        "{ok,C}=beamlet:start_container(S,beamlet_dynamodb:new()),"
        "{ok,U}=beamlet:start_container(S,beamlet_dynamodb_ui:new(C)),"
        "io:format(\"BEAMLET_ID ~s UI_ID ~s~n\",[beamlet:id(C),beamlet:id(U)]),"
        "io:get_line(\"\"),erlang:halt(137).",
    Args = ["-noshell"] ++ lists:append([["-pa", P] || P <- Paths]) ++ ["-eval", Eval],
    Port = open_port(
        {spawn_executable, filename:join([code:root_dir(), "bin", "erl"])},
        [binary, exit_status, use_stdio, stderr_to_stdout, {args, Args}]
    ),
    try
        {Id, UIId} = ready(Port, <<>>),
        {ok, Conn} = beamlet_docker:connect(#{}, beamlet_deadline:new(5000)),
        {ok, Info} = beamlet_docker:inspect(Conn, Id, beamlet_deadline:new(5000)),
        Network = network(Info),
        true = port_command(Port, <<"exit\n">>),
        receive
            {Port, {exit_status, 137}} -> ok
        after 5000 -> ct:fail(vm_did_not_exit)
        end,
        beamlet_test_support:eventually(
            fun() ->
                missing(beamlet_docker:inspect(Conn, Id, beamlet_deadline:new(2000))) andalso
                    missing(beamlet_docker:inspect(Conn, UIId, beamlet_deadline:new(2000))) andalso
                    missing(
                        beamlet_docker:inspect_network(Conn, Network, beamlet_deadline:new(2000))
                    )
            end,
            45000
        ),
        {ok, #{<<"State">> := #{<<"Running">> := true}} = SurvivorInfo} = beamlet:inspect(Survivor),
        {ok, _} = beamlet_docker:inspect_network(
            Conn, network(SurvivorInfo), beamlet_deadline:new(5000)
        ),
        ok
    after
        try
            port_close(Port)
        catch
            error:badarg -> ok
        end
    end.
ready(Port, Acc) ->
    receive
        {Port, {data, Chunk}} ->
            Text = <<Acc/binary, Chunk/binary>>,
            case
                re:run(Text, <<"BEAMLET_ID ([a-f0-9]+) UI_ID ([a-f0-9]+)\\r?\\n">>, [
                    {capture, [1, 2], binary}
                ])
            of
                {match, [Id, UIId]} -> {Id, UIId};
                nomatch -> ready(Port, Text)
            end;
        {Port, {exit_status, Code}} ->
            ct:fail({child_vm_failed, Code, Acc})
    after 120000 -> ct:fail({child_vm_timeout, Acc})
    end.

normal_network_cleanup(_) ->
    {ok, S} = beamlet:start_session(),
    try
        {ok, C} = beamlet:start_container(S, beamlet_redis:new()),
        {ok, Info} = beamlet:inspect(C),
        Network = network(Info),
        {ok, Conn} = beamlet_docker:connect(#{}, beamlet_deadline:new(5000)),
        ok = beamlet:stop_session(S),
        true = missing(beamlet_docker:inspect_network(Conn, Network, beamlet_deadline:new(5000))),
        true = missing(beamlet_docker:inspect(Conn, beamlet:id(C), beamlet_deadline:new(5000)))
    after
        beamlet:stop_session(S)
    end.
network(#{<<"NetworkSettings">> := #{<<"Networks">> := Networks}}) ->
    [Name] = maps:keys(Networks),
    Name.
missing({error, {http, 404, _}}) -> true;
missing(_) -> false.
