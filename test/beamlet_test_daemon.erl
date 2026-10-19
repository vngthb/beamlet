-module(beamlet_test_daemon).
-behaviour(gen_server).
-export([start_link/1, options/1, events/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link(Options) -> gen_server:start_link(?MODULE, Options, []).
options(Pid) -> gen_server:call(Pid, options).
events(Pid) -> gen_server:call(Pid, events).
stop(Pid) -> gen_server:stop(Pid).

init(Options) ->
    {ok, Listen} = gen_tcp:listen(0, [
        binary, {active, false}, {reuseaddr, true}, {ip, {127, 0, 0, 1}}
    ]),
    {ok, {_, Port}} = inet:sockname(Listen),
    Parent = self(),
    Acceptor = spawn_link(fun() -> accept(Listen, Parent) end),
    {ok, #{
        listen => Listen,
        acceptor => Acceptor,
        port => Port,
        options => Options,
        containers => #{},
        networks => #{},
        events => [],
        next => 1
    }}.
handle_call(options, _, S) ->
    {reply,
        #{
            docker_host => iolist_to_binary([
                "http://127.0.0.1:", integer_to_list(maps:get(port, S))
            ]),
            host => <<"127.0.0.1">>,
            ryuk => false
        },
        S};
handle_call(events, _, S) ->
    {reply, lists:reverse(maps:get(events, S)), S};
handle_call({set_ports, Ports}, _, S) ->
    {reply, ok, S#{options => (maps:get(options, S))#{port_map => Ports}}};
handle_call({set_options, Options}, _, S) ->
    {reply, ok, S#{options => maps:merge(maps:get(options, S), Options)}};
handle_call({request, Method, Path0, Body}, _, S) ->
    Path = re:replace(Path0, <<"^/v[0-9]+[.][0-9]+">>, <<>>, [{return, binary}]),
    S1 = S#{events => [{Method, Path, Body} | maps:get(events, S)]},
    {Status, Reply, S2} = route(Method, Path, Body, S1),
    case maps:get(notify, maps:get(options, S), undefined) of
        undefined -> ok;
        Listener -> Listener ! {docker_request, Method, Path}
    end,
    {reply, {Status, Reply, maps:get(options, S)}, S2};
handle_call(_, _, S) ->
    {reply, {error, bad_request}, S}.
handle_cast(_, S) -> {noreply, S}.
handle_info(_, S) -> {noreply, S}.
terminate(_, S) ->
    gen_tcp:close(maps:get(listen, S)),
    exit(maps:get(acceptor, S), shutdown).
code_change(_, S, _) -> {ok, S}.

route(<<"GET">>, <<"/version">>, _, S) ->
    {200, #{<<"ApiVersion">> => <<"1.45">>, <<"MinAPIVersion">> => <<"1.24">>}, S};
route(<<"GET">>, <<"/images/", _/binary>>, _, S) ->
    {200, #{}, S};
route(<<"POST">>, <<"/">>, _, S) ->
    {200, #{<<"TableNames">> => []}, S};
route(<<"GET">>, <<"/">>, _, S) ->
    {200, maps:get(root_body, maps:get(options, S), <<"<html>UI</html>">>), S};
route(<<"GET">>, <<"/api/clusters/beamlet/topics">>, _, S) ->
    ui_route(#{<<"topics">> => []}, S);
route(<<"GET">>, <<"/api/tables">>, _, S) ->
    ui_route([], S);
route(<<"POST">>, <<"/networks/create">>, Body, S) ->
    Config = json:decode(Body),
    Name = maps:get(<<"Name">>, Config),
    Next = S#{networks => (maps:get(networks, S))#{Name => Config}},
    case maps:get(fail_network_create, maps:get(options, S), false) of
        true -> {500, #{<<"message">> => <<"ambiguous network create">>}, Next};
        false -> {201, #{<<"Id">> => Name}, Next}
    end;
route(Method, <<"/networks/", Name/binary>>, _, S) ->
    network_route(Method, Name, S);
route(<<"POST">>, <<"/containers/create?name=", Name/binary>>, Body, S) ->
    N = maps:get(next, S),
    Id = integer_to_binary(N),
    C = #{id => Id, name => Name, config => json:decode(Body), running => false},
    {201, #{<<"Id">> => Id}, S#{
        next => N + 1,
        containers => (maps:get(containers, S))#{Name => C}
    }};
route(Method, <<"/containers/", Rest/binary>>, Body, S) ->
    [Name0 | Suffix] = binary:split(Rest, <<"/">>, [global]),
    [Name | _] = binary:split(Name0, <<"?">>),
    Cs = maps:get(containers, S),
    Found = [{K, C} || {K, C} <- maps:to_list(Cs), K =:= Name orelse maps:get(id, C) =:= Name],
    case Found of
        [] -> {404, #{<<"message">> => <<"missing">>}, S};
        [{Key, C}] -> container_route(Method, Suffix, Key, C, Body, S)
    end;
route(<<"POST">>, <<"/exec/", _/binary>>, _, S) ->
    Options = maps:get(options, S),
    {maps:get(exec_start_status, Options, 200), maps:get(exec_output, Options, <<>>), S};
route(<<"GET">>, <<"/exec/", _/binary>>, _, S) ->
    Options = maps:get(options, S),
    {
        maps:get(exec_inspect_status, Options, 200),
        maps:get(exec_info, Options, #{<<"Running">> => false, <<"ExitCode">> => 0}),
        S
    };
route(_, _, _, S) ->
    {200, #{}, S}.

ui_route(Body, S) ->
    case maps:get(backend_ready, maps:get(options, S), true) of
        true -> {200, maps:get(backend_body, maps:get(options, S), Body), S};
        false -> {503, #{<<"error">> => <<"backend unavailable">>}, S}
    end.

network_route(<<"DELETE">>, Name, S) ->
    case maps:get(fail_network_remove, maps:get(options, S), false) of
        true -> {500, #{<<"message">> => <<"network removal failed">>}, S};
        false -> {204, <<>>, S#{networks => maps:remove(Name, maps:get(networks, S))}}
    end;
network_route(<<"GET">>, Name, S) ->
    case maps:find(Name, maps:get(networks, S)) of
        {ok, Config} -> {200, Config#{<<"Id">> => Name}, S};
        error -> {404, #{<<"message">> => <<"missing network">>}, S}
    end.

container_route(<<"POST">>, [<<"start">>], K, C, _, S) ->
    case maps:get(fail_start, maps:get(options, S), false) of
        true ->
            {500, #{<<"message">> => <<"start failed">>}, S};
        false ->
            Running = not maps:get(exit_on_start, maps:get(options, S), false),
            {204, <<>>, S#{containers => (maps:get(containers, S))#{K => C#{running => Running}}}}
    end;
container_route(<<"GET">>, [<<"json">>], _, C, _, S) ->
    N = binary_to_integer(maps:get(id, C)),
    Exposed = maps:get(<<"ExposedPorts">>, maps:get(config, C), #{}),
    Overrides = maps:get(port_map, maps:get(options, S), #{}),
    Ports = maps:from_list([
        {P, [
            #{
                <<"HostIp">> => <<"0.0.0.0">>,
                <<"HostPort">> => integer_to_binary(maps:get(P, Overrides, 30000 + N))
            }
        ]}
     || P <- maps:keys(Exposed)
    ]),
    {200,
        #{
            <<"Id">> => maps:get(id, C),
            <<"State">> => #{
                <<"Running">> => maps:get(running, C),
                <<"ExitCode">> => 0
            },
            <<"NetworkSettings">> => #{<<"Ports">> => Ports}
        },
        S};
container_route(<<"DELETE">>, [], K, _, _, S) ->
    case maps:get(fail_remove, maps:get(options, S), false) of
        true -> {500, #{<<"message">> => <<"remove failed">>}, S};
        false -> {204, <<>>, S#{containers => maps:remove(K, maps:get(containers, S))}}
    end;
container_route(<<"POST">>, [<<"exec">>], _, _, _, S) ->
    {201, maps:get(exec_create_response, maps:get(options, S), #{<<"Id">> => <<"exec-one">>}), S};
container_route(<<"GET">>, [Logs], _, _, _, S) ->
    case binary:match(Logs, <<"logs">>) of
        {0, 4} ->
            Data = maps:get(logs, maps:get(options, S), <<"ready">>),
            {200, <<1, 0, 0, 0, (byte_size(Data)):32, Data/binary>>, S};
        _ ->
            {200, #{}, S}
    end;
container_route(_, _, _, _, _, S) ->
    {200, <<>>, S}.

accept(Listen, Parent) ->
    case gen_tcp:accept(Listen) of
        {ok, Socket} ->
            Handler = spawn(fun() ->
                receive
                    {socket, Sock} -> serve(Sock, Parent)
                after 1000 -> error(socket_handoff_timeout)
                end
            end),
            ok = gen_tcp:controlling_process(Socket, Handler),
            Handler ! {socket, Socket},
            accept(Listen, Parent);
        {error, closed} ->
            ok
    end.
serve(Socket, Parent) ->
    try
        {Head, Initial} = read_headers(Socket, <<>>),
        [Line | Headers] = binary:split(Head, <<"\r\n">>, [global]),
        [Method, Path, _] = binary:split(Line, <<" ">>, [global]),
        Length = lists:foldl(
            fun(H, Acc) ->
                case binary:split(string:lowercase(H), <<": ">>) of
                    [<<"content-length">>, V] -> binary_to_integer(V);
                    _ -> Acc
                end
            end,
            0,
            Headers
        ),
        Body = read_body(Socket, Initial, Length),
        {Status, Reply, Options} = gen_server:call(Parent, {request, Method, Path, Body}),
        Delay =
            case Method of
                <<"DELETE">> ->
                    maps:get(remove_delay, Options, maps:get(delay, Options, 0));
                _ ->
                    case binary:match(Path, <<"/start">>) of
                        nomatch -> maps:get(delay, Options, 0);
                        _ -> maps:get(start_delay, Options, maps:get(delay, Options, 0))
                    end
            end,
        timer:sleep(Delay),
        Data =
            case is_binary(Reply) of
                true -> Reply;
                false -> iolist_to_binary(json:encode(Reply))
            end,
        Response = iolist_to_binary([
            "HTTP/1.1 ",
            integer_to_list(Status),
            " OK\r\nConnection: close\r\nContent-Length: ",
            integer_to_list(byte_size(Data)),
            "\r\nContent-Type: application/json\r\n\r\n",
            Data
        ]),
        case maps:get(fragment, Options, false) of
            true -> [gen_tcp:send(Socket, <<B>>) || <<B>> <= Response];
            false -> gen_tcp:send(Socket, Response)
        end
    catch
        _:_ -> ok
    after
        gen_tcp:close(Socket)
    end.
read_headers(S, Acc) ->
    case binary:split(Acc, <<"\r\n\r\n">>) of
        [Head, Rest] ->
            {Head, Rest};
        [_] ->
            {ok, More} = gen_tcp:recv(S, 0, 5000),
            read_headers(S, <<Acc/binary, More/binary>>)
    end.
read_body(_, Acc, Length) when byte_size(Acc) >= Length -> binary:part(Acc, 0, Length);
read_body(S, Acc, Length) ->
    {ok, More} = gen_tcp:recv(S, 0, 5000),
    read_body(S, <<Acc/binary, More/binary>>, Length).
