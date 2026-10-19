-module(beamlet_wait).
-export([valid/1, run/5]).

-spec valid(term()) -> boolean().
valid({tcp, P}) ->
    is_integer(P) andalso P > 0 andalso P < 65536;
valid({log, Text}) ->
    is_binary(Text) andalso byte_size(Text) > 0;
valid({http, P, Path, Status}) ->
    valid({tcp, P}) andalso is_binary(Path) andalso
        is_integer(Status) andalso Status >= 100 andalso Status < 600;
valid({command, Args}) ->
    is_list(Args) andalso Args =/= [] andalso
        lists:all(fun is_binary/1, Args);
valid(_) ->
    false.

-spec run(
    [term()],
    beamlet_docker:connection(),
    binary(),
    beamlet:container(),
    beamlet_deadline:deadline()
) -> ok | {error, term()}.
run([], _, _, _, _) ->
    ok;
run([Strategy | Rest], Conn, Name, C, D) ->
    case poll(Strategy, Conn, Name, C, D) of
        ok -> run(Rest, Conn, Name, C, D);
        Error -> Error
    end.
poll(Strategy, Conn, Name, C, D) ->
    case beamlet_deadline:remaining(D) of
        0 ->
            {error, {wait_timeout, Strategy}};
        _ ->
            poll_running(Strategy, Conn, Name, C, D)
    end.

poll_running(Strategy, Conn, Name, C, D) ->
    case beamlet_docker:inspect(Conn, Name, D) of
        {ok, #{<<"State">> := #{<<"Running">> := false} = State}} ->
            {error, {container_exited, State}};
        {ok, _} ->
            retry_probe(probe(Strategy, Conn, Name, C, D), Strategy, Conn, Name, C, D);
        Error ->
            Error
    end.

retry_probe(ok, _, _, _, _, _) ->
    ok;
retry_probe(Last, Strategy, Conn, Name, C, D) ->
    case beamlet_deadline:remaining(D) of
        0 ->
            {error, {wait_timeout, Strategy, Last}};
        Left ->
            timer:sleep(min(100, Left)),
            poll(Strategy, Conn, Name, C, D)
    end.
probe({tcp, P}, _, _, C, D) ->
    case beamlet:endpoint(C, P) of
        {ok, {Host, Port}} ->
            case
                gen_tcp:connect(
                    binary_to_list(Host),
                    Port,
                    [binary, {active, false}],
                    min(1000, beamlet_deadline:remaining(D))
                )
            of
                {ok, S} ->
                    gen_tcp:close(S),
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
probe({log, Text}, Conn, Name, _, D) ->
    case beamlet_docker:logs(Conn, Name, D) of
        {ok, Logs} ->
            case binary:match(Logs, Text) of
                nomatch -> {error, not_found};
                _ -> ok
            end;
        Error ->
            Error
    end;
probe({command, Args}, Conn, Name, _, D) ->
    case beamlet_docker:exec(Conn, Name, Args, D) of
        {ok, 0, _} -> ok;
        Other -> {error, Other}
    end;
probe({http, P, Path, Status}, _, _, C, D) ->
    case http(C, P, Path, D) of
        {ok, Status, _} -> ok;
        Other -> {error, Other}
    end;
probe(kafka_ui, _, _, C, D) ->
    backend(
        C,
        8080,
        <<"/api/clusters/beamlet/topics">>,
        fun
            (#{<<"topics">> := Topics}) -> is_list(Topics);
            (_) -> false
        end,
        D
    );
probe(dynamodb_ui, _, _, C, D) ->
    backend(C, 8001, <<"/api/tables">>, fun is_list/1, D);
probe(redis, _, _, C, D) ->
    {ok, {Host, Port}} = beamlet:endpoint(C, 6379),
    case
        gen_tcp:connect(
            binary_to_list(Host),
            Port,
            [
                binary,
                {active, false},
                {packet, line},
                {packet_size, 64},
                {send_timeout, min(1000, beamlet_deadline:remaining(D))}
            ],
            min(1000, beamlet_deadline:remaining(D))
        )
    of
        {ok, Socket} ->
            try
                case gen_tcp:send(Socket, <<"*1\r\n$4\r\nPING\r\n">>) of
                    ok ->
                        case gen_tcp:recv(Socket, 0, min(1000, beamlet_deadline:remaining(D))) of
                            {ok, <<"+PONG\r\n">>} -> ok;
                            Other -> {error, Other}
                        end;
                    Error ->
                        Error
                end
            after
                gen_tcp:close(Socket)
            end;
        Error ->
            Error
    end;
probe(moto, _, _, C, D) ->
    case http(C, 5000, <<"/">>, D) of
        {ok, 200, Body} ->
            case
                binary:match(Body, <<"<ListAllMyBucketsResult">>) =/= nomatch andalso
                    binary:match(Body, <<"</ListAllMyBucketsResult>">>) =/= nomatch
            of
                true -> ok;
                false -> {error, invalid_s3_response}
            end;
        Other ->
            {error, Other}
    end;
probe(dynamodb, _, _, C, D) ->
    case
        beamlet_dynamodb_api:request(beamlet_dynamodb:client_config(C), <<"ListTables">>, #{}, D)
    of
        {ok, #{<<"TableNames">> := _}} -> ok;
        Other -> {error, Other}
    end.

backend(C, Port, Path, Check, D) ->
    case http(C, Port, Path, D) of
        {ok, 200, Body} ->
            try Check(json:decode(Body)) of
                true -> ok;
                false -> {error, invalid_backend_response}
            catch
                _:_ -> {error, invalid_backend_response}
            end;
        Other ->
            {error, Other}
    end.

http(C, Port, Path, D) ->
    case beamlet:endpoint(C, Port) of
        {ok, {Host, Mapped}} ->
            beamlet_http:request(
                #{transport => {tcp, binary_to_list(Host), Mapped}},
                <<"GET">>,
                Path,
                [],
                <<>>,
                D
            );
        Error ->
            Error
    end.
