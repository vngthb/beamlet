-module(beamlet_docker).
-export([
    connect/2,
    resolve/1,
    create/4,
    start/3,
    inspect/3,
    remove/3,
    logs/3,
    exec/4,
    ensure_image/4,
    upload/5,
    request/5,
    unpack/1,
    create_network/4,
    inspect_network/3,
    remove_network/3
]).
-export_type([connection/0]).
-type connection() :: #{
    transport := {unix, string()} | {tcp, string(), inet:port_number()},
    host := binary(),
    version := binary(),
    socket := string(),
    bind_ip => binary()
}.

-spec connect(map(), beamlet_deadline:deadline()) -> {ok, connection()} | {error, term()}.
connect(#{transport := _} = Base, Deadline) ->
    negotiate(Base, Deadline);
connect(Options, Deadline) ->
    Env = maps:from_list([
        {K, V}
     || {K, Name} <-
            [{docker_host, "DOCKER_HOST"}, {host, "TESTCONTAINERS_HOST_OVERRIDE"}],
        V <- [env(Name, undefined)],
        V =/= undefined
    ]),
    maybe
        {ok, Base} ?= resolve(maps:merge(Env, Options)),
        negotiate(Base, Deadline)
    end.

-doc false.
-spec resolve(map()) -> {ok, map()} | {error, term()}.
resolve(Options) ->
    HostSpec = maps:get(docker_host, Options, <<"unix:///var/run/docker.sock">>),
    case parse_host(HostSpec) of
        {ok, Base} ->
            Host = maps:get(host, Options, maps:get(host, Base)),
            BindIP = maps:get(bind_ip, Options, <<"127.0.0.1">>),
            ValidHost =
                is_binary(Host) andalso
                    re:run(Host, <<"^[a-zA-Z0-9_.:-]+$">>, [{capture, none}]) =:= match,
            case {ValidHost, valid_bind_ip(BindIP)} of
                {false, _} -> {error, invalid_host};
                {_, false} -> {error, invalid_bind_ip};
                {true, true} -> {ok, Base#{host => Host, bind_ip => BindIP}}
            end;
        Error ->
            Error
    end.
valid_bind_ip(IP) when is_binary(IP) ->
    case inet:parse_strict_address(binary_to_list(IP)) of
        {ok, Address} when tuple_size(Address) =:= 4 -> true;
        _ -> false
    end;
valid_bind_ip(_) ->
    false.
parse_host(<<"unix://", Path/binary>>) when byte_size(Path) > 0 ->
    {ok, #{
        transport => {unix, binary_to_list(Path)},
        socket => binary_to_list(Path),
        host => <<"127.0.0.1">>
    }};
parse_host(Host) when is_binary(Host) ->
    try uri_string:parse(Host) of
        #{scheme := Scheme, host := H, port := P} when
            (Scheme =:= <<"tcp">> orelse Scheme =:= <<"http">>) andalso
                is_integer(P) andalso P > 0 andalso P < 65536
        ->
            {ok, #{
                transport => {tcp, binary_to_list(H), P},
                socket => "/var/run/docker.sock",
                host => H
            }};
        _ ->
            {error, unsupported_docker_host}
    catch
        _:_ -> {error, invalid_docker_host}
    end;
parse_host(_) ->
    {error, invalid_docker_host}.
negotiate(Base, Deadline) ->
    case raw(Base, <<"GET">>, <<"/version">>, <<>>, [], Deadline) of
        {ok, 200, Body} ->
            try json:decode(Body) of
                #{<<"ApiVersion">> := Max} = Info ->
                    Low = version(maps:get(<<"MinAPIVersion">>, Info, <<"1.24">>)),
                    High = min(version(Max), {1, 45}),
                    case Low =< High andalso High >= {1, 41} of
                        true ->
                            {A, B} = High,
                            V = iolist_to_binary(io_lib:format("~B.~B", [A, B])),
                            {ok, Base#{version => V}};
                        false ->
                            {error, unsupported_api_version}
                    end;
                _ ->
                    {error, invalid_version_response}
            catch
                _:_ -> {error, invalid_version_response}
            end;
        Other ->
            error_response(Other)
    end.
version(V) ->
    [A, B] = binary:split(V, <<".">>),
    {binary_to_integer(A), binary_to_integer(B)}.
env(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        V -> list_to_binary(V)
    end.

-spec request(connection(), binary(), iodata(), term(), beamlet_deadline:deadline()) ->
    {ok, term()} | {error, term()}.
request(Conn, Method, Path, Payload, Deadline) ->
    Data =
        case Payload of
            undefined -> <<>>;
            _ -> json:encode(Payload)
        end,
    case
        raw(
            Conn,
            Method,
            [<<"/v">>, maps:get(version, Conn), Path],
            Data,
            [{<<"content-type">>, <<"application/json">>}],
            Deadline
        )
    of
        {ok, Status, Body} when (Status >= 200 andalso Status < 300) orelse Status =:= 304 ->
            case Body of
                <<>> ->
                    {ok, undefined};
                _ ->
                    try
                        {ok, json:decode(Body)}
                    catch
                        _:_ -> {ok, Body}
                    end
            end;
        Other ->
            error_response(Other)
    end.
raw(Conn, Method, Path, Body, Headers, Deadline) ->
    beamlet_http:request(Conn, Method, Path, Headers, Body, Deadline).
error_response({error, Reason}) ->
    {error, Reason};
error_response({ok, Status, Body}) ->
    {error, {http, Status, binary:part(Body, 0, min(1024, byte_size(Body)))}}.
path(Id, Tail) -> [<<"/containers/">>, quote(Id), Tail].
quote(B) -> uri_string:quote(B).

-spec create(connection(), binary(), map(), beamlet_deadline:deadline()) ->
    {ok, binary()} | {error, term()}.
create(C, Name, Body, D) ->
    case request(C, <<"POST">>, [<<"/containers/create?name=">>, quote(Name)], Body, D) of
        {ok, #{<<"Id">> := Id}} -> {ok, Id};
        {ok, _} -> {error, invalid_create_response};
        Error -> Error
    end.
-spec start(connection(), binary(), beamlet_deadline:deadline()) -> ok | {error, term()}.
start(C, Id, D) -> ok_result(request(C, <<"POST">>, path(Id, <<"/start">>), undefined, D)).
-spec inspect(connection(), binary(), beamlet_deadline:deadline()) -> {ok, map()} | {error, term()}.
inspect(C, Id, D) -> request(C, <<"GET">>, path(Id, <<"/json">>), undefined, D).
-spec remove(connection(), binary(), beamlet_deadline:deadline()) -> ok | {error, term()}.
remove(C, Id, D) ->
    case request(C, <<"DELETE">>, path(Id, <<"?force=true&v=true">>), undefined, D) of
        {error, {http, 404, _}} -> ok;
        Other -> ok_result(Other)
    end.
ok_result({ok, _}) -> ok;
ok_result(Error) -> Error.

-spec create_network(connection(), binary(), map(), beamlet_deadline:deadline()) ->
    {ok, binary()} | {error, term()}.
create_network(C, Name, Labels, D) ->
    case
        request(
            C,
            <<"POST">>,
            <<"/networks/create">>,
            #{<<"Name">> => Name, <<"Driver">> => <<"bridge">>, <<"Labels">> => Labels},
            D
        )
    of
        {ok, #{<<"Id">> := Id}} -> {ok, Id};
        {ok, _} -> {error, invalid_network_create_response};
        Error -> Error
    end.
-spec inspect_network(connection(), binary(), beamlet_deadline:deadline()) ->
    {ok, map()} | {error, term()}.
inspect_network(C, Name, D) -> request(C, <<"GET">>, [<<"/networks/">>, quote(Name)], undefined, D).
-spec remove_network(connection(), binary(), beamlet_deadline:deadline()) -> ok | {error, term()}.
remove_network(C, Name, D) ->
    case request(C, <<"DELETE">>, [<<"/networks/">>, quote(Name)], undefined, D) of
        {error, {http, 404, _}} -> ok;
        Other -> ok_result(Other)
    end.

-spec ensure_image(connection(), binary(), missing | always | never, beamlet_deadline:deadline()) ->
    ok | {error, term()}.
ensure_image(_, _, never, _) ->
    ok;
ensure_image(C, Image, missing, D) ->
    case request(C, <<"GET">>, [<<"/images/">>, quote(Image), <<"/json">>], undefined, D) of
        {ok, _} -> ok;
        {error, {http, 404, _}} -> ensure_image(C, Image, always, D);
        Error -> Error
    end;
ensure_image(C, Image, always, D) ->
    case request(C, <<"POST">>, [<<"/images/create?fromImage=">>, quote(Image)], undefined, D) of
        {ok, B} when is_binary(B) -> pull_result(binary:split(B, <<"\n">>, [global]));
        {ok, M} when is_map(M) -> pull_event(M);
        Other -> ok_result(Other)
    end.
pull_result([]) ->
    ok;
pull_result([<<>> | Rest]) ->
    pull_result(Rest);
pull_result([Line | Rest]) ->
    try pull_event(json:decode(Line)) of
        ok -> pull_result(Rest);
        Error -> Error
    catch
        _:_ -> {error, invalid_pull_response}
    end.
pull_event(#{<<"error">> := Error}) -> {error, {image_pull, Error}};
pull_event(#{<<"errorDetail">> := Detail}) -> {error, {image_pull, Detail}};
pull_event(_) -> ok.

-spec logs(connection(), binary(), beamlet_deadline:deadline()) -> {ok, binary()} | {error, term()}.
logs(C, Id, D) ->
    case
        request(C, <<"GET">>, path(Id, <<"/logs?stdout=true&stderr=true&tail=1000">>), undefined, D)
    of
        {ok, Data} when is_binary(Data) -> unpack(Data);
        {ok, undefined} -> {ok, <<>>};
        Error -> Error
    end.
-spec unpack(binary()) -> {ok, binary()} | {error, term()}.
unpack(Data) -> unpack(Data, []).
unpack(<<>>, Acc) ->
    {ok, iolist_to_binary(lists:reverse(Acc))};
unpack(<<Type, 0, 0, 0, Size:32, Rest/binary>>, Acc) when Type =< 2, byte_size(Rest) >= Size ->
    <<Chunk:Size/binary, Tail/binary>> = Rest,
    unpack(Tail, [Chunk | Acc]);
unpack(_, _) ->
    {error, invalid_docker_stream}.

-spec exec(connection(), binary(), [binary()], beamlet_deadline:deadline()) ->
    {ok, non_neg_integer(), binary()} | {error, term()}.
exec(C, Id, Args, D) ->
    case
        request(
            C,
            <<"POST">>,
            path(Id, <<"/exec">>),
            #{
                <<"Cmd">> => Args,
                <<"AttachStdout">> => true,
                <<"AttachStderr">> => true,
                <<"Tty">> => false
            },
            D
        )
    of
        {ok, #{<<"Id">> := ExecId}} ->
            start_exec(C, ExecId, D);
        {ok, _} ->
            {error, invalid_exec_response};
        Error ->
            Error
    end.

start_exec(C, ExecId, D) ->
    case
        request(
            C,
            <<"POST">>,
            [<<"/exec/">>, quote(ExecId), <<"/start">>],
            #{<<"Detach">> => false, <<"Tty">> => false},
            D
        )
    of
        {ok, Output} -> exec_result(C, ExecId, Output, D);
        Error -> Error
    end.

exec_result(C, ExecId, Output, D) ->
    case request(C, <<"GET">>, [<<"/exec/">>, quote(ExecId), <<"/json">>], undefined, D) of
        {ok, #{<<"Running">> := false, <<"ExitCode">> := Code}} ->
            case unpack(exec_output(Output)) of
                {ok, Text} -> {ok, Code, Text};
                Error -> Error
            end;
        {ok, _} ->
            {error, exec_still_running};
        Error ->
            Error
    end.

exec_output(undefined) -> <<>>;
exec_output(Output) -> Output.

-spec upload(connection(), binary(), binary(), binary(), beamlet_deadline:deadline()) ->
    ok | {error, term()}.
upload(C, Id, Filename, Contents, D) ->
    Tar = archive(Filename, Contents),
    case
        raw(
            C,
            <<"PUT">>,
            [<<"/v">>, maps:get(version, C), path(Id, <<"/archive?path=/tmp">>)],
            Tar,
            [{<<"content-type">>, <<"application/x-tar">>}],
            D
        )
    of
        {ok, 200, _} -> ok;
        Other -> error_response(Other)
    end.
archive(Name, Contents) ->
    Size = byte_size(Contents),
    Header0 =
        <<Name/binary, 0:((100 - byte_size(Name)) * 8), "0000644", 0, "0000000", 0, "0000000", 0,
            (octal(Size, 11))/binary, 0, "00000000000", 0, "        ", "0", 0:800, "ustar", 0, "00",
            0:1976>>,
    Sum = lists:sum(binary_to_list(Header0)),
    <<Before:148/binary, _:8/binary, After/binary>> = Header0,
    Header = <<Before/binary, (octal(Sum, 6))/binary, 0, 32, After/binary>>,
    <<Header/binary, Contents/binary, 0:(((512 - (Size rem 512)) rem 512) * 8), 0:8192>>.
octal(N, Width) ->
    B = integer_to_binary(N, 8),
    <<(binary:copy(<<"0">>, Width - byte_size(B)))/binary, B/binary>>.
