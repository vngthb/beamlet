-module(beamlet_ryuk).
-export([start/6]).

-spec start(
    beamlet_docker:connection(),
    binary() | undefined,
    binary(),
    map(),
    pid(),
    beamlet_deadline:deadline()
) -> {ok, beamlet_docker:connection(), port() | undefined} | {error, term()}.
start(Conn, _, _, #{ryuk := false}, _, _) ->
    {ok, Conn, undefined};
start(Conn, Name, Token, Options, Owner, D) ->
    try create_and_register(Conn, Name, Token, Options, Owner, D) of
        {ok, _, _} = Ready ->
            Ready;
        Error ->
            beamlet_docker:remove(Conn, Name, beamlet_deadline:new(10000)),
            Error
    catch
        Class:Reason:Stack ->
            beamlet_docker:remove(Conn, Name, beamlet_deadline:new(10000)),
            erlang:raise(Class, Reason, Stack)
    end.

create_and_register(Conn, Name, Token, Options, Owner, D) ->
    Image = <<"testcontainers/ryuk:0.12.0">>,
    maybe
        ok ?=
            beamlet_runtime:stage(ryuk_pull, beamlet_docker:ensure_image(Conn, Image, missing, D)),
        Body = docker_config(Conn, Image, Options),
        {ok, Id} ?= beamlet_runtime:stage(ryuk_create, beamlet_docker:create(Conn, Name, Body, D)),
        ok ?= beamlet_runtime:stage(ryuk_start, beamlet_docker:start(Conn, Id, D)),
        {ok, Info} ?= beamlet_runtime:stage(ryuk_inspect, beamlet_docker:inspect(Conn, Id, D)),
        Handle = beamlet_runtime:handle(Conn, Name, Owner, Info),
        %% Docker can accept a published connection before Ryuk opens its listener.
        ok ?=
            beamlet_runtime:stage(
                ryuk_ready, beamlet_wait:run([{log, <<"Started">>}], Conn, Name, Handle, D)
            ),
        {ok, {Host, Port}} = beamlet:endpoint(Handle, 8080),
        {ok, Socket} ?= beamlet_runtime:stage(ryuk_connect, connect(Host, Port, D)),
        register_socket(Conn, Socket, Token, Owner, D)
    end.

docker_config(Conn, Image, Options) ->
    {ok, C} = beamlet_container:validate(beamlet_container:new(Image, #{ports => [8080]})),
    Body = beamlet_container:docker_config(C, #{}, maps:get(bind_ip, Conn, <<"127.0.0.1">>)),
    SocketPath = maps:get(docker_socket, Options, maps:get(socket, Conn)),
    Bind = iolist_to_binary([SocketPath, ":/var/run/docker.sock:rw"]),
    HostConfig = (maps:get(<<"HostConfig">>, Body))#{
        <<"AutoRemove">> => true, <<"Binds">> => [Bind]
    },
    Body#{<<"HostConfig">> => HostConfig}.

register_socket(Conn, Socket, Token, Owner, D) ->
    try register(Conn, Socket, Token, Owner, D) of
        {ok, _, _} = Ready ->
            Ready;
        Error ->
            gen_tcp:close(Socket),
            Error
    catch
        Class:Reason:Stack ->
            gen_tcp:close(Socket),
            erlang:raise(Class, Reason, Stack)
    end.

register(Conn, Socket, Token, Owner, D) ->
    ok = gen_tcp:send(Socket, <<"label=org.beamlet.session=", Token/binary, "\n">>),
    case gen_tcp:recv(Socket, 0, beamlet_deadline:remaining(D)) of
        {ok, <<"ACK\n">>} ->
            ok = gen_tcp:controlling_process(Socket, Owner),
            {ok, Conn, Socket};
        Other ->
            beamlet_runtime:stage(ryuk_register, {error, Other})
    end.

connect(Host, Port, D) ->
    case
        gen_tcp:connect(
            binary_to_list(Host),
            Port,
            [binary, {active, false}, {packet, line}, {send_timeout, 1000}],
            min(1000, beamlet_deadline:remaining(D))
        )
    of
        {ok, S} ->
            {ok, S};
        Error ->
            case beamlet_deadline:remaining(D) of
                0 ->
                    Error;
                Left ->
                    timer:sleep(min(100, Left)),
                    connect(Host, Port, D)
            end
    end.
