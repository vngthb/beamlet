-module(beamlet).

-export([
    start_session/0,
    start_session/1,
    stop_session/1,
    with_session/2,
    start_container/2,
    stop_container/1,
    endpoint/2,
    id/1,
    inspect/1,
    exec/2,
    logs/1,
    make_container/5,
    settings/1,
    with_settings/2,
    reference/1,
    private_host/1
]).
-export_type([container/0, session/0]).
-type session() :: pid().
-opaque container() :: #{
    id := binary(),
    name := binary(),
    session := session(),
    host := binary(),
    ports := #{pos_integer() => inet:port_number()},
    settings => map()
}.

-spec start_session() -> {ok, session()} | {error, term()}.
start_session() -> start_session(#{}).
-spec start_session(map()) -> {ok, session()} | {error, term()}.
start_session(Options) when is_map(Options) ->
    maybe
        ok ?= load_application(),
        App = application:get_env(beamlet, config, #{}),
        Env = maps:from_list([
            {Key, list_to_binary(Value)}
         || {Key, Name} <-
                [{docker_host, "DOCKER_HOST"}, {host, "TESTCONTAINERS_HOST_OVERRIDE"}],
            Value <- [os:getenv(Name)],
            is_list(Value)
        ]),
        {ok, Resolved, Snapshot} ?= beamlet_config:session(App, Options, Env),
        start_session_tree(Resolved#{config => Snapshot})
    end;
start_session(_) ->
    {error, invalid_session_options}.

load_application() ->
    case application:load(beamlet) of
        ok -> ok;
        {error, {already_loaded, beamlet}} -> ok;
        Error -> Error
    end.

start_session_tree(Options) ->
    %% Supervisor reports print MFA arguments, including any configured credentials.
    GetOptions = fun() -> Options end,
    Spec = #{
        id => make_ref(),
        start => {beamlet_session_tree, start_link, [GetOptions]},
        restart => temporary,
        shutdown => infinity,
        type => supervisor
    },
    maybe
        {ok, _} ?= application:ensure_all_started(beamlet),
        {ok, Tree} ?= supervisor:start_child(beamlet_sup, Spec),
        ready_session(Tree)
    end.

ready_session(Tree) ->
    {session, Session, _, _} = lists:keyfind(session, 1, supervisor:which_children(Tree)),
    case call(Session, ready) of
        ok ->
            {ok, Session};
        Error ->
            stop_session(Session),
            Error
    end.

-spec stop_session(session()) -> ok | {error, term()}.
stop_session(Session) ->
    case call(Session, stop) of
        {error, session_closed} -> ok;
        Result -> Result
    end.
-spec with_session(map(), fun((session()) -> Result)) -> Result | {error, term()}.
with_session(Options, Fun) ->
    case start_session(Options) of
        {ok, Session} ->
            try Fun(Session) of
                Result ->
                    case stop_session(Session) of
                        ok -> Result;
                        {error, Reason} when is_map(Reason) -> {error, Reason#{result => Result}};
                        Error -> {error, #{stage => cleanup, reason => Error, result => Result}}
                    end
            catch
                Class:Reason:Stack ->
                    case stop_session(Session) of
                        ok -> ok;
                        Error -> logger:error("Beamlet cleanup after test failure: ~p", [Error])
                    end,
                    erlang:raise(Class, Reason, Stack)
            end;
        Error ->
            Error
    end.
-spec start_container(session(), beamlet_container:spec()) -> {ok, container()} | {error, term()}.
start_container(Session, Config) -> call(Session, {start, Config}).
-spec stop_container(container()) -> ok | {error, term()}.
stop_container(#{session := Session, name := Name}) -> call(Session, {remove, Name}).
-spec endpoint(container(), pos_integer()) ->
    {ok, {binary(), inet:port_number()}} | {error, port_not_exposed}.
endpoint(#{host := Host, ports := Ports}, Port) ->
    case maps:find(Port, Ports) of
        {ok, P} -> {ok, {Host, P}};
        error -> {error, port_not_exposed}
    end.
-spec id(container()) -> binary().
id(#{id := Id}) -> Id.
-spec inspect(container()) -> {ok, map()} | {error, term()}.
inspect(#{session := S, name := N}) -> call(S, {inspect, N}).
-spec exec(container(), [binary()]) -> {ok, non_neg_integer(), binary()} | {error, term()}.
exec(#{session := S, name := N}, Args) -> call(S, {exec, N, Args}).
-spec logs(container()) -> {ok, binary()} | {error, term()}.
logs(#{session := S, name := N}) -> call(S, {logs, N}).
-doc false.
-spec make_container(binary(), binary(), session(), binary(), map()) -> container().
make_container(Id, Name, Session, Host, Ports) ->
    #{
        id => Id,
        name => Name,
        session => Session,
        host => Host,
        ports => Ports
    }.
-doc false.
-spec settings(container()) -> map().
settings(C) -> maps:get(settings, C, #{}).
-doc false.
-spec with_settings(container(), map()) -> container().
with_settings(C, Settings) -> C#{settings => Settings}.
-doc false.
-spec reference(term()) -> {ok, map()} | error.
reference(#{session := S, name := N, id := Id}) when is_pid(S), is_binary(N), is_binary(Id) ->
    {ok, #{session => S, name => N, id => Id}};
reference(_) ->
    error.
-doc false.
-spec private_host(term()) -> binary().
private_host(C) ->
    case reference(C) of
        {ok, #{name := N}} -> N;
        error -> <<"invalid">>
    end.
call(Session, Request) ->
    try
        gen_server:call(Session, Request, infinity)
    catch
        exit:{noproc, _} -> {error, session_closed};
        exit:{normal, _} -> {error, session_closed};
        exit:{shutdown, _} -> {error, session_closed};
        exit:Reason -> {error, {session_exit, Reason}}
    end.
