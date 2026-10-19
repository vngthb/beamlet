-module(beamlet_session).
-behaviour(gen_server).
-export([
    start_link/2,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    format_status/1
]).

start_link(Tree, GetOptions) -> gen_server:start_link(?MODULE, {Tree, GetOptions}, []).
init({Tree, GetOptions}) ->
    Options = GetOptions(),
    process_flag(trap_exit, true),
    Owner = monitor(process, maps:get(owner, Options)),
    Token = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    {ok, #{
        tree => Tree,
        options => Options,
        owner => Owner,
        token => Token,
        network => <<"beamlet-net-", Token/binary>>,
        phase => new,
        jobs => #{},
        resources => #{},
        remove_queue => [],
        connection => undefined,
        socket => undefined,
        reaper =>
            case maps:get(ryuk, Options) of
                true -> <<"beamlet-ryuk-", Token/binary>>;
                false -> undefined
            end
    }}.

handle_call(ready, From, #{phase := new} = S) ->
    {jobs, Sup, _, _} = lists:keyfind(jobs, 1, supervisor:which_children(maps:get(tree, S))),
    Deadline = beamlet_deadline:new(maps:get(timeout, maps:get(options, S))),
    Self = self(),
    Fun = fun() ->
        case beamlet_docker:connect(maps:get(connection, maps:get(options, S)), Deadline) of
            {ok, Conn} ->
                beamlet_ryuk:start(
                    Conn,
                    maps:get(reaper, S),
                    maps:get(token, S),
                    maps:get(options, S),
                    Self,
                    Deadline
                );
            Error ->
                Error
        end
    end,
    {noreply,
        job(
            bootstrap,
            From,
            undefined,
            Fun,
            beamlet_deadline:remaining(Deadline),
            S#{sup => Sup, phase => starting, bootstrap_deadline => Deadline}
        )};
handle_call(ready, _, #{phase := ready} = S) ->
    {reply, ok, S};
handle_call(stop, From, #{phase := stopping} = S) ->
    {noreply, S#{stop_waiters => [From | maps:get(stop_waiters, S)]}};
handle_call(stop, From, S) ->
    {noreply, shutdown_session(From, S)};
handle_call({start, Input}, From, #{phase := ready} = S) ->
    Snapshot = maps:get(config, maps:get(options, S)),
    case beamlet_config:container(Snapshot, Input) of
        {ok, Config} -> start_resolved(Config, From, S);
        Error -> {reply, Error, S}
    end;
handle_call(Request, From, #{phase := ready} = S) ->
    operation(Request, From, S);
handle_call(_, _, S) ->
    {reply, {error, session_not_ready}, S}.
handle_cast(_, S) -> {noreply, S}.

handle_info({job_done, Pid, Result}, S) ->
    case take_job(Pid, S) of
        error -> {noreply, S};
        {J, S1} -> result(J, Result, S1)
    end;
handle_info({job_timeout, Pid}, S) ->
    cancel(Pid, timeout, S);
handle_info({'DOWN', Ref, process, _, _}, #{owner := Ref, phase := new} = S) ->
    {stop, normal, S};
handle_info({'DOWN', Ref, process, Pid, Reason}, #{owner := Ref} = S) ->
    _ = {Pid, Reason},
    {noreply, shutdown_session(undefined, S)};
handle_info({'DOWN', Ref, process, Pid, Reason}, S) ->
    Jobs = maps:get(jobs, S),
    case [P || {P, J} <- maps:to_list(Jobs), maps:get(caller, J, undefined) =:= Ref] of
        [P | _] ->
            cancel(P, caller_down, S);
        [] ->
            case maps:find(Pid, Jobs) of
                {ok, #{monitor := Ref}} -> cancel(Pid, {worker_down, Reason}, S);
                _ -> {noreply, S}
            end
    end;
handle_info({tcp_closed, Socket}, #{socket := Socket} = S) ->
    {noreply, shutdown_session(undefined, S#{socket => undefined})};
handle_info({tcp_error, Socket, _}, #{socket := Socket} = S) ->
    {noreply, shutdown_session(undefined, S)};
handle_info(_, S) ->
    {noreply, S}.

job(Op, From, Name, Fun, Timeout, S) ->
    Id = make_ref(),
    Spec = #{
        id => Id,
        start => {beamlet_job, start_link, [self(), Fun]},
        restart => temporary,
        shutdown => brutal_kill
    },
    {ok, Pid} = supervisor:start_child(maps:get(sup, S), Spec),
    Ref = monitor(process, Pid),
    Caller =
        case From of
            undefined -> undefined;
            {P, _} -> monitor(process, P)
        end,
    Timer = erlang:send_after(max(1, Timeout), self(), {job_timeout, Pid}),
    J = #{
        operation => Op,
        from => From,
        name => Name,
        id => Id,
        monitor => Ref,
        caller => Caller,
        timer => Timer
    },
    S#{jobs => (maps:get(jobs, S))#{Pid => J}}.
take_job(Pid, S) ->
    case maps:take(Pid, maps:get(jobs, S)) of
        error ->
            error;
        {J, Jobs} ->
            erlang:cancel_timer(maps:get(timer, J)),
            demonitor(maps:get(monitor, J), [flush]),
            case maps:get(caller, J) of
                undefined -> ok;
                Ref -> demonitor(Ref, [flush])
            end,
            {J, S#{jobs => Jobs}}
    end.
reply(#{from := undefined}, _) -> ok;
reply(#{from := From}, Result) -> gen_server:reply(From, Result).

result(#{operation := bootstrap} = J, {ok, Conn, Socket}, S) ->
    case Socket of
        undefined -> ok;
        _ -> inet:setopts(Socket, [{active, once}])
    end,
    D = maps:get(bootstrap_deadline, S),
    Network = maps:get(network, S),
    Labels = #{<<"org.beamlet.session">> => maps:get(token, S)},
    Fun = fun() ->
        case beamlet_docker:create_network(Conn, Network, Labels, D) of
            {ok, _} -> ok;
            Error -> beamlet_runtime:stage(network_create, Error)
        end
    end,
    {noreply,
        job(
            network,
            maps:get(from, J),
            Network,
            Fun,
            beamlet_deadline:remaining(D),
            S#{connection => Conn, socket => Socket}
        )};
result(#{operation := bootstrap} = J, Error, S) ->
    reply(J, Error),
    {stop, normal, S};
result(#{operation := network} = J, ok, S) ->
    reply(J, ok),
    {noreply, S#{phase => ready}};
result(#{operation := network} = J, Error, S) ->
    {noreply, shutdown_session(maps:get(from, J), S#{startup_error => Error})};
result(#{operation := start, name := Name} = J, {ok, C}, S) ->
    reply(J, {ok, C}),
    Resource = (maps:get(Name, maps:get(resources, S)))#{
        phase => ready, id => beamlet:id(C), uncertain => false
    },
    {noreply, S#{resources => (maps:get(resources, S))#{Name => Resource}}};
result(#{operation := start} = J, Error, S) ->
    cleanup_failed(J, Error, S);
result(#{operation := {failed, Original}, name := Name} = J, Cleanup, S) ->
    Result =
        case Cleanup of
            ok -> Original;
            _ -> {error, #{failure => Original, cleanup => Cleanup, resource => Name}}
        end,
    %% Keep the reserved name for a final sweep: a timed-out create may still
    %% finish in the daemon after the first removal attempt returned 404.
    reply(J, Result),
    {noreply, resource_phase(Name, failed, S)};
result(#{operation := remove} = J, {Result, Removed}, S) when is_list(Removed) ->
    reply(J, Result),
    Resources = lists:foldl(
        fun(Name, Acc) ->
            case maps:get(Name, Acc) of
                #{uncertain := true} = R -> Acc#{Name => R#{phase => failed}};
                _ -> maps:remove(Name, Acc)
            end
        end,
        maps:get(resources, S),
        Removed
    ),
    S1 = maps:remove(removing, S#{resources => Resources}),
    {noreply, next_removal(S1)};
result(#{operation := remove} = J, Error, S) ->
    reply(J, Error),
    {noreply, next_removal(maps:remove(removing, S))};
result(#{operation := shutdown}, Result, S) ->
    Reply =
        case maps:find(startup_error, S) of
            {ok, Original} when Result =:= ok -> Original;
            {ok, Original} -> {error, #{failure => Original, cleanup => Result}};
            error -> Result
        end,
    [gen_server:reply(F, Reply) || F <- maps:get(stop_waiters, S, [])],
    {stop, normal, S#{resources => #{}, cleaned => true}};
result(J, Result, S) ->
    reply(J, Result),
    {noreply, S}.
cleanup_failed(J, Error, S) ->
    Name = maps:get(name, J),
    Conn = maps:get(connection, S),
    Fun = fun() -> beamlet_docker:remove(Conn, Name, beamlet_deadline:new(10000)) end,
    {noreply, job({failed, Error}, maps:get(from, J), Name, Fun, 11000, S)}.
cancel(Pid, Reason, S) ->
    case maps:find(Pid, maps:get(jobs, S)) of
        {ok, #{operation := Op} = J} when
            Reason =:= caller_down andalso
                (Op =:= remove orelse is_tuple(Op))
        ->
            {noreply, S#{
                jobs => (maps:get(jobs, S))#{Pid => J#{from => undefined, caller => undefined}}
            }};
        _ ->
            cancel_job(Pid, Reason, S)
    end.
cancel_job(Pid, Reason, S) ->
    case take_job(Pid, S) of
        error ->
            {noreply, S};
        {J, S1} ->
            supervisor:terminate_child(maps:get(sup, S), maps:get(id, J)),
            Error = {error, #{stage => maps:get(operation, J), reason => Reason}},
            case maps:get(operation, J) of
                start ->
                    CleanupJob =
                        case Reason of
                            caller_down -> J#{from => undefined};
                            _ -> J
                        end,
                    cleanup_failed(CleanupJob, Error, S1);
                bootstrap ->
                    reply(J, Error),
                    {stop, normal, S1};
                network ->
                    result(J, Error, S1);
                remove ->
                    result(J, Error, S1);
                shutdown ->
                    result(J, Error, S1);
                _ ->
                    reply(J, Error),
                    {noreply, S1}
            end
    end.

operation({remove, Name}, From, S) ->
    case maps:is_key(Name, maps:get(resources, S)) of
        false ->
            {reply, ok, S};
        true ->
            case length(maps:get(remove_queue, S)) >= 16 of
                true ->
                    {reply, {error, busy}, S};
                false ->
                    Names = removal_names(Name, S),
                    S1 = stop_startups(Names, S),
                    S2 = lists:foldl(
                        fun(N, Acc) -> resource_phase(N, stopping, Acc) end, S1, Names
                    ),
                    Queue = maps:get(remove_queue, S2) ++ [{Name, From}],
                    {noreply, next_removal(S2#{remove_queue => Queue})}
            end
    end;
operation({inspect, _} = Request, From, S) ->
    query(Request, From, S);
operation({logs, _} = Request, From, S) ->
    query(Request, From, S);
operation({exec, _, _} = Request, From, S) ->
    query(Request, From, S);
operation(_, _, S) ->
    {reply, {error, unknown_request}, S}.

query(_, _, #{jobs := Jobs} = S) when map_size(Jobs) >= 16 -> {reply, {error, busy}, S};
query(Request, From, S) ->
    Name = element(2, Request),
    Conn = maps:get(connection, S),
    D = beamlet_deadline:new(10000),
    case maps:get(Name, maps:get(resources, S), missing) of
        #{phase := ready} ->
            Fun = fun() ->
                case Request of
                    {inspect, _} -> beamlet_docker:inspect(Conn, Name, D);
                    {logs, _} -> beamlet_docker:logs(Conn, Name, D);
                    {exec, _, Args} -> beamlet_docker:exec(Conn, Name, Args, D)
                end
            end,
            {noreply, job(query, From, Name, Fun, 10000, S)};
        _ ->
            {reply, {error, container_not_ready}, S}
    end.

shutdown_session(From, #{phase := stopping} = S) ->
    case From of
        undefined -> S;
        _ -> S#{stop_waiters => [From | maps:get(stop_waiters, S)]}
    end;
shutdown_session(From, S) ->
    S1 = lists:foldl(
        fun(P, Acc) ->
            {J, Next} = take_job(P, Acc),
            supervisor:terminate_child(maps:get(sup, Acc), maps:get(id, J)),
            reply(J, {error, session_closed}),
            Next
        end,
        S,
        maps:keys(maps:get(jobs, S))
    ),
    Waiters =
        case From of
            undefined -> [];
            _ -> [From]
        end,
    [gen_server:reply(F, {error, session_closed}) || {_, F} <- maps:get(remove_queue, S1)],
    Conn = maps:get(connection, S1),
    Resources = maps:get(resources, S1),
    Socket = maps:get(socket, S1),
    Network = maps:get(network, S1),
    Fun = fun() -> cleanup(Conn, Resources, Socket, Network) end,
    Stopping = S1#{phase => stopping, stop_waiters => Waiters},
    job(shutdown, undefined, undefined, Fun, 12000, Stopping).
cleanup(undefined, _, _, _) ->
    ok;
cleanup(Conn, Resources, Socket, Network) ->
    D = beamlet_deadline:new(10000),
    ContainerErrors = cleanup_resources(Conn, Resources, D),
    NetworkErrors =
        case beamlet_docker:remove_network(Conn, Network, D) of
            ok -> [];
            Error -> [{Network, Error}]
        end,
    Errors = ContainerErrors ++ NetworkErrors,
    case Socket of
        undefined -> ok;
        _ -> gen_tcp:close(Socket)
    end,
    %% Ryuk must remain alive to retry failed or ambiguous Docker operations.
    %% Closing its connection triggers its sweep and eventual self-removal.
    case Errors of
        [] -> ok;
        _ -> {error, #{stage => cleanup, resources => Errors}}
    end.
terminate(_, S) ->
    case maps:get(cleaned, S, false) of
        true ->
            ok;
        false ->
            case
                cleanup(
                    maps:get(connection, S),
                    maps:get(resources, S),
                    maps:get(socket, S),
                    maps:get(network, S)
                )
            of
                ok -> ok;
                Error -> logger:error("Beamlet cleanup failed: ~p", [Error])
            end
    end.
code_change(_, S, _) -> {ok, S}.

format_status(Status) ->
    maps:map(
        fun
            (state, S) -> maps:with([phase, network, resources], S);
            (message, _) -> redacted;
            (log, _) -> [];
            (_, Value) -> Value
        end,
        Status
    ).

start_resolved(Config, From, S) ->
    case maps:size(maps:get(jobs, S)) >= 16 of
        true ->
            {reply, {error, busy}, S};
        false ->
            case target(Config, S) of
                {ok, Target} -> start_container(Config, Target, From, S);
                Error -> {reply, Error, S}
            end
    end.

start_container(Config, Target, From, S) ->
    Name = <<"beamlet-", (binary:encode_hex(crypto:strong_rand_bytes(16), lowercase))/binary>>,
    D = beamlet_deadline:new(maps:get(timeout, Config)),
    Conn = maps:get(connection, S),
    Labels = #{<<"org.beamlet.session">> => maps:get(token, S)},
    Self = self(),
    Network = maps:get(network, S),
    Fun = fun() -> beamlet_runtime:start(Conn, Name, Config, Labels, Self, Network, D) end,
    Resource = #{
        phase => starting, kind => maps:get(kind, Config), target => Target, uncertain => true
    },
    S1 = S#{resources => (maps:get(resources, S))#{Name => Resource}},
    {noreply, job(start, From, Name, Fun, beamlet_deadline:remaining(D), S1)}.

target(#{kind := Kind, target := C}, S) when Kind =:= kafka_ui; Kind =:= dynamodb_ui ->
    {ok, #{session := Session, name := Name, id := Id}} = beamlet:reference(C),
    Expected =
        case Kind of
            kafka_ui -> kafka;
            dynamodb_ui -> dynamodb
        end,
    case {Session =:= self(), maps:get(Name, maps:get(resources, S), missing)} of
        {false, _} ->
            {error, {invalid_target, wrong_session}};
        {true, #{phase := ready, kind := Expected, id := Id}} ->
            {ok, Name};
        {true, #{phase := ready, kind := Other}} when Other =/= Expected ->
            {error, {invalid_target, wrong_kind}};
        _ ->
            {error, {invalid_target, not_ready}}
    end;
target(_, _) ->
    {ok, undefined}.

resource_phase(Name, Phase, S) ->
    Resources = maps:get(resources, S),
    S#{resources => Resources#{Name => (maps:get(Name, Resources))#{phase => Phase}}}.

removal_names(Name, S) ->
    Resources = maps:get(resources, S),
    Dependents = lists:sort([N || {N, #{target := T}} <- maps:to_list(Resources), T =:= Name]),
    Dependents ++ [Name || maps:is_key(Name, Resources)].

stop_startups(Names, S) ->
    lists:foldl(
        fun(Pid, Acc) ->
            J = maps:get(Pid, maps:get(jobs, Acc)),
            Op = maps:get(operation, J),
            case
                lists:member(maps:get(name, J), Names) andalso (Op =:= start orelse is_tuple(Op))
            of
                true ->
                    {Taken, Next} = take_job(Pid, Acc),
                    supervisor:terminate_child(maps:get(sup, Acc), maps:get(id, Taken)),
                    reply(Taken, {error, #{stage => start, reason => target_stopped}}),
                    Next;
                false ->
                    Acc
            end
        end,
        S,
        maps:keys(maps:get(jobs, S))
    ).

next_removal(#{removing := true} = S) ->
    S;
next_removal(#{remove_queue := []} = S) ->
    S;
next_removal(#{remove_queue := [{Name, From} | Rest]} = S) ->
    Conn = maps:get(connection, S),
    Names = removal_names(Name, S),
    Fun = fun() -> remove_group(Conn, Names, beamlet_deadline:new(10000), []) end,
    job(remove, From, Name, Fun, 11000, S#{remove_queue => Rest, removing => true}).

remove_group(_, [], _, Removed) ->
    {ok, Removed};
remove_group(Conn, [Name | Rest], D, Removed) ->
    case beamlet_docker:remove(Conn, Name, D) of
        ok -> remove_group(Conn, Rest, D, [Name | Removed]);
        Error -> {{error, #{stage => cleanup, resources => [{Name, Error}]}}, Removed}
    end.

cleanup_resources(Conn, Resources, D) ->
    {UIs, Targets} = lists:partition(
        fun({_, R}) -> maps:get(target, R) =/= undefined end,
        maps:to_list(Resources)
    ),
    {_, Errors} = lists:foldl(
        fun({Name, _}, {Remaining, Acc}) ->
            Dependencies = [N || {N, #{target := T}} <- maps:to_list(Remaining), T =:= Name],
            Result =
                case Dependencies of
                    [] -> beamlet_docker:remove(Conn, Name, D);
                    _ -> {error, {dependency_cleanup_failed, Dependencies}}
                end,
            case Result of
                ok -> {maps:remove(Name, Remaining), Acc};
                Error -> {Remaining, [{Name, Error} | Acc]}
            end
        end,
        {Resources, []},
        UIs ++ Targets
    ),
    lists:reverse(Errors).
