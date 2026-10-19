-module(beamlet_http).
-export([request/6, authority/2]).
-export_type([connection/0]).
-type connection() :: #{
    transport := {unix, string()} | {tcp, string(), inet:port_number()},
    atom() => term()
}.

-spec request(
    connection(),
    binary(),
    iodata(),
    [{binary(), binary()}],
    iodata(),
    beamlet_deadline:deadline()
) -> {ok, non_neg_integer(), binary()} | {error, term()}.
request(Connection, Method, Path, Headers, Body, Deadline) ->
    case beamlet_deadline:remaining(Deadline) of
        0 ->
            {error, timeout};
        Left ->
            {ok, _} = application:ensure_all_started(gun),
            Options = #{
                protocols => [http],
                retry => 0,
                connect_timeout => Left,
                supervise => false
            },
            Open =
                case maps:get(transport, Connection) of
                    {unix, Socket} -> gun:open_unix(Socket, Options);
                    {tcp, Host, Port} -> gun:open(Host, Port, Options#{transport => tcp})
                end,
            case Open of
                {ok, Pid} ->
                    unlink(Pid),
                    Ref = monitor(process, Pid),
                    try
                        case gun:await_up(Pid, beamlet_deadline:remaining(Deadline), Ref) of
                            {ok, http} ->
                                StreamOpts = #{flow => 1},
                                Stream = gun:request(Pid, Method, Path, Headers, Body, StreamOpts),
                                response(Pid, Stream, Ref, Deadline);
                            {error, Reason} ->
                                {error, Reason}
                        end
                    after
                        gun:close(Pid),
                        demonitor(Ref, [flush]),
                        gun:flush(Pid)
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

response(Pid, Stream, Ref, Deadline) ->
    receive
        {gun_response, Pid, Stream, fin, Status, _} ->
            {ok, Status, <<>>};
        {gun_response, Pid, Stream, nofin, Status, _} ->
            body(Pid, Stream, Ref, Deadline, Status, [], 0);
        {gun_error, Pid, Stream, Reason} ->
            {error, Reason};
        {gun_error, Pid, Reason} ->
            {error, Reason};
        {'DOWN', Ref, process, Pid, Reason} ->
            {error, {connection_closed, Reason}}
    after beamlet_deadline:remaining(Deadline) -> {error, timeout}
    end.
body(Pid, Stream, Ref, Deadline, Status, Acc, Size) ->
    receive
        {gun_data, Pid, Stream, Fin, Data} ->
            NewSize = Size + byte_size(Data),
            case NewSize > 8388608 of
                true ->
                    {error, response_too_large};
                false when Fin =:= fin ->
                    {ok, Status, iolist_to_binary(lists:reverse([Data | Acc]))};
                false ->
                    gun:update_flow(Pid, Stream, 1),
                    body(Pid, Stream, Ref, Deadline, Status, [Data | Acc], NewSize)
            end;
        {gun_trailers, Pid, Stream, _} ->
            {ok, Status, iolist_to_binary(lists:reverse(Acc))};
        {gun_error, Pid, Stream, Reason} ->
            {error, Reason};
        {gun_error, Pid, Reason} ->
            {error, Reason};
        {'DOWN', Ref, process, Pid, Reason} ->
            {error, {connection_closed, Reason}}
    after beamlet_deadline:remaining(Deadline) -> {error, timeout}
    end.

-spec authority(binary(), inet:port_number()) -> binary().
authority(Host, Port) ->
    Address =
        case binary:match(Host, <<":">>) of
            nomatch -> Host;
            _ -> <<"[", Host/binary, "]">>
        end,
    <<Address/binary, ":", (integer_to_binary(Port))/binary>>.
