-module(beamlet_dynamodb_api).
-moduledoc false.
-export([request/4]).

%% Minimal DynamoDB Local probe. No ambient AWS credentials or configuration.
-spec request(map(), binary(), map(), beamlet_deadline:deadline()) -> {ok, map()} | {error, term()}.
request(Client, Operation, Payload, D) ->
    #{scheme := <<"http">>, host := Host, port := Port} = uri_string:parse(
        maps:get(endpoint, Client)
    ),
    Body = iolist_to_binary(json:encode(Payload)),
    Target = <<"DynamoDB_20120810.", Operation/binary>>,
    DateTime = timestamp(),
    <<Date:8/binary, _/binary>> = DateTime,
    Region = maps:get(region, Client),
    Scope = <<Date/binary, "/", Region/binary, "/dynamodb/aws4_request">>,
    Headers = [
        {<<"content-type">>, <<"application/x-amz-json-1.0">>},
        {<<"host">>, beamlet_http:authority(Host, Port)},
        {<<"x-amz-date">>, DateTime},
        {<<"x-amz-target">>, Target}
    ],
    Signed = <<"content-type;host;x-amz-date;x-amz-target">>,
    Canonical = lines([
        <<"POST">>,
        <<"/">>,
        <<>>,
        [[K, $:, V, $\n] || {K, V} <- Headers],
        Signed,
        hash(Body)
    ]),
    ToSign = lines([<<"AWS4-HMAC-SHA256">>, DateTime, Scope, hash(Canonical)]),
    KDate = mac(<<"AWS4", (maps:get(secret_access_key, Client))/binary>>, Date),
    Key = mac(mac(mac(KDate, Region), <<"dynamodb">>), <<"aws4_request">>),
    Signature = binary:encode_hex(mac(Key, ToSign), lowercase),
    Auth =
        <<"AWS4-HMAC-SHA256 Credential=", (maps:get(access_key_id, Client))/binary, "/",
            Scope/binary, ", SignedHeaders=", Signed/binary, ", Signature=", Signature/binary>>,
    case
        beamlet_http:request(
            #{transport => {tcp, binary_to_list(Host), Port}},
            <<"POST">>,
            <<"/">>,
            [{<<"authorization">>, Auth} | Headers],
            Body,
            D
        )
    of
        {ok, 200, Response} ->
            try
                {ok, json:decode(Response)}
            catch
                _:_ -> {error, invalid_dynamodb_response}
            end;
        {ok, Status, Response} ->
            {error, {dynamodb, Status, Response}};
        Error ->
            Error
    end.
hash(Data) -> binary:encode_hex(crypto:hash(sha256, Data), lowercase).
lines(Parts) -> iolist_to_binary(lists:join($\n, Parts)).
mac(Key, Data) -> crypto:mac(hmac, sha256, Key, Data).
timestamp() ->
    {{Y, M, D}, {H, Mi, S}} = calendar:universal_time(),
    iolist_to_binary(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ", [Y, M, D, H, Mi, S])).
