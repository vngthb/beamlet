-module(beamlet_container).
-export([
    new/1, new/2,
    validate/1,
    validate_options/2,
    kinds/0,
    docker_config/2, docker_config/3,
    port_key/1
]).
-export_type([spec/0, config/0, kind/0]).

-type kind() :: generic | kafka | dynamodb | postgres | redis | moto | kafka_ui | dynamodb_ui.
-type spec() :: #{
    image => binary(),
    ports => [1..65535],
    environment => #{binary() => binary()},
    command => [binary()],
    labels => #{binary() => binary()},
    timeout => pos_integer(),
    pull_policy => missing | always | never,
    wait => [term()],
    kind => kind(),
    recipe => binary(),
    topics => [binary()],
    username => binary(),
    password => binary(),
    database => binary(),
    region => binary(),
    access_key_id => binary(),
    secret_access_key => binary(),
    target => beamlet:container()
}.
-type config() :: #{
    image := binary(),
    kind := kind(),
    ports := [1..65535],
    environment := #{binary() => binary()},
    command := [binary()],
    labels := #{binary() => binary()},
    timeout := pos_integer(),
    pull_policy := missing | always | never,
    wait := [term()],
    topics := [binary()],
    username => binary(),
    password => binary(),
    database => binary(),
    region => binary(),
    access_key_id => binary(),
    secret_access_key => binary(),
    target => beamlet:container()
}.

-spec new(binary()) -> spec().
new(Image) -> new(Image, #{}).
-spec new(binary(), map()) -> spec().
new(Image, Options) -> Options#{kind => generic, image => Image}.

-spec validate(term()) -> {ok, config()} | {error, term()}.
validate(Config) when is_map(Config) ->
    Kind = maps:get(kind, Config, generic),
    case lists:member(Kind, kinds()) of
        true -> validate_config(Kind, Config);
        false -> {error, {invalid_config, [kind]}}
    end;
validate(_) ->
    {error, {invalid_config, [config]}}.

validate_config(Kind, Config) ->
    Allowed = [
        image,
        ports,
        environment,
        command,
        labels,
        timeout,
        pull_policy,
        wait,
        kind,
        topics,
        username,
        password,
        database,
        region,
        access_key_id,
        secret_access_key,
        target
    ],
    Unknown = maps:keys(maps:without(Allowed, Config)),
    C = maps:merge(
        #{
            ports => [],
            environment => #{},
            command => [],
            labels => #{},
            timeout => 120000,
            pull_policy => missing,
            wait => [],
            kind => generic,
            topics => []
        },
        maps:merge(defaults(Kind, Config), Config)
    ),
    Checks =
        [{unknown_options, Unknown =:= []}] ++
            [
                {Key, valid_value(Key, maps:get(Key, C, undefined))}
             || Key <-
                    [image, ports, environment, labels, command, timeout, pull_policy, topics, wait]
            ] ++
            [{service_options, service_options(C)}, {managed_preset_fields, managed_config(C)}],
    case [Key || {Key, false} <- Checks] of
        [] -> {ok, C#{ports => lists:usort(maps:get(ports, C))}};
        Bad -> {error, {invalid_config, Bad}}
    end.

-doc false.
-spec kinds() -> [kind()].
kinds() -> [generic, kafka, dynamodb, postgres, redis, moto, kafka_ui, dynamodb_ui].

-doc false.
-spec validate_options(kind(), term()) -> ok | {error, term()}.
validate_options(Kind, Options) when is_map(Options) ->
    Common = [image, timeout, pull_policy, labels, wait],
    Specific =
        case Kind of
            generic -> [ports, environment, command];
            kafka -> [topics];
            postgres -> [username, password, database];
            moto -> [region, access_key_id, secret_access_key];
            _ -> []
        end,
    case maps:keys(maps:without(Common ++ Specific, Options)) of
        [] ->
            case [K || {K, V} <- maps:to_list(Options), not valid_value(K, V)] of
                [] -> ok;
                Bad -> {error, {invalid_config, Bad}}
            end;
        _ ->
            {error, {invalid_config, [unknown_options]}}
    end;
validate_options(_, _) ->
    {error, {invalid_config, [config]}}.

valid_value(K, V) when K =:= image; K =:= username; K =:= password; K =:= database -> text(V);
valid_value(K, V) when K =:= region; K =:= access_key_id; K =:= secret_access_key -> text(V);
valid_value(ports, V) -> list(fun(P) -> is_integer(P) andalso P > 0 andalso P < 65536 end, V);
valid_value(environment, V) -> pairs(V, fun env_key/1);
valid_value(labels, V) -> pairs(V, fun text/1);
valid_value(command, V) -> command(V);
valid_value(timeout, V) -> is_integer(V) andalso V > 0;
valid_value(pull_policy, V) -> lists:member(V, [missing, always, never]);
valid_value(topics, V) -> list(fun topic/1, V);
valid_value(wait, V) -> list(fun beamlet_wait:valid/1, V).

defaults(generic, _) -> #{};
defaults(kafka, C) -> beamlet_kafka:defaults(C);
defaults(dynamodb, C) -> beamlet_dynamodb:defaults(C);
defaults(postgres, C) -> beamlet_postgres:defaults(C);
defaults(redis, C) -> beamlet_redis:defaults(C);
defaults(moto, C) -> beamlet_moto:defaults(C);
defaults(kafka_ui, C) -> beamlet_kafka_ui:defaults(C);
defaults(dynamodb_ui, C) -> beamlet_dynamodb_ui:defaults(C).

-spec docker_config(config(), map()) -> map().
docker_config(C, Labels) -> docker_config(C, Labels, <<"127.0.0.1">>).

-doc false.
-spec docker_config(config(), map(), binary()) -> map().
docker_config(C, Labels, BindIP) ->
    Ports = [port_key(P) || P <- maps:get(ports, C)],
    Base = #{
        <<"Image">> => maps:get(image, C),
        <<"Env">> => [
            <<K/binary, "=", V/binary>>
         || {K, V} <- maps:to_list(maps:get(environment, C))
        ],
        <<"Labels">> => maps:merge(maps:get(labels, C), Labels),
        <<"ExposedPorts">> => maps:from_list([{P, #{}} || P <- Ports]),
        <<"HostConfig">> => #{
            <<"PortBindings">> => maps:from_list(
                [{P, [#{<<"HostIp">> => BindIP, <<"HostPort">> => <<>>}]} || P <- Ports]
            )
        }
    },
    case maps:get(command, C) of
        [] -> Base;
        Cmd -> Base#{<<"Cmd">> => Cmd}
    end.

-spec port_key(pos_integer()) -> binary().
port_key(P) -> <<(integer_to_binary(P))/binary, "/tcp">>.

text(B) -> is_binary(B) andalso byte_size(B) > 0 andalso binary:match(B, <<0>>) =:= nomatch.
argument(B) -> is_binary(B) andalso binary:match(B, <<0>>) =:= nomatch.
command([]) -> true;
command([Executable | Args]) -> text(Executable) andalso list(fun argument/1, Args);
command(_) -> false.
env_key(B) -> text(B) andalso binary:match(B, <<"=">>) =:= nomatch.
pairs(M, KeyCheck) when is_map(M) ->
    lists:all(
        fun({K, V}) ->
            KeyCheck(K) andalso is_binary(V) andalso binary:match(V, <<0>>) =:= nomatch
        end,
        maps:to_list(M)
    );
pairs(_, _) ->
    false.
list(_, []) -> true;
list(F, [Head | Tail]) -> F(Head) andalso list(F, Tail);
list(_, _) -> false.
topic(B) when is_binary(B), byte_size(B) > 0, byte_size(B) =< 249 ->
    B =/= <<".">> andalso B =/= <<"..">> andalso
        re:run(B, <<"^[a-zA-Z0-9._-]+$">>, [{capture, none}]) =:= match;
topic(_) ->
    false.
managed_config(#{kind := generic}) ->
    true;
managed_config(#{kind := Kind} = C) when
    Kind =:= kafka;
    Kind =:= dynamodb;
    Kind =:= postgres;
    Kind =:= redis;
    Kind =:= moto;
    Kind =:= kafka_ui;
    Kind =:= dynamodb_ui
->
    Default = defaults(Kind, C),
    Fields = [ports, command, environment],
    lists:all(fun(K) -> maps:get(K, C) =:= maps:get(K, Default, #{}) end, Fields);
managed_config(_) ->
    false.

service_options(C) ->
    case maps:get(kind, C) of
        moto ->
            service_credentials(C) andalso text_options(moto_keys(), C);
        _ ->
            maps:with(moto_keys(), C) =:= #{} andalso service_credentials(C)
    end.

moto_keys() -> [region, access_key_id, secret_access_key].

service_credentials(#{kind := postgres} = C) ->
    not maps:is_key(target, C) andalso
        text_options([username, password, database], C);
service_credentials(#{kind := Kind} = C) when Kind =:= kafka_ui; Kind =:= dynamodb_ui ->
    maps:with([username, password, database], C) =:= #{} andalso
        beamlet:reference(maps:get(target, C, undefined)) =/= error;
service_credentials(C) ->
    maps:with([username, password, database, target], C) =:= #{}.

text_options(Keys, C) -> lists:all(fun(K) -> text(maps:get(K, C, undefined)) end, Keys).
