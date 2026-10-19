-module(beamlet_config).
-moduledoc false.
-export([snapshot/2, container/2, session/3]).
-export_type([snapshot/0]).

-opaque snapshot() :: #{defaults := map(), recipes := map()}.

-spec snapshot(term(), term()) -> {ok, snapshot()} | {error, map()}.
snapshot(App, Local) ->
    maybe
        ok ?= layer(App, [application]),
        ok ?= layer(Local, [session]),
        Defaults = maps:from_list([
            {Kind,
                merge([
                    maps:get(container, App, #{}),
                    preset(App, Kind),
                    maps:get(container, Local, #{}),
                    preset(Local, Kind)
                ])}
         || Kind <- beamlet_container:kinds()
        ]),
        Recipes = maps:merge(maps:get(recipes, App, #{}), maps:get(recipes, Local, #{})),
        Snapshot = #{defaults => Defaults, recipes => Recipes},
        ok ?=
            entries(
                Recipes,
                fun(Name, Recipe) ->
                    Kind = maps:get(kind, Recipe),
                    Options = maps:merge(maps:get(Kind, Defaults), maps:remove(kind, Recipe)),
                    case Kind =:= generic andalso not maps:is_key(image, Options) of
                        true -> invalid([recipes, Name, image], required);
                        false -> options(Kind, Options, [recipes, Name])
                    end
                end,
                [recipes]
            ),
        {ok, Snapshot}
    end.

-spec container(snapshot(), term()) -> {ok, beamlet_container:config()} | {error, term()}.
container(Snapshot, Input) when is_map(Input) ->
    maybe
        {ok, Recipe} ?= recipe(Snapshot, Input),
        Kind = maps:get(kind, Input, maps:get(kind, Recipe, generic)),
        ok ?= container_kind(Kind, Recipe),
        Defaults = maps:get(Kind, maps:get(defaults, Snapshot)),
        Config = merge([Defaults, Recipe, maps:remove(recipe, Input)]),
        beamlet_container:validate(Config#{kind => Kind})
    end;
container(_, _) ->
    {error, {invalid_config, [config]}}.

-spec session(term(), term(), map()) -> {ok, map(), snapshot()} | {error, term()}.
session(App, Explicit, Env) when is_map(Explicit) ->
    maybe
        ok ?= keys(Explicit, [owner, config | session_keys()], [options]),
        ok ?= session_values(maps:remove(config, Explicit), [options]),
        Local = maps:get(config, Explicit, #{}),
        {ok, Snapshot} ?= snapshot(App, Local),
        Options = merge([
            #{owner => self(), timeout => 120000, ryuk => true},
            maps:get(session, App, #{}),
            maps:get(session, Local, #{}),
            Env,
            maps:remove(config, Explicit)
        ]),
        {ok, Connection} ?= beamlet_docker:resolve(Options),
        Socket = maps:get(docker_socket, Options, maps:get(socket, Connection)),
        {ok, Options#{connection => Connection, docker_socket => Socket}, Snapshot}
    end;
session(_, _, _) ->
    {error, invalid_session_options}.

layer(Config, Path) when is_map(Config) ->
    maybe
        ok ?= keys(Config, [session, container, presets, recipes], Path),
        Session = maps:get(session, Config, #{}),
        ok ?= session_defaults(Session, Path ++ [session]),
        ok ?= common(maps:get(container, Config, #{}), Path ++ [container]),
        ok ?=
            entries(
                maps:get(presets, Config, #{}),
                fun(Kind, Options) ->
                    case lists:member(Kind, beamlet_container:kinds()) of
                        true -> options(Kind, Options, Path ++ [presets, Kind]);
                        false -> invalid(Path ++ [presets], unknown_kind)
                    end
                end,
                Path ++ [presets]
            ),
        entries(
            maps:get(recipes, Config, #{}),
            fun(Name, Recipe) ->
                recipe_definition(Name, Recipe, Path ++ [recipes])
            end,
            Path ++ [recipes]
        )
    end;
layer(_, Path) ->
    invalid(Path, expected_map).

common(Options, Path) ->
    maybe
        ok ?= keys(Options, [timeout, pull_policy, labels, wait], Path),
        options(generic, Options, Path)
    end.

options(Kind, Options, Path) ->
    case beamlet_container:validate_options(Kind, Options) of
        ok -> ok;
        {error, {invalid_config, [Key | _]}} -> invalid(Path ++ [Key], invalid_option)
    end.

recipe_definition(Name, Recipe, Path) when is_map(Recipe) ->
    case
        text(Name) andalso
            lists:member(maps:get(kind, Recipe, undefined), beamlet_container:kinds())
    of
        true -> options(maps:get(kind, Recipe), maps:remove(kind, Recipe), Path ++ [Name]);
        false -> invalid(Path, invalid_recipe)
    end;
recipe_definition(_, _, Path) ->
    invalid(Path, expected_map).

recipe(#{recipes := Recipes}, Input) ->
    case maps:find(recipe, Input) of
        error ->
            {ok, #{}};
        {ok, Name} ->
            case text(Name) andalso maps:is_key(Name, Recipes) of
                true -> {ok, maps:get(Name, Recipes)};
                false -> invalid([recipe], unknown_recipe)
            end
    end.

container_kind(Kind, Recipe) ->
    case lists:member(Kind, beamlet_container:kinds()) of
        false ->
            {error, {invalid_config, [kind]}};
        true ->
            case maps:get(kind, Recipe, Kind) =:= Kind of
                true -> ok;
                false -> invalid([recipe, kind], kind_mismatch)
            end
    end.

session_defaults(Options, Path) ->
    maybe
        ok ?= keys(Options, session_keys(), Path),
        session_values(Options, Path)
    end.

session_keys() -> [timeout, ryuk, docker_host, host, docker_socket, bind_ip].

session_values(Options, Path) ->
    entries(
        Options,
        fun(Key, Value) ->
            case session_value(Key, Value) of
                true -> ok;
                false -> invalid(Path ++ [Key], invalid_option)
            end
        end,
        Path
    ).

session_value(owner, Value) ->
    is_pid(Value);
session_value(timeout, Value) ->
    is_integer(Value) andalso Value > 0;
session_value(ryuk, Value) ->
    is_boolean(Value);
session_value(docker_host, Value) ->
    text(Value) andalso
        case beamlet_docker:resolve(#{docker_host => Value}) of
            {ok, _} -> true;
            _ -> false
        end;
session_value(Key, Value) when Key =:= host; Key =:= bind_ip ->
    case beamlet_docker:resolve(#{Key => Value}) of
        {ok, _} -> true;
        _ -> false
    end;
session_value(docker_socket, Value) when is_list(Value) ->
    Value =/= [] andalso path_characters(Value);
session_value(docker_socket, Value) ->
    text(Value).

path_characters([]) -> true;
path_characters([C | Rest]) when is_integer(C), C > 0, C < 256 -> path_characters(Rest);
path_characters(_) -> false.

keys(Map, Allowed, Path) when is_map(Map) ->
    case maps:size(maps:without(Allowed, Map)) of
        0 -> ok;
        _ -> invalid(Path, unknown_options)
    end;
keys(_, _, Path) ->
    invalid(Path, expected_map).

entries(Map, Fun, _) when is_map(Map) -> check(maps:to_list(Map), Fun);
entries(_, _, Path) -> invalid(Path, expected_map).

check([], _) ->
    ok;
check([{Key, Value} | Rest], Fun) ->
    case Fun(Key, Value) of
        ok -> check(Rest, Fun);
        Error -> Error
    end.

preset(Config, Kind) -> maps:get(Kind, maps:get(presets, Config, #{}), #{}).
merge(Maps) -> lists:foldl(fun(Map, Acc) -> maps:merge(Acc, Map) end, #{}, Maps).
text(Value) ->
    is_binary(Value) andalso byte_size(Value) > 0 andalso
        binary:match(Value, <<0>>) =:= nomatch.
invalid(Path, Reason) -> {error, #{stage => config, path => Path, reason => Reason}}.
