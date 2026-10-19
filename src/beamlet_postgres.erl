-module(beamlet_postgres).
-export([defaults/1, new/0, new/1, client_config/1]).

-spec new() -> beamlet_container:spec().
new() -> new(#{}).
-spec new(map()) -> beamlet_container:spec().
new(Options) -> Options#{kind => postgres}.

-doc false.
-spec defaults(map()) -> map().
defaults(Options) ->
    C = maps:merge(
        #{
            image => <<"postgres:17.6-alpine">>,
            kind => postgres,
            username => <<"beamlet">>,
            password => <<"beamlet">>,
            database => <<"beamlet">>,
            ports => [5432],
            command => []
        },
        maps:with([username, password, database], Options)
    ),
    Environment = #{
        <<"POSTGRES_USER">> => maps:get(username, C),
        <<"POSTGRES_PASSWORD">> => maps:get(password, C),
        <<"POSTGRES_DB">> => maps:get(database, C)
    },
    maps:merge(#{environment => Environment}, C).

-spec client_config(beamlet:container()) -> map().
client_config(C) ->
    {ok, {Host, Port}} = beamlet:endpoint(C, 5432),
    (beamlet:settings(C))#{host => Host, port => Port}.
