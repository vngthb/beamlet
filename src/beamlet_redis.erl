-module(beamlet_redis).
-export([defaults/1, new/0, new/1, client_config/1]).

-spec new() -> beamlet_container:spec().
new() -> new(#{}).
-spec new(map()) -> beamlet_container:spec().
new(Options) -> Options#{kind => redis}.

-doc false.
-spec defaults(map()) -> map().
defaults(_) ->
    #{
        image => <<"redis:7.4.11-alpine">>,
        kind => redis,
        ports => [6379],
        environment => #{},
        command => [
            <<"redis-server">>,
            <<"--save">>,
            <<>>,
            <<"--appendonly">>,
            <<"no">>
        ]
    }.

-spec client_config(beamlet:container()) -> map().
client_config(C) ->
    {ok, {Host, Port}} = beamlet:endpoint(C, 6379),
    #{host => Host, port => Port}.
