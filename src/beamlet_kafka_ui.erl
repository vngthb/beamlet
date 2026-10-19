-module(beamlet_kafka_ui).
-export([defaults/1, new/1, new/2, url/1]).

-spec new(beamlet:container()) -> beamlet_container:spec().
new(Target) -> new(Target, #{}).
-spec new(beamlet:container(), map()) -> beamlet_container:spec().
new(Target, Options) -> Options#{kind => kafka_ui, target => Target}.

-doc false.
-spec defaults(map()) -> map().
defaults(Options) ->
    Target = maps:get(target, Options, undefined),
    Alias = beamlet:private_host(Target),
    #{
        kind => kafka_ui,
        image => <<"ghcr.io/kafbat/kafka-ui:v1.4.2">>,
        target => Target,
        ports => [8080],
        command => [],
        environment => #{
            <<"KAFKA_CLUSTERS_0_NAME">> => <<"beamlet">>,
            <<"KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS">> => <<Alias/binary, ":9093">>,
            <<"DYNAMIC_CONFIG_ENABLED">> => <<"false">>
        }
    }.

-spec url(beamlet:container()) -> binary().
url(C) ->
    {ok, {Host, Port}} = beamlet:endpoint(C, 8080),
    <<"http://", (beamlet_http:authority(Host, Port))/binary>>.
