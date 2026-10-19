-module(beamlet_moto).
-export([defaults/1, new/0, new/1, client_config/1]).

-spec new() -> beamlet_container:spec().
new() -> new(#{}).
-spec new(map()) -> beamlet_container:spec().
new(Options) -> Options#{kind => moto}.

-doc false.
-spec defaults(map()) -> map().
defaults(_) ->
    #{
        image => <<"motoserver/moto:5.2.3">>,
        kind => moto,
        ports => [5000],
        command => [],
        environment => #{
            <<"MOTO_PORT">> => <<"5000">>,
            <<"S3_IGNORE_SUBDOMAIN_BUCKETNAME">> => <<"true">>
        },
        region => <<"us-east-1">>,
        access_key_id => <<"test">>,
        secret_access_key => <<"test">>
    }.

-spec client_config(beamlet:container()) -> map().
client_config(C) ->
    {ok, {Host, Port}} = beamlet:endpoint(C, 5000),
    (beamlet:settings(C))#{
        endpoint => <<"http://", (beamlet_http:authority(Host, Port))/binary>>,
        force_path_style => true
    }.
