-module(beamlet_dynamodb_ui).
-export([defaults/1, new/1, new/2, url/1]).

-spec new(beamlet:container()) -> beamlet_container:spec().
new(Target) -> new(Target, #{}).
-spec new(beamlet:container(), map()) -> beamlet_container:spec().
new(Target, Options) -> Options#{kind => dynamodb_ui, target => Target}.

-doc false.
-spec defaults(map()) -> map().
defaults(Options) ->
    Target = maps:get(target, Options, undefined),
    Alias = beamlet:private_host(Target),
    #{
        kind => dynamodb_ui,
        image => <<"aaronshaf/dynamodb-admin:5.3.4">>,
        target => Target,
        ports => [8001],
        command => [],
        environment => #{
            <<"HOST">> => <<"0.0.0.0">>,
            <<"DYNAMO_ENDPOINT">> => <<"http://", Alias/binary, ":8000">>,
            <<"AWS_REGION">> => <<"us-east-1">>,
            <<"AWS_ACCESS_KEY_ID">> => <<"beamlet">>,
            <<"AWS_SECRET_ACCESS_KEY">> => <<"beamlet">>
        }
    }.

-spec url(beamlet:container()) -> binary().
url(C) ->
    {ok, {Host, Port}} = beamlet:endpoint(C, 8001),
    <<"http://", (beamlet_http:authority(Host, Port))/binary>>.
