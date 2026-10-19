-module(beamlet_dynamodb).
-export([defaults/1, new/0, new/1, client_config/1]).

-spec new() -> beamlet_container:spec().
new() -> new(#{}).
-spec new(map()) -> beamlet_container:spec().
new(Options) -> Options#{kind => dynamodb}.

-doc false.
-spec defaults(map()) -> map().
defaults(_) ->
    #{
        image => <<"amazon/dynamodb-local:3.3.1">>,
        kind => dynamodb,
        ports => [8000],
        command => [<<"-jar">>, <<"DynamoDBLocal.jar">>, <<"-inMemory">>, <<"-sharedDb">>]
    }.

-spec client_config(beamlet:container()) -> map().
client_config(Container) ->
    {ok, {Host, Port}} = beamlet:endpoint(Container, 8000),
    #{
        endpoint => <<"http://", (beamlet_http:authority(Host, Port))/binary>>,
        region => <<"us-east-1">>,
        access_key_id => <<"beamlet">>,
        secret_access_key => <<"beamlet">>
    }.
