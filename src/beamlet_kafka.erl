-module(beamlet_kafka).
-export([defaults/1, new/0, new/1, bootstrap_servers/1]).

-spec new() -> beamlet_container:spec().
new() -> new(#{}).
-spec new(map()) -> beamlet_container:spec().
new(Options) -> Options#{kind => kafka}.

-doc false.
-spec defaults(map()) -> map().
defaults(_) ->
    #{
        image => <<"apache/kafka:3.9.1">>,
        kind => kafka,
        ports => [9092],
        timeout => 120000,
        environment => #{
            <<"CLUSTER_ID">> => <<"beamletClusterId0000000">>,
            <<"KAFKA_NODE_ID">> => <<"1">>,
            <<"KAFKA_PROCESS_ROLES">> => <<"broker,controller">>,
            <<"KAFKA_LISTENERS">> =>
                <<"EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094">>,
            <<"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP">> =>
                <<"EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT">>,
            <<"KAFKA_INTER_BROKER_LISTENER_NAME">> => <<"INTERNAL">>,
            <<"KAFKA_CONTROLLER_LISTENER_NAMES">> => <<"CONTROLLER">>,
            <<"KAFKA_CONTROLLER_QUORUM_VOTERS">> => <<"1@localhost:9094">>,
            <<"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR">> => <<"1">>,
            <<"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR">> => <<"1">>,
            <<"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR">> => <<"1">>,
            <<"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS">> => <<"0">>
        },
        command => [
            <<"/bin/bash">>,
            <<"-c">>,
            <<
                "while [ ! -f /tmp/beamlet-start.sh ]; do sleep 0.1; done; "
                "exec /bin/bash /tmp/beamlet-start.sh"
            >>
        ]
    }.

-spec bootstrap_servers(beamlet:container()) -> binary().
bootstrap_servers(Container) ->
    {ok, {Host, Port}} = beamlet:endpoint(Container, 9092),
    beamlet_http:authority(Host, Port).
