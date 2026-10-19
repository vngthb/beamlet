-module(beamlet_runtime).
-export([start/7, handle/4, stage/2]).

-spec start(
    beamlet_docker:connection(),
    binary(),
    beamlet_container:config(),
    map(),
    pid(),
    binary(),
    beamlet_deadline:deadline()
) -> {ok, beamlet:container()} | {error, map()}.
start(Conn, Name, Config, Labels, Session, Network, D) ->
    BindIP = maps:get(bind_ip, Conn, <<"127.0.0.1">>),
    Body = (beamlet_container:docker_config(Config, Labels, BindIP))#{
        <<"NetworkingConfig">> => #{
            <<"EndpointsConfig">> => #{Network => #{<<"Aliases">> => [Name]}}
        }
    },
    maybe
        ok ?= check_target(Conn, Config, D),
        ok ?=
            stage(
                pull,
                beamlet_docker:ensure_image(
                    Conn, maps:get(image, Config), maps:get(pull_policy, Config), D
                )
            ),
        {ok, Id} ?= stage(create, beamlet_docker:create(Conn, Name, Body, D)),
        ok ?= stage(start, beamlet_docker:start(Conn, Id, D)),
        {ok, Info} ?= stage(inspect, beamlet_docker:inspect(Conn, Id, D)),
        ok ?= running(Info),
        C = beamlet:with_settings(
            handle(Conn, Name, Session, Info),
            maps:with(
                [username, password, database, region, access_key_id, secret_access_key], Config
            )
        ),
        ok ?= ready(maps:get(kind, Config), Conn, Name, C, Config, D),
        ok ?= stage(wait, beamlet_wait:run(maps:get(wait, Config), Conn, Name, C, D)),
        {ok, C}
    end.

running(#{<<"State">> := #{<<"Running">> := true}}) -> ok;
running(Info) -> stage(inspect, {error, {container_exited, maps:get(<<"State">>, Info)}}).

ready(kafka, Conn, Name, C, Config, D) ->
    Bootstrap = beamlet_kafka:bootstrap_servers(C),
    Script =
        <<"#!/bin/bash\nset -e\nexport KAFKA_ADVERTISED_LISTENERS='EXTERNAL://", Bootstrap/binary,
            ",INTERNAL://", Name/binary, ":9093'\nexec /etc/kafka/docker/run\n">>,
    Ready =
        {command, [
            <<"/opt/kafka/bin/kafka-broker-api-versions.sh">>,
            <<"--bootstrap-server">>,
            <<"localhost:9093">>
        ]},
    maybe
        ok ?=
            stage(
                kafka_config,
                beamlet_docker:upload(
                    Conn, beamlet:id(C), <<"beamlet-start.sh">>, Script, D
                )
            ),
        ok ?= stage(kafka_ready, beamlet_wait:run([Ready], Conn, Name, C, D)),
        create_topics(maps:get(topics, Config), Conn, beamlet:id(C), D)
    end;
ready(dynamodb, Conn, Name, C, _, D) ->
    stage(dynamodb_ready, beamlet_wait:run([dynamodb], Conn, Name, C, D));
ready(moto, Conn, Name, C, _, D) ->
    stage(moto_ready, beamlet_wait:run([moto], Conn, Name, C, D));
ready(postgres, Conn, Name, C, _, D) ->
    Ready =
        {command, [
            <<"/bin/sh">>,
            <<"-c">>,
            <<
                "PGPASSWORD=\"$POSTGRES_PASSWORD\" exec psql --host 127.0.0.1 "
                "--username \"$POSTGRES_USER\" --dbname \"$POSTGRES_DB\" "
                "--no-password --set ON_ERROR_STOP=1 --command 'SELECT 1'"
            >>
        ]},
    stage(postgres_ready, beamlet_wait:run([Ready], Conn, Name, C, D));
ready(redis, Conn, Name, C, _, D) ->
    stage(redis_ready, beamlet_wait:run([redis], Conn, Name, C, D));
ready(kafka_ui, Conn, Name, C, _, D) ->
    stage(
        kafka_ui_ready,
        beamlet_wait:run(
            [{http, 8080, <<"/">>, 200}, kafka_ui], Conn, Name, C, D
        )
    );
ready(dynamodb_ui, Conn, Name, C, _, D) ->
    stage(
        dynamodb_ui_ready,
        beamlet_wait:run(
            [{http, 8001, <<"/">>, 200}, dynamodb_ui], Conn, Name, C, D
        )
    );
ready(generic, _, _, _, _, _) ->
    ok.

create_topics([], _, _, _) ->
    ok;
create_topics([Topic | Rest], Conn, Id, D) ->
    Args = [
        <<"/opt/kafka/bin/kafka-topics.sh">>,
        <<"--bootstrap-server">>,
        <<"localhost:9093">>,
        <<"--create">>,
        <<"--if-not-exists">>,
        <<"--topic">>,
        Topic,
        <<"--partitions">>,
        <<"1">>,
        <<"--replication-factor">>,
        <<"1">>
    ],
    case beamlet_docker:exec(Conn, Id, Args, D) of
        {ok, 0, _} -> create_topics(Rest, Conn, Id, D);
        Other -> stage(kafka_topic, {error, Other})
    end.

check_target(Conn, #{target := Target}, D) ->
    {ok, #{name := Name, id := Id}} = beamlet:reference(Target),
    case beamlet_docker:inspect(Conn, Name, D) of
        {ok, #{<<"Id">> := Id, <<"State">> := #{<<"Running">> := true}}} -> ok;
        {ok, _} -> stage(target, {error, {invalid_target, not_running}});
        Error -> stage(target, Error)
    end;
check_target(_, _, _) ->
    ok.

-spec handle(beamlet_docker:connection(), binary(), pid(), map()) -> beamlet:container().
handle(Conn, Name, Session, #{<<"Id">> := Id, <<"NetworkSettings">> := #{<<"Ports">> := Ports}}) ->
    Mapped = maps:fold(
        fun(Key, Bindings, Acc) ->
            case {binary:split(Key, <<"/">>), Bindings} of
                {[P, <<"tcp">>], [#{<<"HostPort">> := HostPort} | _]} ->
                    Acc#{binary_to_integer(P) => binary_to_integer(HostPort)};
                _ ->
                    Acc
            end
        end,
        #{},
        Ports
    ),
    beamlet:make_container(Id, Name, Session, maps:get(host, Conn), Mapped).

-spec stage(atom(), ok | {ok, Value} | {error, term()}) ->
    ok | {ok, Value} | {error, map()}.
stage(Stage, {error, Reason}) -> {error, #{stage => Stage, reason => Reason}};
stage(_, Result) -> Result.
