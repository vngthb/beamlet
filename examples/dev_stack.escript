#!/usr/bin/env escript
%% Run from a compiled checkout: escript examples/dev_stack.escript
-mode(compile).

main(_) ->
    Root = filename:dirname(filename:dirname(filename:absname(escript:script_name()))),
    ok = code:add_paths(filelib:wildcard(filename:join([Root, "_build/default/lib/*/ebin"]))),
    Result = beamlet:with_session(#{}, fun(Session) ->
        {ok, Kafka} = beamlet:start_container(Session, beamlet_kafka:new(#{topics => [<<"events">>]})),
        {ok, Dynamo} = beamlet:start_container(Session, beamlet_dynamodb:new()),
        {ok, KafkaUI} = beamlet:start_container(Session, beamlet_kafka_ui:new(Kafka)),
        {ok, DynamoUI} = beamlet:start_container(Session, beamlet_dynamodb_ui:new(Dynamo)),
        io:format("Kafka:       ~s~nDynamoDB:    ~s~nKafka UI:    ~s~nDynamoDB UI: ~s~n", [
            beamlet_kafka:bootstrap_servers(Kafka),
            maps:get(endpoint, beamlet_dynamodb:client_config(Dynamo)),
            beamlet_kafka_ui:url(KafkaUI), beamlet_dynamodb_ui:url(DynamoUI)]),
        io:get_line("Press Enter to stop the stack and remove its containers and network.\n"),
        ok
    end),
    case Result of
        ok -> ok;
        Error -> io:format(standard_error, "Development stack failed: ~p~n", [Error]), halt(1)
    end.
