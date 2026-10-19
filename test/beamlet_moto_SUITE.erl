-module(beamlet_moto_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    round_trip_and_isolation/1,
    shared_endpoint/1,
    failed_start_cleanup/1
]).

suite() -> [{timetrap, {minutes, 3}}].
all() -> [round_trip_and_isolation, shared_endpoint, failed_start_cleanup].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok ->
            {ok, _} = application:ensure_all_started(erlcloud),
            Config;
        Skip ->
            Skip
    end.
end_per_suite(_) -> ok.

round_trip_and_isolation(_) ->
    beamlet:with_session(#{}, fun(S) ->
        {ok, A} = beamlet:start_container(S, beamlet_moto:new()),
        {ok, B} = beamlet:start_container(S, beamlet_moto:new()),
        ?assertNotEqual(beamlet_moto:client_config(A), beamlet_moto:client_config(B)),
        ClientA = s3_client(A),
        ClientB = s3_client(B),
        Bucket = "beamlet-round-trip",
        Key = "nested/object.bin",
        Data = <<0, 255, "hello S3", 0>>,
        ok = erlcloud_s3:create_bucket(Bucket, ClientA),
        ok = erlcloud_s3:create_bucket(Bucket, ClientB),
        _ = erlcloud_s3:put_object(Bucket, Key, Data, ClientA),
        ?assertEqual(
            Data, proplists:get_value(content, erlcloud_s3:get_object(Bucket, Key, ClientA))
        ),
        [Object] = proplists:get_value(contents, erlcloud_s3:list_objects(Bucket, ClientA)),
        ?assertEqual(Key, proplists:get_value(key, Object)),
        ?assertEqual([], proplists:get_value(contents, erlcloud_s3:list_objects(Bucket, ClientB))),
        _ = erlcloud_s3:delete_object(Bucket, Key, ClientA),
        ?assertEqual([], proplists:get_value(contents, erlcloud_s3:list_objects(Bucket, ClientA))),
        ok = erlcloud_s3:delete_bucket(Bucket, ClientA),
        ok = erlcloud_s3:delete_bucket(Bucket, ClientB)
    end).

shared_endpoint(_) ->
    beamlet:with_session(#{}, fun(S) ->
        {ok, C} = beamlet:start_container(S, beamlet_moto:new(#{region => <<"eu-west-1">>})),
        Settings = beamlet_moto:client_config(C),
        ?assertEqual(<<"eu-west-1">>, maps:get(region, Settings)),
        ?assertMatch(
            {ok, #{<<"TableNames">> := []}},
            beamlet_dynamodb_api:request(
                Settings, <<"ListTables">>, #{}, beamlet_deadline:new(5000)
            )
        )
    end).

failed_start_cleanup(_) ->
    beamlet:with_session(#{}, fun(S) ->
        ?assertMatch(
            {error, _},
            beamlet:start_container(
                S, beamlet_moto:new(#{image => <<"alpine:3.21.3">>, timeout => 1000})
            )
        )
    end).

%% The SDK is integration-only; default EUnit/CT compile without its headers.
-ifdef(BEAMLET_INTEGRATION).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
s3_client(C) ->
    #{
        endpoint := Endpoint,
        region := Region,
        access_key_id := AccessKey,
        secret_access_key := SecretKey,
        force_path_style := true
    } = beamlet_moto:client_config(C),
    #{host := Host, port := Port} = uri_string:parse(Endpoint),
    #aws_config{
        access_key_id = binary_to_list(AccessKey),
        secret_access_key = binary_to_list(SecretKey),
        aws_region = binary_to_list(Region),
        s3_host = binary_to_list(Host),
        s3_port = Port,
        s3_scheme = "http://",
        s3_bucket_access_method = path
    }.
-else.
s3_client(_) -> error(integration_profile_required).
-endif.
