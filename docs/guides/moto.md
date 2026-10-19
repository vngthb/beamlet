# S3 and AWS mocks with Moto

Use `beamlet_moto` for local S3 bucket/object tests. The preset runs
`motoserver/moto:5.2.3`, publishes container port 5000 through a Docker-assigned
host port, and waits for an S3 ListBuckets XML response. It uses the normal session
network, deadlines, and cleanup. Data lasts for the lifetime of the container.

[Moto](https://github.com/getmoto/moto) is an Apache-2.0 AWS mock. No AWS account,
real credentials, or vendor token is required. Its
[server mode](https://docs.getmoto.org/en/latest/docs/server_mode.html) exposes
supported AWS APIs through one endpoint; there is no service-selection option.

```erlang
ok = beamlet:with_session(#{}, fun(Session) ->
    {ok, Moto} = beamlet:start_container(Session, beamlet_moto:new()),
    #{endpoint := Endpoint, region := Region,
      access_key_id := AccessKey, secret_access_key := SecretKey,
      force_path_style := true} = beamlet_moto:client_config(Moto),
    %% Translate these values to your S3 client's options and use it here.
    io:format("S3 endpoint: ~s, region: ~s~n", [Endpoint, Region]),
    ok
end).
```

The map is client-neutral; Beamlet does not construct an SDK client. Pass every
connection value explicitly to your client so it uses the mock endpoint and test
credentials. Enable path-style addressing (`http://host:port/bucket/key`). Beamlet
sets Moto's `S3_IGNORE_SUBDOMAIN_BUCKETNAME=true`, so custom hostnames work with
path-style clients. See [Moto's S3 documentation](https://docs.getmoto.org/en/latest/docs/services/s3.html).

## Properties and recipes

| Option | Built-in value | Purpose |
| --- | --- | --- |
| `region` | `<<"us-east-1">>` | Region returned to the client |
| `access_key_id` | `<<"test">>` | Explicit test signing identity |
| `secret_access_key` | `<<"test">>` | Explicit test signing secret |

All three require nonempty binaries without NUL bytes. They configure the client;
they do not enable credential enforcement in Moto. Beamlet reads no ambient AWS
settings. Common preset options (`image`, `timeout`, `pull_policy`, `labels`,
additional `wait`) also apply. Ports, command, and environment remain managed.

Store shared settings in your consumer's `sys.config`:

```erlang
[{beamlet, [{config, #{
    presets => #{moto => #{region => <<"eu-west-1">>}},
    recipes => #{<<"s3_test">> => #{kind => moto, timeout => 60000}}
}}]}].
```

Start it with `beamlet:start_container(Session, #{recipe => <<"s3_test">>})`.
Explicit options override recipes and defaults. See
[configuration](configuration.md) for loading and precedence. For S3 regions
other than `us-east-1`, supply the matching location constraint when creating a
bucket, as your SDK requires.

## Erlang S3 client example

Beamlet's acceptance suite uses integration-only `erlcloud` 3.8.3. A consumer
using erlcloud can add it to its test dependencies and put this adapter in a test
module that includes its header:

```erlang
-include_lib("erlcloud/include/erlcloud_aws.hrl").

s3_client(Container) ->
    #{endpoint := Endpoint, region := Region,
      access_key_id := AccessKey, secret_access_key := SecretKey,
      force_path_style := true} = beamlet_moto:client_config(Container),
    #{host := Host, port := Port} = uri_string:parse(Endpoint),
    #aws_config{
        access_key_id = binary_to_list(AccessKey),
        secret_access_key = binary_to_list(SecretKey),
        aws_region = binary_to_list(Region),
        s3_host = binary_to_list(Host), s3_port = Port,
        s3_scheme = "http://", s3_bucket_access_method = path
    }.
```

Inside the session callback, after starting erlcloud and a default Moto container:

```erlang
Client = s3_client(Moto),
ok = erlcloud_s3:create_bucket("beamlet-example", Client),
_ = erlcloud_s3:put_object("beamlet-example", "hello.txt", <<"hello">>, Client),
Object = erlcloud_s3:get_object("beamlet-example", "hello.txt", Client),
<<"hello">> = proplists:get_value(content, Object).
```

See the complete [acceptance suite](../../test/beamlet_moto_SUITE.erl) for list,
delete, and isolation assertions. No service-client dependency is added at runtime.

## Coverage and limits

The same endpoint, region, and credentials can configure another supported AWS
client; omit the S3-specific path-style setting for that client. Beamlet verifies
S3 bucket/object round trips and isolation, plus DynamoDB ListTables through the
shared endpoint. It does not independently verify every Moto service.

Moto simulates APIs; it does not run the AWS services themselves. Consult its
[implemented services](https://docs.getmoto.org/en/latest/docs/services/index.html)
and service-specific coverage before relying on an operation. Account/region
state follows Moto's rules. Separate containers provide independent mock state.
Cross-service orchestration, AWS fidelity, persistence/reuse, and a dedicated UI
companion are outside this preset's contract.
