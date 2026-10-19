-module(beamlet_test_support).
-export([integration/0, eventually/2, http_get/3]).

integration() ->
    case os:getenv("BEAMLET_INTEGRATION") of
        "1" ->
            {ok, _} = application:ensure_all_started(beamlet),
            case beamlet_docker:connect(#{}, beamlet_deadline:new(5000)) of
                {ok, _} -> ok;
                Error -> ct:fail({docker_unavailable, Error})
            end;
        _ ->
            {skip,
                "Set BEAMLET_INTEGRATION=1 and use the integration profile for Docker acceptance tests"}
    end.

http_get(C, Port, Path) ->
    {ok, {Host, Mapped}} = beamlet:endpoint(C, Port),
    beamlet_http:request(
        #{transport => {tcp, binary_to_list(Host), Mapped}},
        <<"GET">>,
        Path,
        [],
        <<>>,
        beamlet_deadline:new(15000)
    ).

eventually(Fun, Milliseconds) -> loop(Fun, beamlet_deadline:new(Milliseconds)).
loop(Fun, D) ->
    case Fun() of
        true ->
            ok;
        Last ->
            case beamlet_deadline:remaining(D) of
                0 ->
                    ct:fail({eventually_failed, Last});
                Left ->
                    timer:sleep(min(100, Left)),
                    loop(Fun, D)
            end
    end.
