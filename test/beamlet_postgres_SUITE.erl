-module(beamlet_postgres_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    round_trip_and_isolation/1,
    failed_start_cleanup/1
]).

suite() -> [{timetrap, {minutes, 3}}].
all() -> [round_trip_and_isolation, failed_start_cleanup].
init_per_suite(Config) ->
    case beamlet_test_support:integration() of
        ok ->
            {ok, _} = application:ensure_all_started(epgsql),
            Config;
        Skip ->
            Skip
    end.
end_per_suite(_) -> ok.

round_trip_and_isolation(_) ->
    beamlet:with_session(#{}, fun(S) ->
        Options = #{
            username => <<"tester">>, password => <<"test-secret">>, database => <<"app_test">>
        },
        {ok, A} = beamlet:start_container(S, beamlet_postgres:new(Options)),
        {ok, B} = beamlet:start_container(S, beamlet_postgres:new(Options)),
        ?assertNotEqual(beamlet_postgres:client_config(A), beamlet_postgres:client_config(B)),
        {ok, ClientA} = connect(beamlet_postgres:client_config(A)),
        {ok, ClientB} = connect(beamlet_postgres:client_config(B)),
        try
            {ok, [], []} = epgsql:squery(ClientA, "CREATE TABLE beamlet_items (value text)"),
            {ok, 1} = epgsql:squery(ClientA, "INSERT INTO beamlet_items VALUES ('hello')"),
            {ok, _, [{<<"hello">>}]} = epgsql:squery(ClientA, "SELECT value FROM beamlet_items"),
            {ok, _, [{null}]} = epgsql:squery(ClientB, "SELECT to_regclass('beamlet_items')"),
            ?assertMatch(
                {error, _}, connect((beamlet_postgres:client_config(A))#{password => <<"wrong">>})
            )
        after
            epgsql:close(ClientA),
            epgsql:close(ClientB)
        end
    end).

failed_start_cleanup(_) ->
    beamlet:with_session(#{}, fun(S) ->
        ?assertMatch(
            {error, _},
            beamlet:start_container(
                S,
                beamlet_postgres:new(#{image => <<"alpine:3.21.3">>, timeout => 1000})
            )
        )
    end).

connect(#{host := Host, port := Port, username := User, password := Password, database := Database}) ->
    epgsql:connect(
        binary_to_list(Host),
        binary_to_list(User),
        binary_to_list(Password),
        [{port, Port}, {database, binary_to_list(Database)}, {timeout, 5000}]
    ).
