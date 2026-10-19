-module(beamlet_job_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
start_link() -> supervisor:start_link(?MODULE, []).
init([]) -> {ok, {#{strategy => one_for_one, intensity => 3, period => 5}, []}}.
