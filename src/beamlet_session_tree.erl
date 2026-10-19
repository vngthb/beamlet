-module(beamlet_session_tree).
-behaviour(supervisor).
-export([start_link/1, init/1]).
start_link(GetOptions) -> supervisor:start_link(?MODULE, GetOptions).
init(GetOptions) ->
    {ok,
        {#{strategy => one_for_one, auto_shutdown => any_significant}, [
            #{
                id => session,
                start => {beamlet_session, start_link, [self(), GetOptions]},
                restart => temporary,
                significant => true,
                shutdown => 15000
            },
            #{
                id => jobs,
                start => {beamlet_job_sup, start_link, []},
                restart => permanent,
                shutdown => infinity,
                type => supervisor
            }
        ]}}.
