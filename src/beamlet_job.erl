-module(beamlet_job).
-export([start_link/2, init/2]).

start_link(Owner, Fun) -> proc_lib:start_link(?MODULE, init, [Owner, Fun]).

init(Owner, Fun) ->
    proc_lib:init_ack({ok, self()}),
    Result =
        try
            Fun()
        catch
            Class:Reason -> {error, #{stage => worker, reason => {Class, Reason}}}
        end,
    Owner ! {job_done, self(), Result}.
