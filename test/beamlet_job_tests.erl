-module(beamlet_job_tests).
-include_lib("eunit/include/eunit.hrl").

startup_acknowledges_before_work_completes_test() ->
    with_supervisor(fun(Sup) ->
        Parent = self(),
        {Starter, StartRef} = spawn_monitor(fun() ->
            Parent ! {started_job, self(), start_job(Sup, Parent, fun() -> completed end)}
        end),
        Pid =
            receive
                {working, Worker} -> Worker
            after 1000 -> error(no_worker)
            end,
        receive
            {started_job, Starter, {_, Pid}} -> ok
        after 1000 -> error(startup_waited_for_work)
        end,
        receive
            {'DOWN', StartRef, process, Starter, normal} -> ok
        after 1000 -> error(starter_did_not_exit)
        end,
        Ref = monitor(process, Pid),
        Pid ! run,
        expect_result(Pid, completed),
        expect_exit(Pid, Ref, normal)
    end).

worker_failures_return_tagged_errors_test_() ->
    [
        {atom_to_list(Class), fun() ->
            with_supervisor(fun(Sup) ->
                {_, Pid} = start_job(Sup, self(), fun() -> erlang:raise(Class, failed, []) end),
                Ref = monitor(process, Pid),
                Pid ! run,
                expect_result(Pid, {error, #{stage => worker, reason => {Class, failed}}}),
                expect_exit(Pid, Ref, normal)
            end)
        end}
     || Class <- [error, exit, throw]
    ].

cancelled_job_is_removed_without_a_result_test() ->
    with_supervisor(fun(Sup) ->
        {Id, Pid} = start_job(Sup, self(), fun() -> completed end),
        Ref = monitor(process, Pid),
        ok = supervisor:terminate_child(Sup, Id),
        expect_exit(Pid, Ref, killed),
        ?assertEqual([], supervisor:which_children(Sup)),
        expect_no_result(Pid)
    end).

supervisor_shutdown_terminates_pending_job_test() ->
    with_supervisor(fun(Sup) ->
        {_, Pid} = start_job(Sup, self(), fun() -> completed end),
        Ref = monitor(process, Pid),
        ok = gen_server:stop(Sup),
        expect_exit(Pid, Ref, killed),
        expect_no_result(Pid)
    end).

start_job(Sup, Owner, Fun) ->
    Work = fun() ->
        Owner ! {working, self()},
        receive
            run -> Fun()
        after 2000 -> error(work_not_released)
        end
    end,
    Id = make_ref(),
    {ok, Pid} = supervisor:start_child(Sup, #{
        id => Id,
        start => {beamlet_job, start_link, [Owner, Work]},
        restart => temporary,
        shutdown => brutal_kill
    }),
    {Id, Pid}.

expect_result(Pid, Expected) ->
    receive
        {job_done, Pid, Result} -> ?assertEqual(Expected, Result)
    after 1000 -> error(no_job_result)
    end.

expect_exit(Pid, Ref, Reason) ->
    receive
        {'DOWN', Ref, process, Pid, Actual} -> ?assertEqual(Reason, Actual)
    after 1000 -> error(worker_did_not_exit)
    end.

expect_no_result(Pid) ->
    receive
        {job_done, Pid, Result} -> error({unexpected_result, Result})
    after 0 -> ok
    end.

with_supervisor(Fun) ->
    {ok, Sup} = beamlet_job_sup:start_link(),
    try
        Fun(Sup)
    after
        case is_process_alive(Sup) of
            true -> gen_server:stop(Sup);
            false -> ok
        end
    end.
