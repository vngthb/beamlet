-module(beamlet_deadline).
-export([new/1, remaining/1]).
-export_type([deadline/0]).
-opaque deadline() :: integer().
-spec new(pos_integer()) -> deadline().
new(Milliseconds) -> erlang:monotonic_time(millisecond) + Milliseconds.
-spec remaining(deadline()) -> non_neg_integer().
remaining(Deadline) -> max(0, Deadline - erlang:monotonic_time(millisecond)).
