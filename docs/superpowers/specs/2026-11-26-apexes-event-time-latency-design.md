# F03.02 — Event-time direct-order latency

Opt-in fixed positive latency for backtest and paper, configured through
launch JSON and registry. Legacy paper wall-clock delay and unrestricted
baseline are unchanged when absent. Live rejects the simulation option.
No sleeps in causal mode. A bounded FIFO (1024, one in-flight intent per
venue/symbol) admits direct strategy orders after current pretrade checks;
on the first observed event at or after the deadline, match after applying
that event, with fresh rule, inventory and risk checks and before the next
strategy decision. Execution time is the observation time, not an
invented exact arrival between observations. In-flight entries are not
exchange working orders. Duplicate intents are refused while queued.
Automatic liquidation/de-risking remains immediate. A paused strategy
makes no new decisions, but in-flight arrivals continue. End of stream
or stop discards queued intents, counted in the report. Record scheduled,
arrived and outstanding counts and delay; arrival checks can reject.
Ignore regressing event timestamps in causal mode; use bar close time
rather than open time. Equal timestamps retain input order. This is a
sampled event-time model, not an independent exchange clock. EMS parent
intents are explicitly refused in this mode until causal child scheduling
is implemented; no silent mixing of wall-time scheduling. Test actual
changed execution prices, risk recheck, end-of-stream, stable FIFO and
launch/report validation. No live execution changes.
