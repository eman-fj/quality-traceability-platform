"""Single-command demonstration layer for QTP.

`qtp demo run` orchestrates the whole ISO 9001 story end-to-end in one command:
service preflight -> seed a deterministic "golden thread" scenario -> run the real
compliance suite (defect-rate monitor, 2-hour traceability SLA, shift report) ->
assemble an evidence pack -> print a compliance scorecard.

The demonstration is deliberately decoupled from the live StreamSets engine: it seeds
its scenario straight into the quality warehouse (all rows namespaced ``DEMO-*`` so they
never collide with real pipeline output) and exercises the same SDK scripts a live run
would. That makes the demo deterministic and repeatable — it tells an identical, complete
story whether or not Control Hub and the jobs happen to be reachable. Live overlays
(inject a real Kafka CRITICAL, start Control Hub jobs) are opt-in flags on top.
"""
