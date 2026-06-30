"""Control Hub job lifecycle for QTP.

A *pipeline* is a definition; a *job* is a running instance of it on an engine. The demo's
deterministic path needs no jobs (it seeds the warehouse directly), but the live path does:
jobs are what actually consume the simulator's Kafka events and write rows to Postgres in
real time. `qtp jobs` creates, starts, stops, and reports those jobs via the SDK.
"""
