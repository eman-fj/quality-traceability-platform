"""Builders for the shared fragments QTP owns.

Each module exposes FRAGMENT_NAME, DESCRIPTION, and build(sch, engine_id) -> fragment.
Fragments are parameter-driven (${...}) - no hardcoded buckets, URLs, or credentials.
"""
