"""Prometheus metrics exporter for QTP pipeline health.

Real StreamSets job metrics (records processed, error rate, run duration) pulled through
the same authenticated Control Hub session the whole CLI already uses - not a proxy computed
from the warehouse, and not the SDC engine's own JVM metrics endpoint (which requires DPM/SSO
credentials this project doesn't have; see qtp/metrics/exporter.py for why).
"""
