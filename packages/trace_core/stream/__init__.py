"""The warm path: Kafka, Spark Structured Streaming and the Delta medallion (Phase 3).

Nothing here is imported by the hot path (ADR-0002), and importing this package
never starts a JVM or imports pyspark: `toolchain` is stdlib-only so `make doctor`
can use it before dependencies exist, and `session` imports pyspark only inside the
function that builds a session, after the toolchain has been checked.
"""
