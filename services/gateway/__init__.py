"""`trace-gateway` — the synchronous scoring hot path.

Separate from `trace-api` because it carries a ~100 ms p99 budget and must never
share a thread pool, connection pool or GC pause with a 60-second LLM
investigation (`docs/ARCHITECTURE.md` §2). The application itself is assembled in
`services.gateway.app`; this package holds only wiring.
"""
