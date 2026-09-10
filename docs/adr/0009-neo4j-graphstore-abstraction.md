# ADR-0009: Neo4j with a GraphStore abstraction proven by two adapters

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 5

## Context
The Graph Investigation agent needs variable-length path queries, k-hop neighbourhoods, shared-entity
clusters and community/ring scoring. The brief asks for Neo4j initially with an abstraction allowing
Neptune later.

Two honest complications. First, at the scale this project will reach locally (well under 10 M edges),
Postgres recursive CTEs are likely competitive for 2–3 hop queries — so Neo4j's benefit is a hypothesis,
not a given. Second, **an abstraction with one implementation is not an abstraction**; a `GraphStore`
port with only a Neo4j adapter would be an untested claim about Neptune portability.

## Decision
Define a `GraphStore` port exposing six parameterized, allow-listed queries (never LLM-generated
Cypher), each with a row cap and timeout. Ship **two real adapters from Phase 5**:

- `Neo4jGraphStore` — default, `graph` profile
- `PostgresGraphStore` — recursive CTEs, correct to 3 hops, used when the `graph` profile is off and in CI

Both pass the same `GraphStoreConformanceSuite`. `NeptuneGraphStore` is a third adapter in Phase 12 and
inherits the identical suite.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Neo4j only | Forces the `graph` profile to be running for any graph work, and leaves Neptune portability unproven. Also makes CI depend on a 1 GB-heap container |
| Postgres recursive CTEs only | Loses genuine variable-length path and community-detection expressiveness, and forfeits a real capability demonstration |
| Port with one adapter, "Neptune later" | The untested-claim failure mode this ADR exists to prevent |
| Neo4j GDS enterprise algorithms | Not available in Community edition; would make the local and cloud paths diverge |

## Consequences
**Positive.** Portability is demonstrated rather than asserted. CI runs graph tests without Neo4j.
Neo4j outage degrades to the Postgres adapter with reduced, recorded confidence rather than failing.
**Negative.** Two implementations of six queries to keep in step; the port is constrained to the
intersection of what both can express. Community detection must be implemented in-adapter.
**Risks.** Phase 9's Arm G ablation may show graph evidence changes no outcomes — in which case Neo4j
does not earn its place. That finding would be published, not buried.

## Status
Accepted
