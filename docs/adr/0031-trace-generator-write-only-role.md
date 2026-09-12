# ADR-0031: `trace_generator` — a write-only ground-truth role

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 1

## Context
ADR-0004 made ground truth **unreachable** rather than hidden: labels, `fraud_pattern` and
`causal_evidence_keys` live in the `groundtruth` schema, `trace_app` has no grant on it at all, and
only `trace_eval` may read it. That covers reading. It says nothing about **writing**, because until
Phase 1 nothing wrote there.

The generator has to. And none of the four existing roles can do it: `trace_app` has no grant,
`trace_eval` holds `SELECT` only, and `trace_stream` and `trace_auditor` are irrelevant. So either a
new role appears, or the seed runs as the migration owner.

Running as the owner is the tempting option — no migration, no new secret, no ADR. It is also the one
that quietly dismantles the control. The owner credential would move into the ordinary development
loop, where it would be exported in shells, pasted into `.env` files and used for debugging. "Who may
write ground truth" would stop being a grant and become a convention about which credential people
happen to use — which is precisely the failure mode ADR-0004 rejected when it chose a database grant
over a naming convention.

## Decision
A fifth role, **`trace_generator`**, created by migration 0002:

| Grant | Value |
|---|---|
| `USAGE` on `groundtruth` | ✅ |
| `INSERT`, `DELETE` on `transaction_labels`, `causal_evidence`, `scenario_instances` | ✅ |
| `SELECT`, `INSERT`, `DELETE` on `groundtruth.datasets` | ✅ — the registry only |
| **`SELECT` on the label tables** | ❌ **denied** |
| Any grant on `app`, `audit`, `eval`, `external` | ❌ none |

**The generator can write ground truth and cannot read the labels it wrote.** That is the property
worth having, and it is stronger than it first looks: combined with ADR-0004 it means **no credential
used in ordinary development can read ground truth** — not the application's, and not the generator's
own. A careless debugging session cannot surface a label, because no credential on the developer's
machine can select one.

`groundtruth.datasets` is readable because the generator must check whether a `dataset_version`
already exists before writing it. The registry holds digests and row counts, never a label.

Three supporting decisions:

* **Writes are one transaction.** A partially-written dataset would let the harness compute metrics
  over a subset while believing it had the whole — a silently wrong number, the category this project
  treats as worse than a crash. Proven by a test that forces a constraint violation mid-write and
  asserts no dataset row survives.
* **`dataset_version` is `UNIQUE`.** Re-seeding an existing version is refused by the database, not by
  an application check, so the refusal holds regardless of which writer runs. `eval-v1` must never be
  regenerated in place (`docs/EVALUATION.md` §2).
* **Label consistency is a `CHECK` constraint** as well as a Python invariant. A fraud label with no
  pattern is unscoreable and a legitimate label with one is a contradiction; the table outlives any
  particular writer, so the rule belongs in the schema too.

**Exactly one module may name the schema.** `data/generator/groundtruth.py` is the only non-migration
file permitted to reference `groundtruth`, asserted by a tokenising structural test that ignores
comments and docstrings — documentation *about* the boundary is encouraged, references *to* it are not.
The database grant remains the real control; this bounds the code that could even attempt a leak to
one reviewable file.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Run the seed as the migration owner (`tracex_owner`) | No new role, no new secret — and it puts an all-privileges credential into the everyday development loop, turning "who may write ground truth" back into a convention. It also gives the seed path implicit read access to every label it writes |
| Grant `trace_eval` INSERT as well as SELECT | One fewer role, and it collapses the reader and the writer into a single credential. The harness would then be able to *modify* the ground truth it is scoring against, which is the one capability an evaluation role must never have |
| Grant `trace_app` write access to `groundtruth` | Directly contradicts ADR-0004's release-blocking test. Not seriously considered; recorded because it is the shortcut someone will eventually propose when a seed script fails |
| Give `trace_generator` SELECT too, for convenience | Would make the writer idempotent by lookup rather than by constraint, and would put a label-reading credential on every developer's machine. The UNIQUE constraint provides the same safety without the capability |
| Keep ground truth in files rather than PostgreSQL | Sidesteps the role question entirely, and loses transactional consistency with the dataset registry plus the ability to express isolation as a grant — the two reasons ADR-0004 chose a database |
| Rely on the structural test alone, with no separate role | A code-level test cannot constrain a credential. The failure mode is a code bug, so the control must sit below the code — ADR-0004's own reasoning |

## Consequences
**Positive.** The write path is least-privilege and the writer cannot read its own output. Isolation is
now verified *with tables present*, which the Phase 0 test could not do because the schema was empty.
Re-seeding and partial writes fail at the database rather than in application logic. The set of code
that could leak a label is one file.

**Negative.** A fifth role and a fifth secret to manage, and `.env.example` grows another entry. Any
harness that runs migrations must supply `TRACE_GENERATOR_DB_PASSWORD` or the upgrade fails — which
surfaced immediately as a broken Phase 0 integration test, and is the intended behaviour: the
migration refuses to create a passwordless role. Debugging the generator is slightly harder, because
the natural "let me just select the labels" step is unavailable by design.

**Risks.** Someone grants `SELECT` to `trace_generator` to make debugging easier, quietly removing the
property this ADR exists for. Mitigated by a test asserting the denial on all three label tables, and
by the deliberately-absent block in the migration saying so in place. A second risk is the structural
test's allow-list growing until it means nothing; it is three files, and each addition is a visible
diff.

## Status
Accepted
