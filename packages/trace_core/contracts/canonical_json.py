"""Canonical JSON — the one encoding that hashes are computed over.

Two things in this system hash structured data, and they must agree:

* the **dataset digest** (`data.generator.digest`), which is `eval-v1`'s identity;
* the envelope's **`idempotency_key`**, which is what makes "equal keys mean
  equal meaning" true rather than aspirational.

Two implementations of the input to a hash is exactly the kind of duplication
that drifts silently, and when it drifts the symptom is a digest mismatch nobody
can attribute. So the encoding is defined once, here, and both callers import it.

This function was moved out of `data/generator/digest.py` **byte-for-byte**.
`eval-v1`'s recorded digest is a published, frozen artifact
(`eval/track_a/eval-v1.manifest.json`); changing the encoding would move it and
silently invalidate every result that cites it, so any edit here must be
validated by `pytest -m slow tests/unit/test_eval_v1_freeze.py`, which
regenerates all 1,000,000 rows and asserts the digest is unchanged.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Final

SHA256_PREFIX: Final = "sha256:"


def canonical_bytes(row: Any) -> bytes:
    """Sorted keys, no incidental whitespace, UTF-8.

    `sort_keys` matters: a dict literal's insertion order is an implementation
    detail, and letting it into the digest would make an unrelated refactor look
    like a data change.
    """
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode()


def content_hash(value: Any) -> str:
    """`sha256:<hex>` over the canonical encoding of `value`.

    The prefix is part of the format the released envelope schema requires
    (`^sha256:[0-9a-f]{64}$`), so it is produced here rather than bolted on by
    each caller -- a caller that forgot it would produce an event that fails its
    own contract at publish time.
    """
    return SHA256_PREFIX + hashlib.sha256(canonical_bytes(value)).hexdigest()
