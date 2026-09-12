"""Deterministic random substreams (ADR-0029).

"Same seed produces the same dataset" is a Phase 1 exit condition, and every
future run manifest cites a dataset digest, so a stream that shifts silently
invalidates every number that ever referenced it.

Two decisions make that hold.

**Stdlib `random.Random`, not `numpy.random.Generator`.** NumPy explicitly
reserves the right to change `Generator`'s stream between versions; CPython's
Mersenne Twister is documented as stable. Stream stability outranks draw speed
here, because a digest that moves on a dependency bump is indistinguishable from
a fabricated one.

**Named substreams, not one shared stream.** With a single stream, every draw
depends on every draw before it, so adding a scenario or reordering a loop
reshuffles unrelated data and changes the digest for no semantic reason. A
substream is seeded from a hash of `(master_seed, namespace, key)`, so
`derive(seed, "amount", "acct_000042")` is the same stream no matter what else
the run did. That is what lets step 7 add ten fraud scenarios without perturbing
the legitimate traffic already generated.

Seeds come from BLAKE2b rather than `hash()`: the builtin is randomised per
process by `PYTHONHASHSEED`, so using it would make the generator
non-reproducible across runs on the same machine.
"""

from __future__ import annotations

import hashlib
import random
from typing import Final

_SEED_BYTES: Final = 8


def substream_seed(master_seed: int, namespace: str, key: str = "") -> int:
    """A stable 64-bit seed for one named substream.

    Stable across processes, machines and Python versions, because it depends
    only on BLAKE2b of the inputs.
    """
    payload = f"{master_seed}\x1f{namespace}\x1f{key}".encode()
    return int.from_bytes(hashlib.blake2b(payload, digest_size=_SEED_BYTES).digest(), "big")


def derive(master_seed: int, namespace: str, key: str = "") -> random.Random:
    """An independent, reproducible stream for `(namespace, key)`.

    Independent in the sense that matters: drawing from one substream never
    advances another, so generation order stops being part of the contract.
    """
    # Non-cryptographic by requirement: this stream must be reproducible, which
    # is the opposite of what a CSPRNG provides.
    return random.Random(substream_seed(master_seed, namespace, key))  # noqa: S311
