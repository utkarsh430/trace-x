"""Loading rule packs, and reloading them without ever serving a broken one.

**Hot reload is the requirement; keeping the last good pack is what makes it
safe.** ROADMAP Phase 2 asks for hot-reloadable rules. The failure mode that
matters is not "the new pack is wrong" — review catches most of that — but "the
new pack is *invalid*, and the reloading process now has no rules at all". A
gateway scoring every transaction zero is worse than a gateway running slightly
stale rules, because it looks exactly like a quiet day.

So reload is **atomic and fail-safe**: the candidate is parsed, validated and
compiled in full before anything is swapped, and any failure leaves the running
pack untouched, is counted, and is reported. There is deliberately no partial
load — a pack with one bad rule does not load nineteen good ones, because the
resulting behaviour would match neither what was reviewed nor what was intended.

**The digest is what a decision cites**, so the swap and the digest move
together: `active()` returns one object carrying both, and a scorer that read the
rules and the digest in two separate calls could straddle a reload and report a
digest that never produced that decision.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import yaml

from trace_core.domain.errors import ContractError
from trace_core.rules.pack import CompiledPack, RulePackModel

DEFAULT_PACK: Final = Path(__file__).resolve().parent / "packs" / "core.v1.yaml"


@dataclass(frozen=True, slots=True)
class ReloadOutcome:
    """What a reload attempt did. Returned rather than logged-and-forgotten, so
    a caller can increment the right counter and surface the reason."""

    loaded: bool
    digest: str
    previous_digest: str | None = None
    error: str | None = None

    @property
    def changed(self) -> bool:
        return self.loaded and self.digest != self.previous_digest


def parse_pack(text: str, *, known_features: frozenset[str]) -> CompiledPack:
    """Parse and compile YAML into a validated pack, or raise.

    `yaml.safe_load` rather than `yaml.load`: the full loader can construct
    arbitrary Python objects, which would reintroduce exactly the code-execution
    surface the closed grammar exists to remove (ADR-0033).
    """
    try:
        raw: Any = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ContractError(f"rule pack is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ContractError(
            f"rule pack must be a mapping at the top level, got {type(raw).__name__}"
        )
    try:
        model = RulePackModel.model_validate(raw)
    except Exception as exc:
        raise ContractError(f"rule pack failed validation: {exc}") from exc
    return CompiledPack(model, known_features=known_features)


class RulePackLoader:
    """Holds the active pack and swaps it atomically.

    Thread-safe because the gateway serves concurrently: a reload must not be
    visible half-applied, and two simultaneous reloads must not interleave.
    """

    def __init__(self, path: Path, *, known_features: frozenset[str]) -> None:
        self._path = path
        self._known_features = known_features
        self._lock = threading.Lock()
        self._pack: CompiledPack | None = None
        self._failed_reloads = 0

    @property
    def path(self) -> Path:
        return self._path

    @property
    def failed_reloads(self) -> int:
        """Counted, not merely logged: a pack that keeps failing to reload is an
        operational condition someone has to see (`rule_pack_reload_failed_total`)."""
        return self._failed_reloads

    def load(self) -> CompiledPack:
        """Load for the first time. Failure here is fatal, by design.

        There is no previous pack to fall back to, and starting with no rules
        would mean serving traffic that is scored by nothing at all -- the same
        reasoning that makes a corrupt model artifact refuse to boot
        (docs/ARCHITECTURE.md §18).
        """
        with self._lock:
            pack = parse_pack(
                self._path.read_text(encoding="utf-8"), known_features=self._known_features
            )
            self._pack = pack
            return pack

    def active(self) -> CompiledPack:
        """The pack currently in force, loading it on first use."""
        pack = self._pack
        if pack is None:
            return self.load()
        return pack

    def reload(self) -> ReloadOutcome:
        """Re-read and swap, or keep what is running and say why.

        The whole candidate is compiled before the swap, so a failure cannot
        leave a half-applied pack. The running pack is replaced only by a fully
        valid successor.
        """
        with self._lock:
            previous = self._pack
            previous_digest = previous.digest if previous is not None else None
            try:
                candidate = parse_pack(
                    self._path.read_text(encoding="utf-8"), known_features=self._known_features
                )
            except (OSError, ContractError) as exc:
                self._failed_reloads += 1
                if previous is None:
                    raise
                return ReloadOutcome(
                    loaded=False,
                    digest=previous.digest,
                    previous_digest=previous_digest,
                    error=str(exc),
                )
            self._pack = candidate
            return ReloadOutcome(
                loaded=True, digest=candidate.digest, previous_digest=previous_digest
            )


def default_loader(known_features: frozenset[str]) -> RulePackLoader:
    return RulePackLoader(DEFAULT_PACK, known_features=known_features)
