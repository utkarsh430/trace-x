"""The deterministic rule engine.

Rules are declarative, versioned, individually testable and hot-reloadable
(docs/ARCHITECTURE.md §7). Zero LLM involvement: this is the tier that is always
correct about what it did, even when it is wrong about the answer.
"""

from trace_core.rules.engine import Evaluation, RuleOutcome, evaluate_pack
from trace_core.rules.grammar import (
    AllOf,
    AnyOf,
    Compare,
    Comparison,
    EvalContext,
    InSet,
    Node,
    Not,
    Truth,
)
from trace_core.rules.loader import RulePackLoader, default_loader, parse_pack
from trace_core.rules.pack import CompiledPack, CompiledRule, RulePackModel

__all__ = [
    "AllOf",
    "AnyOf",
    "Compare",
    "Comparison",
    "CompiledPack",
    "CompiledRule",
    "EvalContext",
    "Evaluation",
    "InSet",
    "Node",
    "Not",
    "RuleOutcome",
    "RulePackLoader",
    "RulePackModel",
    "Truth",
    "default_loader",
    "evaluate_pack",
    "parse_pack",
]
