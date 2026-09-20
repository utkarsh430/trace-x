"""Where the local lake lives, how its tables and queries are named, and the app ids writes carry.

`TRACE_DELTA_ROOT` (default `./data/lake`) roots every local Delta table and every
streaming checkpoint:

    <root>/<tier>/<table>/                one Delta table per directory (`tables.TableRef`)
    <root>/_checkpoints/<query>/v<N>/     one Spark checkpoint per query version (`checkpoints`)

**A relative root is anchored to the source checkout, never to the working directory.**
`./data/lake` is what `.env.example` ships and what `.gitignore` excludes, at the
repository root. Resolved against the working directory, a job started from
`services/stream/` would write a second lake into `services/stream/data/lake`, which
nothing ignores. Installed without a checkout, a relative root is refused.

**Checkpoints sit beside the tier directories, never inside a table directory.** A
checkpoint inside a table directory is copied, restored and deleted with that table, so
progress and data can be separated by accident -- the state in which an idempotent writer
skips data (see `checkpoints`). The leading underscore keeps a checkpoint out of Spark's
file listing.

**Local paths only.** The same logical tables resolve to Unity Catalog names on
Databricks (`TableRef.uc_name`). A root written as a URI (`s3://`, `dbfs:/`, `file:`) is
refused rather than half-supported: nothing here has been exercised against object-store
listing or rename semantics, and Phase 12 is where that is decided.

**Names are lowercase snake_case -- an open decision.** CLAUDE.md §6 asks for kebab-case
file paths; a table name is also an unquoted Unity Catalog identifier and part of a Delta
app id, where a hyphen needs quoting. The draft ADR records this for a user decision.

Importing this module never imports pyspark or starts a JVM.
"""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from trace_core.domain.errors import LakeConfigError, LakeNameError
from trace_core.observability import get_logger

LAKE_ROOT_ENV: Final = "TRACE_DELTA_ROOT"
DEFAULT_LAKE_ROOT: Final = "./data/lake"
"""Mirrors `.env.example`; anchored to the source checkout (see the module docstring)."""

CHECKPOINTS_DIRNAME: Final = "_checkpoints"
WAREHOUSE_DIRNAME: Final = "_warehouse"
"""Where Spark puts anything it manages without a path: under the lake root, never a
`spark-warehouse` beside whichever directory a job happened to start in."""
UC_CATALOG: Final = "tracex"
"""The Unity Catalog catalog of `docs/DATA_ENGINEERING.md` §8."""

IDENTIFIER: Final = re.compile(r"[a-z][a-z0-9_]{0,63}")
"""Table and query names: lowercase snake_case, starting with a letter."""

APP_ID_PREFIX: Final = "trace-x"

_URI_SCHEME: Final = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_PROJECT_NAME: Final = re.compile(r'^name\s*=\s*"trace-x"\s*$', re.M)
_APP_ID: Final = re.compile(r"trace-x:([a-z][a-z0-9_]{0,63}):v([1-9][0-9]*):([0-9a-f]{32})")

_log = get_logger(__name__)


class Tier(StrEnum):
    """Medallion tiers (ADR-0005). The Unity Catalog schema carries the same name."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


def require_identifier(kind: str, value: str) -> str:
    """Return `value` if it is a valid table or query name, else raise `LakeNameError`."""
    if not IDENTIFIER.fullmatch(value):
        raise LakeNameError(
            f"{kind} {value!r} is not a lowercase snake_case identifier "
            f"(pattern {IDENTIFIER.pattern}); it must be usable unchanged as a directory, a Unity "
            f"Catalog identifier and part of a Delta transaction app id"
        )
    return value


def source_checkout_root() -> Path | None:
    """The TRACE-X repository this module runs from, or None when installed without one."""
    candidate = Path(__file__).resolve().parents[3]
    project = candidate / "pyproject.toml"
    if project.is_file() and _PROJECT_NAME.search(project.read_text(encoding="utf-8")):
        return candidate
    return None


@dataclass(frozen=True, slots=True)
class LakeConfig:
    """The resolved, absolute root of the local lake."""

    root: Path

    def __post_init__(self) -> None:
        if not self.root.is_absolute():
            raise LakeConfigError(
                f"lake root {self.root} is not absolute; build it with LakeConfig.at() or "
                f"LakeConfig.from_env(), which anchor a relative setting to the source checkout"
            )
        if "`" in str(self.root):
            raise LakeConfigError(
                f"lake root {self.root} contains a backquote, which cannot appear inside a "
                f"delta.`<path>` identifier"
            )

    @classmethod
    def at(cls, root: Path | str, *, base: Path | None = None) -> LakeConfig:
        """Resolve `root` once, with symlinks resolved, so a table never has two spellings.

        A relative `root` is anchored to `base`, or by default to the source checkout --
        never to the working directory."""
        text = str(root)
        if _URI_SCHEME.match(text):
            raise LakeConfigError(
                f"lake root {text!r} is a URI; only local paths are supported until Phase 12. "
                f"On Databricks tables are addressed by Unity Catalog name (TableRef.uc_name)"
            )
        path = Path(text).expanduser()
        if not path.is_absolute():
            anchor = base if base is not None else source_checkout_root()
            if anchor is None or not anchor.is_absolute():
                raise LakeConfigError(
                    f"lake root {text!r} is relative and there is no source checkout to anchor "
                    f"it to; set {LAKE_ROOT_ENV} to an absolute path"
                )
            path = anchor / path
        return cls(root=path.resolve())

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        base: Path | None = None,
        log: bool = True,
    ) -> LakeConfig:
        """`TRACE_DELTA_ROOT`, else `./data/lake`; a set-but-blank value is an error.

        The resolved root is logged, so a job's output says where its lake actually is. `log=False`
        is for infrastructure that only derives a path from the root -- the Spark session factory --
        and must not add output to every session a process builds."""
        env = os.environ if environ is None else environ
        raw = env.get(LAKE_ROOT_ENV)
        origin = LAKE_ROOT_ENV
        if raw is None:
            raw, origin = DEFAULT_LAKE_ROOT, "default"
        elif not raw.strip():
            raise LakeConfigError(
                f"{LAKE_ROOT_ENV} is set but blank; unset it to use {DEFAULT_LAKE_ROOT} or set a "
                f"path"
            )
        config = cls.at(raw.strip(), base=base)
        if log:
            _log.info("lake_root_resolved", lake_root=str(config.root), setting=raw, origin=origin)
        return config

    @property
    def checkpoints_root(self) -> Path:
        return self.root / CHECKPOINTS_DIRNAME

    def tier_root(self, tier: Tier) -> Path:
        return self.root / tier.value


def _is_random_uuid(nonce: str) -> bool:
    if not re.fullmatch(r"[0-9a-f]{32}", nonce):
        return False
    value = uuid.UUID(hex=nonce)
    return value.variant == uuid.RFC_4122 and value.version == 4


@dataclass(frozen=True, slots=True)
class AppId:
    """A Delta transaction app id that names its query and checkpoint version.

    `trace-x:<query>:v<N>:<nonce>`, where the nonce is a random (version 4) UUID. An app id
    that does not parse, or whose nonce is not random (an all-zero nonce, a hand-typed one),
    was not minted by `AppId.new` and is not attributed to any query."""

    query: str
    version: int
    nonce: str

    def __post_init__(self) -> None:
        require_identifier("query name", self.query)
        if self.version < 1:
            raise LakeNameError(f"checkpoint version {self.version} is not a positive integer")
        if not _is_random_uuid(self.nonce):
            raise LakeNameError(
                f"app id nonce {self.nonce!r} is not a random (version 4) UUID in hex; app ids "
                f"are minted by AppId.new, never written by hand"
            )

    def __str__(self) -> str:
        return f"{APP_ID_PREFIX}:{self.query}:v{self.version}:{self.nonce}"

    @classmethod
    def new(cls, query: str, version: int) -> AppId:
        return cls(query, version, uuid.uuid4().hex)

    @classmethod
    def parse(cls, text: str) -> AppId | None:
        """None for an app id this project did not mint (another writer's, or Spark's query id)."""
        match = _APP_ID.fullmatch(text)
        if match is None or not _is_random_uuid(match.group(3)):
            return None
        return cls(match.group(1), int(match.group(2)), match.group(3))


__all__ = [
    "APP_ID_PREFIX",
    "CHECKPOINTS_DIRNAME",
    "DEFAULT_LAKE_ROOT",
    "IDENTIFIER",
    "LAKE_ROOT_ENV",
    "UC_CATALOG",
    "AppId",
    "LakeConfig",
    "Tier",
    "require_identifier",
    "source_checkout_root",
]
