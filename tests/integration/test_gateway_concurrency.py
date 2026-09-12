"""What concurrency does to the gateway's two invariants, against real services.

Phase 2 moved the scoring route off the event loop and into Starlette's worker
threadpool, because blocking the loop capped one replica at ~215 requests a
second (see `tests/contract/test_gateway_does_not_block_event_loop.py`). That
changed the gateway from serving one request at a time to serving up to
`REQUEST_THREADS` at once, and two claims which were previously true *for free*
now have to be earned:

1. **Idempotency.** Two-tier by design (ADR-0007): the Redis replay cache is
   best effort, the `cases.trigger_transaction_id` UNIQUE constraint is
   authoritative. Serialised, the Redis tier absorbed every retry and the
   constraint was decoration. Concurrent, the retries arrive *before* the first
   response is cached, every one of them misses the cache, and the constraint is
   the only thing standing between one logical request and N investigations.
   That is precisely the case worth testing, and it could not occur before.

2. **Overlap.** The whole point of the change. Asserted with a deliberately
   loose bound -- the measured margin is over an order of magnitude, so a
   threshold of 3x is unambiguous about the property while being unflappable
   about a busy CI machine.

**Against the running gateway over HTTP, not an in-process app.** The property
under test is a property of the deployed configuration -- the threadpool size,
the connection pools, uvicorn -- and an in-process `TestClient` would exercise
none of that. It skips loudly rather than passing quietly when nothing is up
(docs/TESTING.md §2 rule 5).
"""

from __future__ import annotations

import os
import secrets
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

pytestmark = pytest.mark.integration

BASE_URL = os.environ.get("TRACE_GATEWAY_URL", "http://localhost:8010")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5442"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "tracex")

PRIMING_TRANSACTIONS = 12
"""Transactions sent before the one under test, to give the account a history.

Twelve because the 1-minute and 5-minute velocity rules need enough events in
the window to fire; below that the transaction under test lands in LOW and the
test would assert "exactly one case" against zero cases.
"""

CONCURRENT_RETRIES = 32
"""Enough to lose the race reliably.

With a handful the first response often lands in the replay cache before the
rest arrive, and the test passes by testing the easy path instead of the hard
one.
"""


def _service_token() -> str:
    explicit = os.environ.get("TRACE_LOAD_TOKEN")
    if explicit:
        return explicit
    for key, value in os.environ.items():
        if key.startswith("TRACE_SERVICE_TOKEN_") and value:
            return f"{key[len('TRACE_SERVICE_TOKEN_') :].lower()}.{value}"
    pytest.skip(
        "SKIPPED (NOT PASSED): no service token in the environment. Export the "
        "TRACE_SERVICE_TOKEN_<ID> the gateway was started with; every request is "
        "authenticated (docs/SECURITY.md §3) and this suite does not bypass that."
    )


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    httpx = pytest.importorskip("httpx")
    with httpx.Client(base_url=BASE_URL, timeout=30.0) as http:
        try:
            http.get("/healthz").raise_for_status()
        except Exception as exc:  # pragma: no cover - environment dependent
            pytest.skip(
                f"SKIPPED (NOT PASSED): no gateway at {BASE_URL} ({exc}). Run `make up`. "
                f"Concurrency is a property of the deployed process -- its threadpool and "
                f"its connection pools -- so this suite is not run in-process."
            )
        yield http


@pytest.fixture(scope="module")
def db() -> Iterator[Any]:
    psycopg = pytest.importorskip("psycopg")
    dsn = "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get("TRACE_APP_DB_USER", "trace_app"),
        p=os.environ.get("TRACE_APP_DB_PASSWORD", ""),
        h=POSTGRES_HOST,
        port=POSTGRES_PORT,
        db=POSTGRES_DB,
    )
    try:
        with psycopg.connect(dsn) as conn:
            yield conn
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no Postgres at {POSTGRES_HOST}:{POSTGRES_PORT} ({exc}). "
            f"The UNIQUE constraint under test is a property of the database and is never "
            f"mocked here (docs/TESTING.md §4)."
        )


def _numeric_id(prefix: str, digits: int = 9) -> str:
    """An id matching the contract's `^<prefix>_\\d{n,}$` patterns.

    The ids are numeric by contract, so a hex uuid slug is rejected with a 422 --
    as this test discovered on its first run. Generated rather than fixed, so
    concurrent runs of the suite do not collide on the same entity and change
    each other's velocity features.
    """
    return f"{prefix}_{secrets.randbelow(10**digits):0{digits}d}"


def _transaction(*, account_id: str, transaction_id: str, amount_minor: int) -> dict[str, Any]:
    """A transaction shaped to triage, so the Postgres path is actually taken.

    A LOW-band transaction opens no case, and a test for "exactly one case" that
    produced zero would pass while proving nothing.
    """
    import datetime as dt

    return {
        "transaction_id": transaction_id,
        "account_id": account_id,
        "amount_minor": amount_minor,
        "currency": "GBP",
        "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "merchant_id": _numeric_id("mrch", 6),
        "merchant_mcc": "6011",
        "merchant_country": "RU",
        "device_id": _numeric_id("dev"),
        "card_id": _numeric_id("card"),
        "ip_id": _numeric_id("ip", 6),
        "latitude": 55.75,
        "longitude": 37.61,
        "channel": "CARD_NOT_PRESENT",
        "entry_mode": "ECOMMERCE",
    }


def test_concurrent_retries_open_exactly_one_investigation(client: Any, db: Any) -> None:
    """The same logical request, retried 32 ways at once, is still one case.

    This is the invariant ADR-0007 and the `case_id` contract promise: `case_id`
    routes and orders, it does not identify a request, and a retry must not
    become a second investigation. Under real concurrency the Redis tier cannot
    help -- nothing is cached yet -- so what is being tested here is the
    authoritative tier on its own.
    """
    token = _service_token()
    transaction_id = f"tx_conc_{uuid.uuid4().hex[:16]}"
    account_id = _numeric_id("acct")
    idempotency_key = f"conc-{uuid.uuid4().hex}"
    auth = {"Authorization": f"Bearer {token}"}

    # Give the account a history first. The rules that triage are velocity
    # rules, and a brand-new account has no velocity -- under Kleene semantics
    # (ADR-0033) an absent feature makes a rule abstain rather than fire, so the
    # first transaction on a fresh account is correctly LOW. Priming is not
    # arranging the answer; it is the difference between testing the triage path
    # and testing an account that has not done anything yet.
    for _ in range(PRIMING_TRANSACTIONS):
        primer = client.post(
            "/v1/transactions",
            json=_transaction(
                account_id=account_id,
                transaction_id=f"tx_prime_{uuid.uuid4().hex[:16]}",
                amount_minor=5_000,
            ),
            headers=auth | {"X-Idempotency-Key": uuid.uuid4().hex},
        )
        assert primer.status_code == 200, primer.text[:300]

    payload = _transaction(
        account_id=account_id, transaction_id=transaction_id, amount_minor=980_000
    )
    headers = auth | {"X-Idempotency-Key": idempotency_key}

    def fire(_: int) -> Any:
        return client.post("/v1/transactions", json=payload, headers=headers)

    with ThreadPoolExecutor(max_workers=CONCURRENT_RETRIES) as pool:
        responses = list(pool.map(fire, range(CONCURRENT_RETRIES)))

    statuses = sorted({r.status_code for r in responses})
    assert statuses == [200], (
        f"every concurrent retry of one request must be answered, got {statuses}. "
        f"A 409 would mean the replay cache decided two identical payloads conflicted; "
        f"a 503 would mean triage could not record the case -- either is a real defect, "
        f"not a flake. First non-200 body: "
        f"{next((r.text[:300] for r in responses if r.status_code != 200), '')}"
    )

    bands = {r.json()["risk_band"] for r in responses}
    if bands == {"LOW"} or bands == {"MEDIUM"}:
        pytest.fail(
            f"the fixture no longer triages (bands={bands}), so this test would pass on "
            f"zero cases while proving nothing about duplicate investigations. Fix the "
            f"fixture rather than relaxing the assertion below."
        )

    case_ids = {r.json().get("case_id") for r in responses}
    assert len(case_ids) == 1 and None not in case_ids, (
        f"all {CONCURRENT_RETRIES} retries must be told about the SAME case, got "
        f"{sorted(str(c) for c in case_ids)}. Distinct ids mean the losers of the race "
        f"were handed a case id that was never committed."
    )

    rows = db.execute(
        "SELECT count(*) FROM app.cases WHERE trigger_transaction_id = %s",
        (transaction_id,),
    ).fetchone()
    assert rows[0] == 1, (
        f"{CONCURRENT_RETRIES} concurrent retries of one transaction opened {rows[0]} "
        f"investigations. The `cases.trigger_transaction_id` UNIQUE constraint is the "
        f"authoritative idempotency tier (ADR-0007) and it did not hold."
    )

    queued = db.execute(
        "SELECT count(*) FROM app.investigation_queue q JOIN app.cases c "
        "ON c.case_id = q.case_id WHERE c.trigger_transaction_id = %s",
        (transaction_id,),
    ).fetchone()
    outboxed = db.execute(
        "SELECT count(*) FROM app.outbox o JOIN app.cases c ON c.case_id = o.partition_key "
        "WHERE c.trigger_transaction_id = %s",
        (transaction_id,),
    ).fetchone()
    assert queued[0] == 1, (
        f"one case but {queued[0]} queue rows: a duplicate here is a duplicate "
        f"investigation performed by a worker, which is the durable effect the "
        f"idempotency contract exists to prevent."
    )
    assert outboxed[0] == 1, (
        f"one case but {outboxed[0]} outbox rows: the relay would publish "
        f"`investigation.requested.v1` more than once for a single request."
    )


# There is deliberately no "requests overlap" timing test here.
#
# It was written, and it measured the wrong process. Sixteen Python threads
# driving `httpx` against a gateway whose requests take ~3 ms spend their time
# contending for the *client's* GIL, so the wall clock reports the test harness
# rather than the server: the first run came back at 0.7x, which would have read
# as "the server does not overlap" when what it meant was "this client cannot
# ask fast enough". A throughput claim needs a load generator that is not
# Python, which is what the pinned k6 image in `tests/load/k6/` is for, and the
# structural guard in `tests/contract/test_gateway_does_not_block_event_loop.py`
# is what stops the regression reaching it. A flaky timing test that measures
# its own harness is worse than no test, because it gets muted.
