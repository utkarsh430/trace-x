"""`LPC-5` §4 attributes and §1-§2 frame: every stated boundary, on hand-built rows."""

from __future__ import annotations

from data.generator import outcomes
from data.generator.lpc5 import declaration as d
from data.generator.lpc5.attributes import Table, band, compute_tables, count_bin, decimals
from data.generator.lpc5.fixtures import T0, Rows, account, knowledge
from data.generator.lpc5.frame import Knowledge, build_frame

from trace_core.domain.enums import FraudPattern

A0, A1, A2 = account(0), account(1), account(2)
MIN = 60_000
HOUR = d.HOUR_MS
DAY = d.DAY_MS


def _tables(rows: Rows, know: Knowledge | None = None) -> dict[d.Population, Table]:
    know = know or knowledge()
    return compute_tables(build_frame(rows.rows, seed=know.seed), know)


def _tx(rows: Rows, name: str, know: Knowledge | None = None) -> list[str | None]:
    return _tables(rows, know)[d.Population.TX].column(name).values()


def test_band_and_count_bins_use_their_lower_edges() -> None:
    assert band(4.999, (5, 25), ("<5", "[5,25)", "≥25")) == "<5"
    assert band(5, (5, 25), ("<5", "[5,25)", "≥25")) == "[5,25)"
    assert band(25, (5, 25), ("<5", "[5,25)", "≥25")) == "≥25"
    assert [count_bin(n) for n in (1, 2, 3, 4, 5, 9, 10, 19, 20)] == [
        "1", "2", "3-4", "3-4", "5-9", "5-9", "10-19", "10-19", "20+",
    ]  # fmt: skip
    assert decimals(51.51) == 2
    assert decimals(51.123456) == 6
    assert decimals(1e-7) == 7


def test_counts_include_this_row_and_same_millisecond_rows_and_exclude_the_window_edge() -> None:
    rows = Rows()
    rows.tx(T0 - MIN, A0)  # exactly one minute before: outside (t - 1m, t]
    rows.tx(T0 - MIN + 1, A0)
    rows.tx(T0, A0)
    rows.tx(T0, A0)  # a same-millisecond row counts for both
    counts = _tx(rows, "tx_count_1m")
    assert counts == ["1", "2", "3-4", "3-4"]
    assert counts[2] == count_bin(3)


def test_gap_and_leg_speed_read_the_strictly_earlier_previous_transaction() -> None:
    rows = Rows()
    rows.tx(T0, A0)
    rows.tx(T0, A0)  # a tie is not a previous transaction
    rows.tx(T0 + 5_000, A0, latitude=52.5, longitude=-0.12)
    gap = _tx(rows, "gap_prev")
    speed = _tx(rows, "leg_speed")
    assert gap == ["none", "none", "<10s"]
    assert speed[:2] == ["none", "none"]
    assert speed[2] == "≥1000"  # about 110 km in five seconds


def test_leg_speed_needs_a_previous_transaction_within_a_day() -> None:
    rows = Rows()
    rows.tx(T0, A0)
    rows.tx(T0 + DAY, A0, latitude=40.0, longitude=-3.7)
    assert _tx(rows, "leg_speed") == ["none", "none"]
    assert _tx(rows, "gap_prev")[1] == "≥24h"


def test_location_novel_and_coordinate_repeat_use_only_strictly_earlier_rows() -> None:
    rows = Rows()
    rows.tx(T0, A0, latitude=51.5, longitude=-0.12)
    rows.tx(T0, A0, latitude=51.5, longitude=-0.12)
    rows.tx(T0 + 1, A0, latitude=51.5, longitude=-0.12)
    rows.tx(T0 + 2, A0, latitude=40.4168, longitude=-3.7038)
    assert _tx(rows, "location_novel") == ["no-prior", "no-prior", "<25", "≥500"]
    assert _tx(rows, "coordinate_repeat") == ["new", "new", "repeat", "new"]


def test_device_age_first_use_is_the_first_emitted_row_and_ties_age_zero() -> None:
    rows = Rows()
    rows.tx(T0, A0, device_id="dev_000002")
    rows.tx(T0, A0, device_id="dev_000002")
    rows.tx(T0 + HOUR, A0, device_id="dev_000002")
    rows.tx(T0 + 8 * DAY, A0, device_id="dev_000002")
    assert _tx(rows, "device_age") == ["first-use", "<1h", "[1h,24h)", "≥7d"]
    assert _tx(rows, "device_home") == ["not"] * 4
    assert _tx(rows, "device_account_tx") == ["3-5"] * 4


def test_device_accounts_24h_is_a_half_open_sliding_window_over_accounts() -> None:
    rows = Rows()
    rows.tx(T0, A0, device_id="dev_000002")
    rows.tx(T0 + DAY, A1, device_id="dev_000002")  # A0's row is exactly a day old: outside
    rows.tx(T0 + DAY + 1, A2, device_id="dev_000002")
    assert _tx(rows, "device_accounts_24h") == ["1", "1", "2"]
    assert _tx(rows, "device_accounts") == ["3-5"] * 3


def test_merchant_cv_and_same_amount_accounts_are_exact_integer_windows() -> None:
    rows = Rows()
    for n in range(5):
        rows.tx(T0 + n * HOUR, account(n), merchant_id="mrch_00002", amount_minor=5_000 + n)
    rows.tx(T0 + 5 * HOUR, account(5), merchant_id="mrch_00002", amount_minor=5_300)  # beyond 2 %
    cv = _tx(rows, "merchant_amount_cv_24h")
    same = _tx(rows, "merchant_same_amount_accounts_24h")
    assert cv[0] == "n<2"
    assert cv[1:5] == ["<0.05"] * 4
    assert same[:5] == ["1", "2-4", "2-4", "2-4", "5+"]
    assert same[5] == "1"


def test_prior_events_take_the_most_specific_event_in_the_24h_before() -> None:
    rows = Rows()
    rows.identity(T0 - DAY, A0, "LOGIN_SUCCEEDED")  # exactly 24 h before: inside [t - 24h, t)
    rows.identity(T0 - HOUR, A0, "LOGIN_FAILED")
    rows.tx(T0, A0)
    rows.identity(T0 + 1, A0, "PASSWORD_CHANGE")
    rows.tx(T0 + 1, A0)  # the change at this millisecond is not before it
    rows.tx(T0 + 2, A0)
    tables = _tables(rows)
    prior = tables[d.Population.TX].column("prior_events_24h").values()
    since = tables[d.Population.TX].column("hours_since_identity_change").values()
    failed = tables[d.Population.TX].column("failed_logins_1h").values()
    assert prior == ["failed-login", "failed-login", "identity-change"]
    assert since == ["none", "none", "<1h"]
    assert failed == ["1-4", "0", "0"]


def test_ip_login_accounts_1h_counts_distinct_accounts_strictly_before() -> None:
    rows = Rows()
    for n in range(5):
        rows.identity(T0 - n * MIN - 1, account(n + 10), "LOGIN_FAILED", ip="ip_00009")
    rows.identity(T0, account(20), "LOGIN_FAILED", ip="ip_00009")  # at t: not before
    rows.tx(T0, A0, ip_id="ip_00009")
    assert _tx(rows, "ip_login_accounts_1h") == ["5+"]


def test_prior_decisions_exclude_the_transactions_own_outcome_and_the_window_edges() -> None:
    rows = Rows()
    rows.tx(T0 - HOUR, A0, authorization_outcome="DECLINED")
    rows.tx(T0 - 10 * MIN, A0, authorization_outcome="DECLINED")
    rows.tx(T0, A0, authorization_outcome="APPROVED")
    rows.tx(T0 + 2 * HOUR, A0, authorization_outcome="APPROVED")
    tables = _tables(rows)
    decisions = tables[d.Population.TX].column("prior_decisions_1h").values()
    shares = tables[d.Population.TX].column("prior_declined_share_1h").values()
    assert decisions[0] == "0"
    assert decisions[2] == "2-4"  # the outcome of the row an hour earlier is decided ~0.34 s later
    assert shares[2] == "≥0.4"
    assert decisions[3] == "0"
    assert shares[3] == "none"


def test_outcome_rows_are_derived_with_dm1_when_the_stream_has_none() -> None:
    rows = Rows()
    rows.tx(T0, A0, authorization_outcome="DECLINED")
    rows.tx(T0 + 1, A1, authorization_outcome="UNKNOWN")
    frame = build_frame(rows.rows, seed=42)
    assert frame.outcomes_derived
    (out,) = frame.out
    tx = frame.tx[0]
    assert out.t == outcomes.decided_ms(42, tx.transaction_id, tx.t)
    assert out.envelope.correlation_id == tx.envelope.correlation_id
    assert out.envelope.trace_id == tx.envelope.trace_id
    assert out.group == d.LEGIT
    tables = compute_tables(frame, knowledge())
    out_table = tables[d.Population.OUT]
    assert out_table.column("tx_link").values() == ["one"]
    assert out_table.column("account_match").values() == ["equal"]
    assert out_table.column("transaction_time_match").values() == ["equal"]
    assert out_table.column("timestamp_format").values() == ["ms-z"]
    assert tables[d.Population.TX].column("outcome_event").values() == ["DECLINED", "none"]


def test_dm1_is_a_pure_function_of_the_seed_and_the_transaction_id() -> None:
    first = outcomes.decided_ms(42, "tx_000000000001", T0)
    assert first == outcomes.decided_ms(42, "tx_000000000001", T0)
    assert first - T0 >= outcomes.DM1_FLOOR_MS
    assert (d.DM1_NAMESPACE, d.DM1_FLOOR_MS, d.DM1_MEAN_MS, d.DM1_SD_MS) == (
        outcomes.DM1_NAMESPACE,
        outcomes.DM1_FLOOR_MS,
        outcomes.DM1_MEAN_MS,
        outcomes.DM1_SD_MS,
    )


def test_representation_attributes_see_whole_seconds_shared_ids_and_ties() -> None:
    rows = Rows()
    rows.tx(T0 + 1, A0, envelope={"occurred_at": "2026-01-15T00:00:00.001Z"})
    shared = {"correlation_id": "corr_shared", "trace_id": "f" * 32}
    rows.tx(T0 + 1, A1, envelope=shared)
    rows.tx(T0 + 1_000, A2, envelope={**shared, "occurred_at": "2026-01-15T00:00:01Z"})
    tx = _tables(rows)[d.Population.TX]
    assert tx.column("subsecond").values() == ["fractional", "fractional", "whole"]
    assert tx.column("timestamp_format").values() == ["ms-z", "ms-z", "other"]
    assert tx.column("tie_rank").values() == ["first", "later", "alone"]
    # Each transaction shares its ids with its derived outcome row (one business flow).
    assert tx.column("correlation_members").values() == ["2", "3+", "3+"]
    assert tx.column("event_id_time").values() == ["equal", "equal", "equal"]
    assert tx.column("envelope_unique").values() == ["unique"] * 3


def test_joint_link_needs_one_other_account_sharing_both_a_device_and_an_ip() -> None:
    rows = Rows()
    rows.tx(T0, A0, device_id="dev_000002", ip_id="ip_00002")
    rows.tx(T0 + 1, A1, device_id="dev_000002", ip_id="ip_00003")
    rows.tx(T0 + 2, A2, device_id="dev_000003", ip_id="ip_00002")
    assert _tx(rows, "joint_link") == ["no", "no", "no"]
    rows.tx(T0 + 3, A1, device_id="dev_000009", ip_id="ip_00002")
    assert _tx(rows, "joint_link") == ["yes", "yes", "no", "yes"]


def test_shared_merchant_link_needs_two_linked_accounts_within_seven_days() -> None:
    rows = Rows()
    for n in (1, 2):
        rows.tx(T0 + 6 * DAY, account(n), device_id="dev_000002", merchant_id="mrch_00003")
    rows.tx(T0, A0, device_id="dev_000002", merchant_id="mrch_00003")
    link = _tx(rows, "shared_merchant_link")
    assert link[2] == "yes"
    far = Rows()
    far.tx(T0 + 7 * DAY, A1, device_id="dev_000002", merchant_id="mrch_00003")
    far.tx(T0 + 7 * DAY, A2, device_id="dev_000002", merchant_id="mrch_00003")
    far.tx(T0, A0, device_id="dev_000002", merchant_id="mrch_00003")
    assert _tx(far, "shared_merchant_link")[2] == "no"


def test_side_rows_age_devices_by_first_reference_in_any_stream() -> None:
    rows = Rows()
    rows.identity(
        T0, A0, "PASSWORD_CHANGE", device="dev_000002", pattern=FraudPattern.ACCOUNT_TAKEOVER
    )
    rows.device(T0 + 2 * MIN, A0, "dev_000002", pattern=FraudPattern.ACCOUNT_TAKEOVER)
    rows.identity(T0 + 5 * MIN, A0, "LOGIN_SUCCEEDED", device=None, ip="ip_00005")
    tables = _tables(rows, knowledge(datacenter_ips=frozenset({"ip_00005"})))
    ident = tables[d.Population.ID]
    dev = tables[d.Population.DEV]
    # §4.8 (revision 3): no LOGIN_SUCCEEDED row carries a device, and legitimate logins occur, so
    # the login's device attributes are not applicable rather than `absent`.
    assert ident.column("device_age").values() == ["first-reference", None]
    assert ident.column("device_home").values() == ["not", None]
    assert ident.column("ip_datacenter").values() == ["absent", "datacenter"]
    assert ident.column("ip_login_accounts").values() == ["absent", "1"]
    assert dev.column("device_age").values() == ["<1h"]
    assert dev.column("platform_consistent").values() == ["not"]
    assert ident.groups == [FraudPattern.ACCOUNT_TAKEOVER.value, d.LEGIT]
    assert ident.clusters[0] == "fi_00000001"


def test_amount_attributes_bin_against_the_accounts_own_distribution() -> None:
    rows = Rows()
    rows.tx(T0, A0, amount_minor=100)
    rows.tx(T0 + 1, A1, amount_minor=1_600)
    rows.tx(T0 + 2, A2, amount_minor=40_000)
    tx = _tables(rows)[d.Population.TX]
    assert tx.column("amount_vs_account").values() == ["<0.1", "[0.5,2)", "≥20"]
    assert tx.column("amount_z").values() == ["<-2", "[-0.5,0)", "≥2"]
    assert tx.column("amount_roundness").values() == ["x100", "x100", "x1000"]
    assert tx.column("amount_decile").values() == ["3", "6", "9"]  # edges: nearest-rank legit


def test_an_attribute_no_row_of_its_event_type_provides_is_not_applicable() -> None:
    """Revision 3 §4.8: identity changes carry no IP, legitimate or planted, so IP attributes are
    not judged for them; logins carry one and are. One carrying row makes the type applicable."""
    takeover = FraudPattern.ACCOUNT_TAKEOVER
    rows = Rows()
    rows.tx(T0 - HOUR, A0)
    rows.identity(T0, A0, "LOGIN_FAILED", ip="ip_00009")
    rows.identity(T0 + MIN, A0, "PASSWORD_CHANGE")
    rows.identity(T0 + 2 * MIN, A1, "PASSWORD_CHANGE", pattern=takeover)
    table = _tables(rows)[d.Population.ID]
    assert table.column("ip_datacenter").values() == ["not", None, None]
    assert table.column("ip_login_accounts").values()[1:] == [None, None]
    assert table.column("device_home").values()[1] is not None  # the device is carried

    carried = Rows()
    carried.tx(T0 - HOUR, A0)
    carried.identity(T0, A0, "PASSWORD_CHANGE")
    carried.identity(T0 + MIN, A1, "PASSWORD_CHANGE", pattern=takeover, ip="ip_00009")
    assert _tables(carried)[d.Population.ID].column("ip_datacenter").values() == ["absent", "not"]

    planted_only = Rows()
    planted_only.tx(T0 - HOUR, A0)
    planted_only.identity(T0, A0, "LOGIN_FAILED", ip="ip_00009")
    planted_only.identity(T0 + MIN, A1, "MFA_RESET", pattern=takeover)
    values = _tables(planted_only)[d.Population.ID].column("ip_datacenter").values()
    assert values == ["not", "absent"]  # no legitimate MFA_RESET shows the absence is structural
