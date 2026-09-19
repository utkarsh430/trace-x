"""The murmur2 port places keys exactly where the Java client does.

`partitioner=murmur2_random` is only half of the Java-compatibility claim; the
other half is being able to *predict* a record's partition, which is how the
integration test proves librdkafka and the Java client agree. So the port is
pinned to values the Java implementation produced, not to values it computed
itself:

* Kafka's own `UtilsTest.testMurmur2` vectors (clients/src/test/.../UtilsTest.java);
* further keys run through `org.apache.kafka.common.utils.Utils.murmur2` from the
  verified kafka-clients 3.9.1 jar on Temurin 17 (2026-09-13), chosen to cover
  every tail length (0-3 bytes past a 4-byte block) and a non-ASCII key.

The negative control shows the partitioner choice is observable: librdkafka's
default `consistent_random` (CRC32) disagrees with murmur2 on most keys, so a
producer built with the wrong partitioner cannot pass the integration test.
"""

from __future__ import annotations

import zlib

import pytest

from trace_core.contracts.publish import PRODUCER_CONTRACT, java_partition, murmur2

pytestmark = pytest.mark.unit

KAFKA_UTILS_TEST_VECTORS = {
    b"21": -973932308,
    b"foobar": -790332482,
    b"a-little-bit-long-string": -985981536,
    b"a-little-bit-longer-string": -1486304829,
    b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
    bytes([ord("a"), ord("b"), ord("c")]): 479470107,
}

JAVA_RECORDED = {
    # key -> (Utils.murmur2, toPositive % 6, toPositive % 3), from the Java client.
    b"": (275646681, 3, 0),
    b"4": (1514888353, 1, 1),
    b"34": (-2026295078, 0, 0),
    b"234": (-406844982, 2, 2),
    b"1234": (-1614185708, 0, 0),
    b"kafka": (-798503068, 4, 1),
    b"giberish123456789": (-1890243828, 2, 2),
    b"PreAmbleWillBeRemoved,ThePrePartThatIsa4ByteThing": (-724143892, 4, 1),
    b"acct_000000001": (-1071540212, 0, 0),
    b"dev_000000042": (-1691845284, 2, 2),
    b"case_0123456789abcdef0123456789abcdef": (-1842953864, 0, 0),
    "été".encode(): (-2101193575, 1, 1),
}


@pytest.mark.parametrize("key", list(KAFKA_UTILS_TEST_VECTORS), ids=lambda k: k.decode()[:16])
def test_the_port_matches_kafkas_own_test_vectors(key: bytes) -> None:
    assert murmur2(key) == KAFKA_UTILS_TEST_VECTORS[key]


@pytest.mark.parametrize("key", list(JAVA_RECORDED), ids=lambda k: repr(k)[:20])
def test_the_port_and_partition_match_the_java_client(key: bytes) -> None:
    hashed, mod6, mod3 = JAVA_RECORDED[key]
    assert murmur2(key) == hashed
    assert java_partition(key, 6) == mod6
    assert java_partition(key, 3) == mod3


def test_every_tail_length_is_covered() -> None:
    """The fall-through switch on `length % 4` is where ports go wrong."""
    assert {len(key) % 4 for key in JAVA_RECORDED} == {0, 1, 2, 3}


def test_to_positive_masks_the_sign_bit_rather_than_negating() -> None:
    hashed = KAFKA_UTILS_TEST_VECTORS[b"21"]
    assert hashed < 0
    assert java_partition(b"21", 6) == (hashed & 0x7FFFFFFF) % 6
    # abs() would put this key elsewhere; the mask is what the Java client does.
    assert java_partition(b"21", 6) != abs(hashed) % 6


def test_the_result_is_a_signed_32_bit_integer() -> None:
    for key in [*KAFKA_UTILS_TEST_VECTORS, *JAVA_RECORDED]:
        assert -(2**31) <= murmur2(key) < 2**31


def test_partition_counts_must_be_positive() -> None:
    with pytest.raises(ValueError):
        java_partition(b"acct_000000001", 0)


def test_the_contract_names_the_java_compatible_partitioner() -> None:
    assert PRODUCER_CONTRACT["partitioner"] == "murmur2_random"


def test_crc32_partitioning_would_be_detected() -> None:
    """Negative control: librdkafka's default partitioner puts most keys elsewhere."""
    keys = [f"acct_{i:09d}".encode() for i in range(1_000)]
    disagreements = sum(1 for key in keys if zlib.crc32(key) % 6 != java_partition(key, 6))
    assert disagreements > len(keys) // 2, (
        f"CRC32 and murmur2 agreed on {len(keys) - disagreements} of {len(keys)} keys; the "
        f"partition comparison would not distinguish the two partitioners"
    )
