# Online feature store — distinct-cardinality benchmark

> Evidence for ADR-0034. Every number below comes from the run recorded as
> `run_id: bench-20260912-cardinality-41059ab1`, which `make check-claims` resolves.

**What is being compared.** Two representations of an event-time sliding
distinct count: a sorted set keyed by the counted value (`ZADD … GT`,
`ZCOUNT` over the window) and one HyperLogLog per five-minute bucket unioned
by `PFCOUNT`. Both were driven with identical inputs at each cardinality.

Redis 7.4.11. Each read figure is the median and p99 of
200 reads; update cost is wall-clock per event over a pipelined load.

| window | cardinality | repr | counted | rel. error | memory | read p50 | read p99 | update/event |
|---|---|---|---|---|---|---|---|---|
| 300s | 10 | EXACT | 10 | 0.00% | 0.3 KB | 0.152 ms | 0.425 ms | 0.2231 ms |
| 300s | 10 | APPROXIMATE | 10 | 0.00% | 0.2 KB | 0.123 ms | 0.157 ms | 0.0322 ms |
| 300s | 100 | EXACT | 100 | 0.00% | 2.6 KB | 0.122 ms | 0.137 ms | 0.0085 ms |
| 300s | 100 | APPROXIMATE | 100 | 0.00% | 0.4 KB | 0.120 ms | 0.176 ms | 0.0059 ms |
| 300s | 1,000 | EXACT | 1,000 | 0.00% | 90.0 KB | 0.119 ms | 0.208 ms | 0.0057 ms |
| 300s | 1,000 | APPROXIMATE | 1,004 | 0.40% | 2.1 KB | 0.104 ms | 0.126 ms | 0.0041 ms |
| 300s | 5,000 | EXACT | 5,000 | 0.00% | 487.4 KB | 0.109 ms | 0.141 ms | 0.0047 ms |
| 300s | 5,000 | APPROXIMATE | 4,984 | 0.32% | 14.1 KB | 0.106 ms | 0.131 ms | 0.0039 ms |
| 300s | 20,000 | EXACT | 20,000 | 0.00% | 2,009.8 KB | 0.109 ms | 0.125 ms | 0.0050 ms |
| 300s | 20,000 | APPROXIMATE | 19,789 | 1.05% | 14.1 KB | 0.107 ms | 0.131 ms | 0.0040 ms |
| 300s | 50,000 | EXACT | 50,000 | 0.00% | 4,809.7 KB | 0.133 ms | 0.187 ms | 0.0050 ms |
| 300s | 50,000 | APPROXIMATE | 49,513 | 0.97% | 14.1 KB | 0.131 ms | 0.159 ms | 0.0039 ms |
| 3600s | 10 | EXACT | 10 | 0.00% | 0.3 KB | 0.108 ms | 0.185 ms | 0.0233 ms |
| 3600s | 10 | APPROXIMATE | 10 | 0.00% | 0.8 KB | 0.079 ms | 0.119 ms | 0.0147 ms |
| 3600s | 100 | EXACT | 100 | 0.00% | 2.6 KB | 0.108 ms | 0.145 ms | 0.0079 ms |
| 3600s | 100 | APPROXIMATE | 100 | 0.00% | 1.7 KB | 0.116 ms | 0.127 ms | 0.0056 ms |
| 3600s | 1,000 | EXACT | 1,000 | 0.00% | 105.7 KB | 0.106 ms | 0.125 ms | 0.0050 ms |
| 3600s | 1,000 | APPROXIMATE | 1,004 | 0.40% | 5.3 KB | 0.137 ms | 0.160 ms | 0.0040 ms |
| 3600s | 5,000 | EXACT | 5,000 | 0.00% | 542.1 KB | 0.121 ms | 0.146 ms | 0.0046 ms |
| 3600s | 5,000 | APPROXIMATE | 4,984 | 0.32% | 12.8 KB | 0.157 ms | 0.179 ms | 0.0039 ms |
| 3600s | 20,000 | EXACT | 20,000 | 0.00% | 2,072.3 KB | 0.120 ms | 0.197 ms | 0.0046 ms |
| 3600s | 20,000 | APPROXIMATE | 19,789 | 1.05% | 80.8 KB | 0.217 ms | 0.400 ms | 0.0037 ms |
| 3600s | 50,000 | EXACT | 50,000 | 0.00% | 4,965.9 KB | 0.142 ms | 0.241 ms | 0.0047 ms |
| 3600s | 50,000 | APPROXIMATE | 49,513 | 0.97% | 168.8 KB | 0.400 ms | 0.546 ms | 0.0037 ms |
| 86400s | 10 | EXACT | 10 | 0.00% | 0.3 KB | 0.143 ms | 0.203 ms | 0.0218 ms |
| 86400s | 10 | APPROXIMATE | 10 | 0.00% | 1.2 KB | 0.125 ms | 0.161 ms | 0.0205 ms |
| 86400s | 100 | EXACT | 100 | 0.00% | 2.6 KB | 0.120 ms | 0.134 ms | 0.0072 ms |
| 86400s | 100 | APPROXIMATE | 100 | 0.00% | 9.6 KB | 0.174 ms | 0.196 ms | 0.0055 ms |
| 86400s | 1,000 | EXACT | 1,000 | 0.00% | 94.7 KB | 0.118 ms | 0.173 ms | 0.0049 ms |
| 86400s | 1,000 | APPROXIMATE | 1,004 | 0.40% | 33.3 KB | 0.305 ms | 0.353 ms | 0.0039 ms |
| 86400s | 5,000 | EXACT | 5,000 | 0.00% | 487.4 KB | 0.119 ms | 0.145 ms | 0.0052 ms |
| 86400s | 5,000 | APPROXIMATE | 4,984 | 0.32% | 48.2 KB | 0.326 ms | 0.402 ms | 0.0038 ms |
| 86400s | 20,000 | EXACT | 20,000 | 0.00% | 2,384.8 KB | 0.120 ms | 0.154 ms | 0.0044 ms |
| 86400s | 20,000 | APPROXIMATE | 19,789 | 1.05% | 121.1 KB | 0.349 ms | 0.475 ms | 0.0039 ms |
| 86400s | 50,000 | EXACT | 50,000 | 0.00% | 5,356.5 KB | 0.128 ms | 0.166 ms | 0.0046 ms |
| 86400s | 50,000 | APPROXIMATE | 49,513 | 0.97% | 235.9 KB | 0.525 ms | 0.765 ms | 0.0040 ms |
