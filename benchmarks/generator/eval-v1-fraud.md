# Injected fraud — `eval-v1`

> Produced by `scripts/inspect_fraud.py`, which connects as **`trace_eval`** —
> the only role that may read ground truth (ADR-0004). Not even the generator
> that wrote these labels can read them back (ADR-0031).

- rows: **1,000,000**
- digest: `sha256:0679d08a29ee3fd2e065c431cf59cfa2da922c960f382b690e9da3ab52eaef0b`

## Pattern mix

```
  VELOCITY_ATTACK             1,605  32.08%
  CARD_TESTING                  776  15.51%
  FRAUD_RING                    760  15.19%
  DEVICE_FARM                   598  11.95%
  MERCHANT_COLLUSION            442   8.83%
  ACCOUNT_TAKEOVER              422   8.43%
  IMPOSSIBLE_TRAVEL             116   2.32%
  UNUSUAL_LOCATION_DEVICE        99   1.98%
  CREDENTIAL_STUFFING            93   1.86%
  ANOMALOUS_HIGH_VALUE           92   1.84%
  TOTAL                       5,003
```

## Episodes per pattern

```
  ACCOUNT_TAKEOVER              90 episodes
  ANOMALOUS_HIGH_VALUE          92 episodes
  CARD_TESTING                  50 episodes
  CREDENTIAL_STUFFING           28 episodes
  DEVICE_FARM                   38 episodes
  FRAUD_RING                    35 episodes
  IMPOSSIBLE_TRAVEL             58 episodes
  MERCHANT_COLLUSION            17 episodes
  UNUSUAL_LOCATION_DEVICE       99 episodes
  VELOCITY_ATTACK               60 episodes
```

## Causal evidence keys, as recorded

```
  ACCOUNT_TAKEOVER           AMOUNT_ANOMALY, DEVICE_NOVELTY, IDENTITY_CHANGE, SPEND_PROFILE
  ANOMALOUS_HIGH_VALUE       AMOUNT_ANOMALY, MCC_ANOMALY, SPEND_PROFILE
  CARD_TESTING               AMOUNT_ANOMALY, DEVICE_SHARING, MCC_ANOMALY, VELOCITY
  CREDENTIAL_STUFFING        DEVICE_SHARING, IDENTITY_CHANGE, IP_REPUTATION
  DEVICE_FARM                DEVICE_NOVELTY, DEVICE_SHARING, GRAPH_CLUSTER
  FRAUD_RING                 DEVICE_SHARING, GRAPH_CLUSTER, LINK_PATH, RING_SCORE
  IMPOSSIBLE_TRAVEL          GEO_DISPERSION, VELOCITY
  MERCHANT_COLLUSION         MCC_ANOMALY, MERCHANT_PATTERN, MERCHANT_RISK
  UNUSUAL_LOCATION_DEVICE    DEVICE_NOVELTY, GEO_DISPERSION
  VELOCITY_ATTACK            SPEND_PROFILE, VELOCITY
```

## Sample episodes

### VELOCITY_ATTACK

```
  fi_00000541                    17 tx   accounts=1
  fi_00000182                    20 tx   accounts=1
```

### CARD_TESTING

```
  fi_00000416                    19 tx   accounts=1, devices=1
  fi_00000353                    14 tx   accounts=1, devices=1
```

### FRAUD_RING

```
  fi_00000189                    33 tx   accounts=8, devices=2, ips=1, merchants=2
  fi_00000039                    27 tx   accounts=8, devices=2, ips=1, merchants=2
```

### DEVICE_FARM

```
  fi_00000209                    17 tx   accounts=11, devices=1
  fi_00000114                    21 tx   accounts=13, devices=1
```

### MERCHANT_COLLUSION

```
  fi_00000467                    25 tx   accounts=25, merchants=1
  fi_00000395                    22 tx   accounts=22, merchants=1
```

### ACCOUNT_TAKEOVER

```
  fi_00000513                     4 tx   accounts=1, devices=1
  fi_00000158                     4 tx   accounts=1, devices=1
```

### IMPOSSIBLE_TRAVEL

```
  fi_00000063                     2 tx   accounts=1
  fi_00000223                     2 tx   accounts=1
```

### UNUSUAL_LOCATION_DEVICE

```
  fi_00000412                     1 tx   accounts=1, devices=1
  fi_00000167                     1 tx   accounts=1, devices=1
```

### CREDENTIAL_STUFFING

```
  fi_00000047                     2 tx   accounts=36, devices=1, ips=3
  fs_credential_stuffing          4 tx   accounts=30, devices=1, ips=1
```

### ANOMALOUS_HIGH_VALUE

```
  fi_00000130                     1 tx   accounts=1
  fi_00000170                     1 tx   accounts=1
```

