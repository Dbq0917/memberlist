# Broadcast retransmission reliability

This document describes the broadcast retransmission controls used when gossip
packets are lost or delayed, for example on a cross-datacenter link.

## Invariants

- `RetransmitMult` keeps its existing meaning:
  `RetransmitMult * ceil(log10(N+1))` is still the per-message transmit limit.
- Message encoding and decoding are unchanged. The controls below only affect
  local queue scheduling and retention.
- Message invalidation semantics are unchanged: a newer named broadcast still
  replaces an older broadcast for the same node.
- Setting the new timing fields to zero preserves the previous "no time-based
  limit" behavior for callers that construct `TransmitLimitedQueue` directly or
  use a `Config` literal.

## Controls

| Config field | LAN default | WAN default | Purpose |
| --- | --- | --- | --- |
| `BroadcastRetransmitBackoff` | `100ms` | `250ms` | Base delay before a retransmission. The delay doubles for each retransmission after the fanout. |
| `BroadcastMaxRetransmitBackoff` | `2s` | `5s` | Upper bound for exponential backoff. Zero means `16 * base`. |
| `BroadcastRetransmitFanout` | `0` (`GossipNodes`) | `0` (`GossipNodes`) | Immediate transmissions before backoff. This keeps normal gossip fanout unchanged. |
| `BroadcastMessageTTL` | `30s` | `60s` | Maximum local queue age. Stale messages are dropped and `Finished` is called. |
| `BroadcastQueueWarnDepth` | `512` | `512` | Emit a warning when queue depth reaches this value. Zero disables the warning. |

`DefaultLocalConfig` uses a `50ms` base backoff, `1s` maximum backoff, and a
`15s` TTL.

Fresh messages are still ordered ahead of retransmissions. A fresh membership
update is therefore not delayed by older messages that are currently backing
off.

## Metrics

The existing sample is retained:

- `memberlist.queue.broadcasts`: periodic queue-depth samples.

Additional observability:

- `memberlist.queue.broadcasts.depth` (gauge): current queue depth.
- `memberlist.queue.broadcasts.max` (gauge): high-water mark since process start.
- `memberlist.queue.broadcasts.retransmit_limit` (gauge): current per-message
  transmit limit.
- `memberlist.broadcast.transmits` (counter): attempted transmissions in the
  reporting interval.
- `memberlist.broadcast.retransmits` (counter): attempted transmissions after
  the first attempt for a message.
- `memberlist.broadcast.expired` (counter): messages dropped by TTL.
- `memberlist.broadcast.dropped` (counter): messages dropped by `Prune`.

`Memberlist.BroadcastQueueStats()` exposes the same values programmatically.

For alerting, use a sustained high `queue.broadcasts.depth` (for example, above
`BroadcastQueueWarnDepth` for several consecutive `QueueCheckInterval`
periods). A rising `max` gauge with a low `expired` rate means the queue is
retaining work; a high `expired` rate alongside high depth points at packet
loss, a constrained link, or churn exceeding the gossip budget.

## Reproducible measurement

Run the deterministic constrained-link test:

```sh
go test -run TestTransmitLimited_LossyBoundedAndRecovers -count=1 -v .
```

The test models 10 membership updates/second while the lossy link only has
capacity for 5 transmission attempts/second. The legacy control queue disables
TTL and backoff; the bounded queue uses a `100ms` base backoff and a `10s` TTL.
It then restores a healthy packet budget and measures drain time.

Recorded result from the current implementation:

```text
legacy lossy round:  enqueued=405 max_depth=405 queued=405 transmits=402 retransmits=1 expired=0   max_per_message=2 limit=6
bounded lossy round: enqueued=405 max_depth=100 queued=100 transmits=402 retransmits=1 expired=305 max_per_message=2 limit=6
healthy recovery:    queued=100 recovery=1.6s
```

The legacy queue grows linearly with the duration of the lossy period. The
bounded queue stabilizes at the TTL-derived working set, drops stale entries,
and drains in `1.6s` after the link recovers.

The normal-path checks are:

```sh
go test -run 'TestTransmitLimited_(Backoff|Expiry|FreshUpdateNotBlockedByBackoff|BackoffPreservesNormalFanout)' -count=1 -v .
```

`TestTransmitLimited_BackoffPreservesNormalFanout` verifies that a healthy node
still hands a fresh update to `GossipNodes` peers immediately in the same
gossip round, so normal-network convergence behavior is unchanged.
