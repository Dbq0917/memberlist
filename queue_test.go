// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"testing"
	"time"

	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

// for testing only
func (q *TransmitLimitedQueue) orderedView() []*limitedBroadcast {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*limitedBroadcast, 0, q.lenLocked())
	q.walkReadOnlyLocked(true, func(cur *limitedBroadcast) bool {
		out = append(out, cur)
		return true
	})

	return out
}

func TestLimitedBroadcastLess(t *testing.T) {
	cases := []struct {
		Name string
		A    *limitedBroadcast // lesser
		B    *limitedBroadcast
	}{
		{
			"diff-transmits",
			&limitedBroadcast{transmits: 0, msgLen: 10, id: 100},
			&limitedBroadcast{transmits: 1, msgLen: 10, id: 100},
		},
		{
			"same-transmits--diff-len",
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 100},
			&limitedBroadcast{transmits: 0, msgLen: 10, id: 100},
		},
		{
			"same-transmits--same-len--diff-id",
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 100},
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 90},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			a, b := c.A, c.B

			require.True(t, a.Less(b))

			tree := btree.New(32)

			tree.ReplaceOrInsert(b)
			tree.ReplaceOrInsert(a)

			min := tree.Min().(*limitedBroadcast)
			require.Equal(t, a.transmits, min.transmits)
			require.Equal(t, a.msgLen, min.msgLen)
			require.Equal(t, a.id, min.id)

			max := tree.Max().(*limitedBroadcast)
			require.Equal(t, b.transmits, max.transmits)
			require.Equal(t, b.msgLen, max.msgLen)
			require.Equal(t, b.id, max.id)
		})
	}
}

func TestTransmitLimited_Queue(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 1 }}
	q.QueueBroadcast(&memberlistBroadcast{"test", nil, nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", nil, nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump := q.orderedView()
	if dump[0].b.(*memberlistBroadcast).node != "test" {
		t.Fatalf("missing test")
	}
	if dump[1].b.(*memberlistBroadcast).node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[2].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}

	// Should invalidate previous message
	q.QueueBroadcast(&memberlistBroadcast{"test", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump = q.orderedView()
	if dump[0].b.(*memberlistBroadcast).node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[1].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[2].b.(*memberlistBroadcast).node != "test" {
		t.Fatalf("missing test")
	}
}

func TestTransmitLimited_GetBroadcasts(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 3, NumNodes: func() int { return 10 }}

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// 2 byte overhead per message, should get all 4 messages
	all := q.GetBroadcasts(2, 80)
	require.Equal(t, 4, len(all), "missing messages: %v", prettyPrintMessages(all))

	// 3 byte overhead, should only get 3 messages back
	partial := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial), "missing messages: %v", prettyPrintMessages(partial))
}

func TestTransmitLimited_GetBroadcasts_Limit(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	require.Equal(t, int64(0), q.idGen, "the id generator seed starts at zero")
	require.Equal(t, 2, retransmitLimit(q.RetransmitMult, q.NumNodes()), "sanity check transmit limits")

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	require.Equal(t, int64(4), q.idGen, "we handed out 4 IDs")

	// 3 byte overhead, should only get 3 messages back
	partial1 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial1), "missing messages: %v", prettyPrintMessages(partial1))

	require.Equal(t, int64(4), q.idGen, "id generator doesn't reset until empty")

	partial2 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial2), "missing messages: %v", prettyPrintMessages(partial2))

	require.Equal(t, int64(4), q.idGen, "id generator doesn't reset until empty")

	// Only two not expired
	partial3 := q.GetBroadcasts(3, 80)
	require.Equal(t, 2, len(partial3), "missing messages: %v", prettyPrintMessages(partial3))

	require.Equal(t, int64(0), q.idGen, "id generator resets on empty")

	// Should get nothing
	partial5 := q.GetBroadcasts(3, 80)
	require.Equal(t, 0, len(partial5), "missing messages: %v", prettyPrintMessages(partial5))

	require.Equal(t, int64(0), q.idGen, "id generator resets on empty")
}

func prettyPrintMessages(msgs [][]byte) []string {
	var out []string
	for _, msg := range msgs {
		out = append(out, "'"+string(msg)+"'")
	}
	return out
}

func TestTransmitLimited_Prune(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	ch1 := make(chan struct{}, 1)
	ch2 := make(chan struct{}, 1)

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), ch1})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), ch2})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// Keep only 2
	q.Prune(2)

	require.Equal(t, 2, q.NumQueued())

	// Should notify the first two
	select {
	case <-ch1:
	default:
		t.Fatalf("expected invalidation")
	}
	select {
	case <-ch2:
	default:
		t.Fatalf("expected invalidation")
	}

	dump := q.orderedView()

	if dump[0].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[1].b.(*memberlistBroadcast).node != "baz" {
		t.Fatalf("missing baz")
	}
}

func TestTransmitLimited_ordering(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	insert := func(name string, transmits int) {
		q.queueBroadcast(&memberlistBroadcast{name, []byte(name), make(chan struct{})}, transmits)
	}

	insert("node0", 0)
	insert("node1", 10)
	insert("node2", 3)
	insert("node3", 4)
	insert("node4", 7)

	dump := q.orderedView()

	if dump[0].transmits != 10 {
		t.Fatalf("bad val %v, %d", dump[0].b.(*memberlistBroadcast).node, dump[0].transmits)
	}
	if dump[1].transmits != 7 {
		t.Fatalf("bad val %v, %d", dump[7].b.(*memberlistBroadcast).node, dump[7].transmits)
	}
	if dump[2].transmits != 4 {
		t.Fatalf("bad val %v, %d", dump[2].b.(*memberlistBroadcast).node, dump[2].transmits)
	}
	if dump[3].transmits != 3 {
		t.Fatalf("bad val %v, %d", dump[3].b.(*memberlistBroadcast).node, dump[3].transmits)
	}
	if dump[4].transmits != 0 {
		t.Fatalf("bad val %v, %d", dump[4].b.(*memberlistBroadcast).node, dump[4].transmits)
	}
}

func TestTransmitLimited_Backoff(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	q := &TransmitLimitedQueue{
		RetransmitMult:       3,
		NumNodes:             func() int { return 10 },
		RetransmitBackoff:    time.Second,
		MaxRetransmitBackoff: 4 * time.Second,
		RetransmitFanout:     2,
		now:                  func() time.Time { return now },
	}

	require.Equal(t, 6, q.transmitLimit(), "sanity check transmit limit")
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("test"), nil})

	// The configured gossip fanout is delivered immediately.
	require.Len(t, q.GetBroadcasts(2, 100), 1)
	require.Len(t, q.GetBroadcasts(2, 100), 1)

	// After the fanout, the first retransmission waits for the base backoff.
	require.Empty(t, q.GetBroadcasts(2, 100))
	now = now.Add(999 * time.Millisecond)
	require.Empty(t, q.GetBroadcasts(2, 100))
	now = now.Add(time.Millisecond)
	require.Len(t, q.GetBroadcasts(2, 100), 1)

	// The next backoff doubles.
	require.Empty(t, q.GetBroadcasts(2, 100))
	now = now.Add(2 * time.Second)
	require.Len(t, q.GetBroadcasts(2, 100), 1)

	// The backoff is capped at MaxRetransmitBackoff.
	now = now.Add(3 * time.Second)
	require.Empty(t, q.GetBroadcasts(2, 100))
	now = now.Add(time.Second)
	require.Len(t, q.GetBroadcasts(2, 100), 1)
	now = now.Add(4 * time.Second)
	require.Len(t, q.GetBroadcasts(2, 100), 1)

	require.Empty(t, q.GetBroadcasts(2, 100), "message is removed at the transmit limit")
	stats := q.Stats()
	require.Equal(t, uint64(6), stats.Transmits)
	require.Equal(t, uint64(5), stats.Retransmits)
	require.Equal(t, 6, stats.MaxTransmits)
	require.Zero(t, stats.NumQueued)
}

func TestTransmitLimited_Expiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	q := &TransmitLimitedQueue{
		RetransmitMult: 3,
		NumNodes:       func() int { return 10 },
		MessageTTL:     5 * time.Second,
		now:            func() time.Time { return now },
	}

	ch1 := make(chan struct{}, 1)
	ch2 := make(chan struct{}, 1)
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("test"), ch1})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("foo"), ch2})

	now = now.Add(4 * time.Second)
	require.Equal(t, 2, q.NumQueued())
	require.Len(t, q.GetBroadcasts(2, 100), 2)

	now = now.Add(time.Second)
	require.Zero(t, q.NumQueued(), "messages at the TTL boundary are expired")
	require.Empty(t, q.GetBroadcasts(2, 100))

	select {
	case <-ch1:
	default:
		t.Fatal("expected expired broadcast to be finished")
	}
	select {
	case <-ch2:
	default:
		t.Fatal("expected expired broadcast to be finished")
	}

	stats := q.Stats()
	require.Equal(t, uint64(2), stats.Expired)
	require.Equal(t, uint64(2), stats.Transmits)
	require.Zero(t, q.idGen, "id generator resets once the queue is empty")
}

func TestTransmitLimited_FreshUpdateNotBlockedByBackoff(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	q := &TransmitLimitedQueue{
		RetransmitMult:    3,
		NumNodes:          func() int { return 10 },
		RetransmitBackoff: time.Second,
		RetransmitFanout:  1,
		now:               func() time.Time { return now },
	}

	q.QueueBroadcast(&memberlistBroadcast{"old", []byte("old-update"), nil})
	msgs := q.GetBroadcasts(2, 100)
	require.Len(t, msgs, 1)
	require.Equal(t, []byte("old-update"), msgs[0])
	require.Empty(t, q.GetBroadcasts(2, 100), "old update is backing off")

	// A fresh membership update must still be selected immediately even though
	// the older retransmission is in backoff.
	q.QueueBroadcast(&memberlistBroadcast{"new", []byte("new-update"), nil})
	msgs = q.GetBroadcasts(2, 100)
	require.Len(t, msgs, 1)
	require.Equal(t, []byte("new-update"), msgs[0])
}

func TestTransmitLimited_ExpiryWithInvalidation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	q := &TransmitLimitedQueue{
		RetransmitMult: 3,
		NumNodes:       func() int { return 10 },
		MessageTTL:     5 * time.Second,
		now:            func() time.Time { return now },
	}

	oldCh := make(chan struct{}, 1)
	newCh := make(chan struct{}, 1)
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("old"), oldCh})
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("new"), newCh})

	select {
	case <-oldCh:
	default:
		t.Fatal("expected invalidated broadcast to be finished")
	}
	require.Equal(t, 1, q.NumQueued())

	now = now.Add(5 * time.Second)
	require.Zero(t, q.NumQueued())
	select {
	case <-newCh:
	default:
		t.Fatal("expected replacement broadcast to expire")
	}
	require.Equal(t, uint64(1), q.Stats().Expired)
}

func TestTransmitLimited_BackoffPreservesNormalFanout(t *testing.T) {
	const gossipNodes = 3

	now := time.Unix(1_700_000_000, 0)
	q := &TransmitLimitedQueue{
		RetransmitMult:    4,
		NumNodes:          func() int { return 10 },
		RetransmitBackoff: time.Second,
		RetransmitFanout:  gossipNodes,
		now:               func() time.Time { return now },
	}
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("update"), nil})

	// A healthy node still hands a fresh update to GossipNodes peers in the
	// same gossip round, matching the queue's behavior without backoff.
	for range gossipNodes {
		require.Len(t, q.GetBroadcasts(2, 100), 1)
	}
	require.Empty(t, q.GetBroadcasts(2, 100))
}

type lossyBroadcastRound struct {
	enqueued   int
	maxDepth   int
	queued     int
	transmits  uint64
	retransmit uint64
	expired    uint64
	maxPerMsg  int
	limit      int
}

// runLossyBroadcastRound models a constrained cross-datacenter link:
// 10 membership updates/second are produced but the lossy path only has
// capacity for 5 transmission attempts/second.
func runLossyBroadcastRound(backoff, ttl time.Duration) lossyBroadcastRound {
	now := time.Unix(1_700_000_000, 0)
	q := &TransmitLimitedQueue{
		RetransmitMult:       2,
		NumNodes:             func() int { return 100 },
		RetransmitBackoff:    backoff,
		MaxRetransmitBackoff: time.Second,
		RetransmitFanout:     2,
		MessageTTL:           ttl,
		now:                  func() time.Time { return now },
	}

	const (
		step          = 100 * time.Millisecond
		lossyDuration = 40 * time.Second
		enqueueEvery  = 500 * time.Millisecond
		enqueueBatch  = 5
	)

	var result lossyBroadcastRound
	var nextEnqueue time.Time
	deadline := now.Add(lossyDuration)
	for tick := 0; !now.After(deadline); tick++ {
		if !now.Before(nextEnqueue) {
			for range enqueueBatch {
				q.QueueBroadcast(&memberlistBroadcast{
					node: time.Duration(result.enqueued).String(),
					msg:  []byte("member-update"),
				})
				result.enqueued++
			}
			nextEnqueue = now.Add(enqueueEvery)
		}

		// Every other step represents packets lost to the constrained link.
		if tick%2 == 0 {
			q.GetBroadcasts(2, 32)
		}
		if depth := q.NumQueued(); depth > result.maxDepth {
			result.maxDepth = depth
		}
		now = now.Add(step)
	}

	stats := q.Stats()
	result.queued = stats.NumQueued
	result.transmits = stats.Transmits
	result.retransmit = stats.Retransmits
	result.expired = stats.Expired
	result.maxPerMsg = stats.MaxTransmits
	result.limit = stats.RetransmitLimit
	return result
}

func TestTransmitLimited_LossyBoundedAndRecovers(t *testing.T) {
	legacy := runLossyBroadcastRound(0, 0)
	bounded := runLossyBroadcastRound(100*time.Millisecond, 10*time.Second)

	require.Greater(t, legacy.maxDepth, 150, "control queue grows without a TTL")
	require.LessOrEqual(t, bounded.maxDepth, 120, "TTL bounds the queue under sustained loss")
	require.LessOrEqual(t, bounded.maxPerMsg, bounded.limit, "no message exceeds the transmit limit")

	// Restore the link by giving the queue a healthy per-step packet budget.
	now := time.Unix(1_700_000_000, 0).Add(40 * time.Second)
	q := &TransmitLimitedQueue{
		RetransmitMult:       2,
		NumNodes:             func() int { return 100 },
		RetransmitBackoff:    100 * time.Millisecond,
		MaxRetransmitBackoff: time.Second,
		RetransmitFanout:     2,
		MessageTTL:           10 * time.Second,
		now:                  func() time.Time { return now },
	}
	for i := range 100 {
		q.QueueBroadcast(&memberlistBroadcast{node: time.Duration(i).String(), msg: []byte("member-update")})
	}

	recoveryStart := now
	for q.NumQueued() > 0 && now.Sub(recoveryStart) <= 10*time.Second {
		q.GetBroadcasts(2, 1<<20)
		now = now.Add(50 * time.Millisecond)
	}
	recovery := now.Sub(recoveryStart)

	t.Logf("legacy lossy round: enqueued=%d max_depth=%d queued=%d transmits=%d retransmits=%d expired=%d max_per_message=%d limit=%d",
		legacy.enqueued, legacy.maxDepth, legacy.queued, legacy.transmits,
		legacy.retransmit, legacy.expired, legacy.maxPerMsg, legacy.limit)
	t.Logf("bounded lossy round: enqueued=%d max_depth=%d queued=%d transmits=%d retransmits=%d expired=%d max_per_message=%d limit=%d",
		bounded.enqueued, bounded.maxDepth, bounded.queued, bounded.transmits,
		bounded.retransmit, bounded.expired, bounded.maxPerMsg, bounded.limit)
	t.Logf("healthy recovery: queued=%d recovery=%s", 100, recovery)

	require.Zero(t, q.NumQueued(), "queue drains after network recovery")
	require.LessOrEqual(t, recovery, 10*time.Second, "recovery stays within the message TTL")
}
