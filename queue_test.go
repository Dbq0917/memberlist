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

func TestTransmitLimited_DuplicateSuppression(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	q := &TransmitLimitedQueue{
		RetransmitMult: 4,
		MaxTransmits:   4,
		DedupWindow:    time.Minute,
		NumNodes:       func() int { return 200 },
		now:            func() time.Time { return now },
	}

	queueSuspect := func(from string) {
		buf, err := encode(suspectMsg, &suspect{
			Incarnation: 1,
			Node:        "node",
			From:        from,
		}, false)
		require.NoError(t, err)
		q.QueueBroadcast(&memberlistBroadcast{"node", buf.Bytes(), nil})
	}

	for i := 0; i < 100; i++ {
		queueSuspect(time.Duration(i).String())
	}

	require.Equal(t, 1, q.NumQueued(),
		"independent confirmations of the same suspect event must not be queued again")
	require.Equal(t, uint64(99), q.Stats().Suppressed)

	sent := 0
	for {
		msgs := q.GetBroadcasts(2, 1<<20)
		if len(msgs) == 0 {
			break
		}
		sent += len(msgs)
	}
	require.Equal(t, 4, sent, "the per-message transmit cap must be enforced")

	stats := q.Stats()
	require.Equal(t, 4, stats.RetransmitLimit)
	require.Equal(t, 4, stats.MaxTransmits)
	require.Equal(t, uint64(4), stats.Transmits)
	require.Equal(t, uint64(3), stats.Retransmits)

	// The suppression window also covers a duplicate that arrives after the
	// original has left the queue.
	queueSuspect("late")
	require.Zero(t, q.NumQueued())
	require.Equal(t, uint64(100), q.Stats().Suppressed)

	now = now.Add(time.Minute + time.Nanosecond)
	queueSuspect("after-window")
	require.Equal(t, 1, q.NumQueued(),
		"suppression is bounded so a delayed retransmission can still make progress")
}

func TestTransmitLimited_RealChangesNotSuppressed(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	q := &TransmitLimitedQueue{
		RetransmitMult: 4,
		MaxTransmits:   4,
		DedupWindow:    time.Minute,
		NumNodes:       func() int { return 200 },
		now:            func() time.Time { return now },
	}

	queue := func(msgType messageType, msg any) {
		buf, err := encode(msgType, msg, false)
		require.NoError(t, err)
		q.QueueBroadcast(&memberlistBroadcast{"node", buf.Bytes(), nil})
	}

	queue(aliveMsg, &alive{Incarnation: 1, Node: "node"})
	queue(suspectMsg, &suspect{Incarnation: 1, Node: "node", From: "a"})
	require.Equal(t, 1, q.NumQueued())
	require.Equal(t, suspectMsg, messageType(q.orderedView()[0].b.Message()[0]),
		"a suspect event must replace the older alive event")

	queue(deadMsg, &dead{Incarnation: 1, Node: "node", From: "b"})
	require.Equal(t, 1, q.NumQueued())
	require.Equal(t, deadMsg, messageType(q.orderedView()[0].b.Message()[0]),
		"a failure must replace the older suspect event")

	queue(aliveMsg, &alive{Incarnation: 2, Node: "node"})
	require.Equal(t, 1, q.NumQueued())
	require.Equal(t, aliveMsg, messageType(q.orderedView()[0].b.Message()[0]),
		"a newer incarnation must replace the older failure event")
	require.Zero(t, q.Stats().Suppressed)
}

func TestTransmitLimited_LargeClusterVolumeIsLinear(t *testing.T) {
	const (
		numNodes = 200
		numJoins = 50
	)

	q := &TransmitLimitedQueue{
		RetransmitMult: 4,
		MaxTransmits:   4,
		DedupWindow:    time.Minute,
		NumNodes:       func() int { return numNodes },
	}

	for node := 0; node < numJoins; node++ {
		msg := &alive{
			Incarnation: 1,
			Node:        time.Duration(node).String(),
			Addr:        []byte{127, 0, 0, 1},
			Port:        7946,
			Vsn:         []uint8{1, 5, 5, 1, 5, 5},
		}
		buf, err := encode(aliveMsg, msg, false)
		require.NoError(t, err)

		// Model the same event arriving from every peer. Without suppression
		// each duplicate resets the transmit counter and starts another fanout.
		for range numNodes {
			q.QueueBroadcast(&memberlistBroadcast{msg.Node, buf.Bytes(), nil})
		}
	}

	require.Equal(t, numJoins, q.NumQueued())
	require.Equal(t, uint64(numJoins*(numNodes-1)), q.Stats().Suppressed)

	sent := 0
	for {
		msgs := q.GetBroadcasts(2, 1<<20)
		if len(msgs) == 0 {
			break
		}
		sent += len(msgs)
	}

	// The legacy retransmit limit at 200 nodes is 12, and every duplicate reset
	// it. The bounded queue instead emits at most four copies of each event.
	require.Equal(t, numJoins*q.MaxTransmits, sent)
	require.LessOrEqual(t, sent, numNodes, "one batch of joins is linear in the number of joins")
	t.Logf("nodes=%d joins=%d suppressed=%d transmitted=%d legacy_limit=%d",
		numNodes, numJoins, q.Stats().Suppressed, sent,
		retransmitLimit(q.RetransmitMult, numNodes))
}
