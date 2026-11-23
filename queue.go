// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"container/list"
	"math"
	"sync"
	"time"

	"github.com/google/btree"
)

// TransmitLimitedQueue is used to queue messages to broadcast to
// the cluster (via gossip) but limits the number of transmits per
// message. It also prioritizes messages with lower transmit counts
// (hence newer messages).
type TransmitLimitedQueue struct {
	// NumNodes returns the number of nodes in the cluster. This is
	// used to determine the retransmit count, which is calculated
	// based on the log of this.
	NumNodes func() int

	// RetransmitMult is the multiplier used to determine the maximum
	// number of retransmissions attempted.
	RetransmitMult int

	// MaxTransmits caps the number of times a single broadcast is sent. A
	// zero value leaves the retransmit limit uncapped.
	MaxTransmits int

	// DedupWindow is how long the queue remembers recently accepted logical
	// broadcasts. A duplicate received during this window is acknowledged but
	// not queued again. A zero value disables duplicate suppression.
	DedupWindow time.Duration

	// DedupCacheSize bounds the number of remembered broadcasts. A zero value
	// uses a conservative default when DedupWindow is enabled.
	DedupCacheSize int

	mu          sync.Mutex
	tq          *btree.BTree // stores *limitedBroadcast as btree.Item
	tm          map[string]*limitedBroadcast
	idGen       int64
	seen        map[string]*list.Element
	seenOrder   *list.List
	now         func() time.Time
	transmits   uint64
	retransmits uint64
	suppressed  uint64
	maxQueued   int
	maxSent     int
}

type seenBroadcast struct {
	key       string
	expiresAt time.Time
}

const defaultDedupCacheSize = 4096

type limitedBroadcast struct {
	transmits int   // btree-key[0]: Number of transmissions attempted.
	msgLen    int64 // btree-key[1]: copied from len(b.Message())
	id        int64 // btree-key[2]: unique incrementing id stamped at submission time
	b         Broadcast

	name string // set if Broadcast is a NamedBroadcast
}

// Less tests whether the current item is less than the given argument.
//
// This must provide a strict weak ordering.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
// hold one of either a or b in the tree).
//
// default ordering is
// - [transmits=0, ..., transmits=inf]
// - [transmits=0:len=999, ..., transmits=0:len=2, ...]
// - [transmits=0:len=999,id=999, ..., transmits=0:len=999:id=1, ...]
func (b *limitedBroadcast) Less(than btree.Item) bool {
	o := than.(*limitedBroadcast)
	if b.transmits < o.transmits {
		return true
	} else if b.transmits > o.transmits {
		return false
	}
	if b.msgLen > o.msgLen {
		return true
	} else if b.msgLen < o.msgLen {
		return false
	}
	return b.id > o.id
}

// walkReadOnlyLocked calls f for each item in the queue traversing it in
// natural order (by Less) when reverse=false and the opposite when true. You
// must hold the mutex.
//
// This method panics if you attempt to mutate the item during traversal.  The
// underlying btree should also not be mutated during traversal.
func (q *TransmitLimitedQueue) walkReadOnlyLocked(reverse bool, f func(*limitedBroadcast) bool) {
	if q.lenLocked() == 0 {
		return
	}

	iter := func(item btree.Item) bool {
		cur := item.(*limitedBroadcast)

		prevTransmits := cur.transmits
		prevMsgLen := cur.msgLen
		prevID := cur.id

		keepGoing := f(cur)

		if prevTransmits != cur.transmits || prevMsgLen != cur.msgLen || prevID != cur.id {
			panic("edited queue while walking read only")
		}

		return keepGoing
	}

	if reverse {
		q.tq.Descend(iter) // end with transmit 0
	} else {
		q.tq.Ascend(iter) // start with transmit 0
	}
}

// Broadcast is something that can be broadcasted via gossip to
// the memberlist cluster.
type Broadcast interface {
	// Invalidates checks if enqueuing the current broadcast
	// invalidates a previous broadcast
	Invalidates(b Broadcast) bool

	// Returns a byte form of the message
	Message() []byte

	// Finished is invoked when the message will no longer
	// be broadcast, either due to invalidation or to the
	// transmit limit being reached
	Finished()
}

// NamedBroadcast is an optional extension of the Broadcast interface that
// gives each message a unique string name, and that is used to optimize
//
// You shoud ensure that Invalidates() checks the same uniqueness as the
// example below:
//
//	func (b *foo) Invalidates(other Broadcast) bool {
//		nb, ok := other.(NamedBroadcast)
//		if !ok {
//			return false
//		}
//		return b.Name() == nb.Name()
//	}
//
// Invalidates() isn't currently used for NamedBroadcasts, but that may change
// in the future.
type NamedBroadcast interface {
	Broadcast
	// The unique identity of this broadcast message.
	Name() string
}

// UniqueBroadcast is an optional interface that indicates that each message is
// intrinsically unique and there is no need to scan the broadcast queue for
// duplicates.
//
// You should ensure that Invalidates() always returns false if implementing
// this interface. Invalidates() isn't currently used for UniqueBroadcasts, but
// that may change in the future.
type UniqueBroadcast interface {
	Broadcast
	// UniqueBroadcast is just a marker method for this interface.
	UniqueBroadcast()
}

// QueueBroadcast is used to enqueue a broadcast
func (q *TransmitLimitedQueue) QueueBroadcast(b Broadcast) {
	q.queueBroadcast(b, 0)
}

// lazyInit initializes internal data structures the first time they are
// needed.  You must already hold the mutex.
func (q *TransmitLimitedQueue) lazyInit() {
	if q.tq == nil {
		q.tq = btree.New(32)
	}
	if q.tm == nil {
		q.tm = make(map[string]*limitedBroadcast)
	}
	if q.seen == nil {
		q.seen = make(map[string]*list.Element)
		q.seenOrder = list.New()
	}
}

// queueBroadcast is like QueueBroadcast but you can use a nonzero value for
// the initial transmit tier assigned to the message. This is meant to be used
// for unit testing.
func (q *TransmitLimitedQueue) queueBroadcast(b Broadcast, initialTransmits int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.lazyInit()
	now := q.nowTime()
	if q.isDuplicateLocked(b, now) {
		q.suppressed++
		b.Finished()
		return
	}
	q.rememberLocked(b, now)

	if q.idGen == math.MaxInt64 {
		// it's super duper unlikely to wrap around within the retransmit limit
		q.idGen = 1
	} else {
		q.idGen++
	}
	id := q.idGen

	lb := &limitedBroadcast{
		transmits: initialTransmits,
		msgLen:    int64(len(b.Message())),
		id:        id,
		b:         b,
	}
	unique := false
	if nb, ok := b.(NamedBroadcast); ok {
		lb.name = nb.Name()
	} else if _, ok := b.(UniqueBroadcast); ok {
		unique = true
	}

	// Check if this message invalidates another.
	if lb.name != "" {
		if old, ok := q.tm[lb.name]; ok {
			old.b.Finished()
			q.deleteItem(old)
		}
	} else if !unique {
		// Slow path, hopefully nothing hot hits this.
		var remove []*limitedBroadcast
		q.tq.Ascend(func(item btree.Item) bool {
			cur := item.(*limitedBroadcast)

			// Special Broadcasts can only invalidate each other.
			switch cur.b.(type) {
			case NamedBroadcast:
				// noop
			case UniqueBroadcast:
				// noop
			default:
				if b.Invalidates(cur.b) {
					cur.b.Finished()
					remove = append(remove, cur)
				}
			}
			return true
		})
		for _, cur := range remove {
			q.deleteItem(cur)
		}
	}

	// Append to the relevant queue.
	q.addItem(lb)
}

// deleteItem removes the given item from the overall datastructure. You
// must already hold the mutex.
func (q *TransmitLimitedQueue) deleteItem(cur *limitedBroadcast) {
	_ = q.tq.Delete(cur)
	if cur.name != "" {
		delete(q.tm, cur.name)
	}

	if q.tq.Len() == 0 {
		// At idle there's no reason to let the id generator keep going
		// indefinitely.
		q.idGen = 0
	}
}

// addItem adds the given item into the overall datastructure. You must already
// hold the mutex.
func (q *TransmitLimitedQueue) addItem(cur *limitedBroadcast) {
	_ = q.tq.ReplaceOrInsert(cur)
	if cur.name != "" {
		q.tm[cur.name] = cur
	}
	if queued := q.tq.Len(); queued > q.maxQueued {
		q.maxQueued = queued
	}
}

type broadcastFingerprinter interface {
	Fingerprint() string
}

func (q *TransmitLimitedQueue) nowTime() time.Time {
	if q.now != nil {
		return q.now()
	}
	return time.Now()
}

// isDuplicateLocked reports whether this exact logical broadcast was accepted
// recently. A queued duplicate is not allowed to reset the original message's
// transmit count, which is what turns independent confirmations into a storm.
func (q *TransmitLimitedQueue) isDuplicateLocked(b Broadcast, now time.Time) bool {
	if q.DedupWindow <= 0 {
		return false
	}
	fp, ok := b.(broadcastFingerprinter)
	if !ok {
		return false
	}
	key := fp.Fingerprint()
	if key == "" {
		return false
	}

	q.expireSeenLocked(now)
	_, ok = q.seen[key]
	return ok
}

func (q *TransmitLimitedQueue) rememberLocked(b Broadcast, now time.Time) {
	if q.DedupWindow <= 0 {
		return
	}
	fp, ok := b.(broadcastFingerprinter)
	if !ok {
		return
	}
	key := fp.Fingerprint()
	if key == "" {
		return
	}

	q.expireSeenLocked(now)
	if _, ok := q.seen[key]; ok {
		return
	}

	elem := q.seenOrder.PushBack(seenBroadcast{
		key:       key,
		expiresAt: now.Add(q.DedupWindow),
	})
	q.seen[key] = elem

	limit := q.DedupCacheSize
	if limit <= 0 {
		limit = defaultDedupCacheSize
	}
	for q.seenOrder.Len() > limit {
		q.removeOldestSeenLocked()
	}
}

func (q *TransmitLimitedQueue) expireSeenLocked(now time.Time) {
	for q.seenOrder != nil && q.seenOrder.Len() > 0 {
		elem := q.seenOrder.Front()
		seen := elem.Value.(seenBroadcast)
		if now.Before(seen.expiresAt) {
			return
		}
		q.removeOldestSeenLocked()
	}
}

func (q *TransmitLimitedQueue) removeOldestSeenLocked() {
	elem := q.seenOrder.Front()
	seen := elem.Value.(seenBroadcast)
	delete(q.seen, seen.key)
	q.seenOrder.Remove(elem)
}

// getTransmitRange returns a pair of min/max values for transmit values
// represented by the current queue contents. Both values represent actual
// transmit values on the interval [0, len). You must already hold the mutex.
func (q *TransmitLimitedQueue) getTransmitRange() (minTransmit, maxTransmit int) {
	if q.lenLocked() == 0 {
		return 0, 0
	}
	minItem, maxItem := q.tq.Min(), q.tq.Max()
	if minItem == nil || maxItem == nil {
		return 0, 0
	}

	min := minItem.(*limitedBroadcast).transmits
	max := maxItem.(*limitedBroadcast).transmits

	return min, max
}

// GetBroadcasts is used to get a number of broadcasts, up to a byte limit
// and applying a per-message overhead as provided.
func (q *TransmitLimitedQueue) GetBroadcasts(overhead, limit int) [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Fast path the default case
	if q.lenLocked() == 0 {
		return nil
	}

	transmitLimit := q.transmitLimit()

	var (
		bytesUsed int
		toSend    [][]byte
		reinsert  []*limitedBroadcast
	)

	// Visit fresher items first, but only look at stuff that will fit.
	// We'll go tier by tier, grabbing the largest items first.
	minTr, maxTr := q.getTransmitRange()
	for transmits := minTr; transmits <= maxTr; /*do not advance automatically*/ {
		free := int64(limit - bytesUsed - overhead)
		if free <= 0 {
			break // bail out early
		}

		// Search for the least element on a given tier (by transmit count) as
		// defined in the limitedBroadcast.Less function that will fit into our
		// remaining space.
		greaterOrEqual := &limitedBroadcast{
			transmits: transmits,
			msgLen:    free,
			id:        math.MaxInt64,
		}
		lessThan := &limitedBroadcast{
			transmits: transmits + 1,
			msgLen:    math.MaxInt64,
			id:        math.MaxInt64,
		}
		var keep *limitedBroadcast
		q.tq.AscendRange(greaterOrEqual, lessThan, func(item btree.Item) bool {
			cur := item.(*limitedBroadcast)
			// Check if this is within our limits
			if int64(len(cur.b.Message())) > free {
				// If this happens it's a bug in the datastructure or
				// surrounding use doing something like having len(Message())
				// change over time. There's enough going on here that it's
				// probably sane to just skip it and move on for now.
				return true
			}
			keep = cur
			return false
		})
		if keep == nil {
			// No more items of an appropriate size in the tier.
			transmits++
			continue
		}

		msg := keep.b.Message()

		// Add to slice to send
		bytesUsed += overhead + len(msg)
		toSend = append(toSend, msg)
		q.transmits++
		if keep.transmits > 0 {
			q.retransmits++
		}
		if keep.transmits+1 > q.maxSent {
			q.maxSent = keep.transmits + 1
		}

		// Check if we should stop transmission
		q.deleteItem(keep)
		if keep.transmits+1 >= transmitLimit {
			keep.b.Finished()
		} else {
			// We need to bump this item down to another transmit tier, but
			// because it would be in the same direction that we're walking the
			// tiers, we will have to delay the reinsertion until we are
			// finished our search. Otherwise we'll possibly re-add the message
			// when we ascend to the next tier.
			keep.transmits++
			reinsert = append(reinsert, keep)
		}
	}

	for _, cur := range reinsert {
		q.addItem(cur)
	}

	return toSend
}

func (q *TransmitLimitedQueue) transmitLimit() int {
	nodes := 0
	if q.NumNodes != nil {
		nodes = q.NumNodes()
	}

	limit := retransmitLimit(q.RetransmitMult, nodes)
	if q.MaxTransmits > 0 && (limit <= 0 || limit > q.MaxTransmits) {
		limit = q.MaxTransmits
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// TransmitLimitedQueueStats is a snapshot of broadcast queue activity.
type TransmitLimitedQueueStats struct {
	NumQueued       int
	MaxQueued       int
	Transmits       uint64
	Retransmits     uint64
	Suppressed      uint64
	MaxTransmits    int
	RetransmitLimit int
}

// Stats returns a snapshot of queue depth and transmission counters.
func (q *TransmitLimitedQueue) Stats() TransmitLimitedQueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	return TransmitLimitedQueueStats{
		NumQueued:       q.lenLocked(),
		MaxQueued:       q.maxQueued,
		Transmits:       q.transmits,
		Retransmits:     q.retransmits,
		Suppressed:      q.suppressed,
		MaxTransmits:    q.maxSent,
		RetransmitLimit: q.transmitLimit(),
	}
}

// NumQueued returns the number of queued messages
func (q *TransmitLimitedQueue) NumQueued() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.lenLocked()
}

// lenLocked returns the length of the overall queue datastructure. You must
// hold the mutex.
func (q *TransmitLimitedQueue) lenLocked() int {
	if q.tq == nil {
		return 0
	}
	return q.tq.Len()
}

// Reset clears all the queued messages. Should only be used for tests.
func (q *TransmitLimitedQueue) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.walkReadOnlyLocked(false, func(cur *limitedBroadcast) bool {
		cur.b.Finished()
		return true
	})

	q.tq = nil
	q.tm = nil
	q.idGen = 0
	q.seen = nil
	q.seenOrder = nil
	q.transmits = 0
	q.retransmits = 0
	q.suppressed = 0
	q.maxQueued = 0
	q.maxSent = 0
}

// Prune will retain the maxRetain latest messages, and the rest
// will be discarded. This can be used to prevent unbounded queue sizes
func (q *TransmitLimitedQueue) Prune(maxRetain int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Do nothing if queue size is less than the limit
	for q.tq.Len() > maxRetain {
		item := q.tq.Max()
		if item == nil {
			break
		}
		cur := item.(*limitedBroadcast)
		cur.b.Finished()
		q.deleteItem(cur)
	}
}
