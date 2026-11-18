// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"errors"
	"fmt"
	"time"
)

// ErrClusterAtCapacity is the reason given to a node whose join is rejected
// because the cluster has reached its configured membership limit
// (Config.MaxMembers). It is delivered to the joining node as an explicit
// error so that capacity rejections are not mistaken for network timeouts.
var ErrClusterAtCapacity = errors.New("memberlist: cluster at membership capacity")

// maxJoinEvents bounds how many join decisions are retained for review.
const maxJoinEvents = 256

// JoinEvent records the admission decision made for a single join attempt
// so that operators can review why a node was admitted or rejected.
type JoinEvent struct {
	// Time is when the decision was made.
	Time time.Time

	// RemoteAddr is the network address the join attempt came from.
	RemoteAddr string

	// Node is the name of the joining node, if it could be determined.
	Node string

	// Admitted is true if the join was allowed to proceed.
	Admitted bool

	// Reason explains a rejection. It is empty for admitted joins.
	Reason string

	// NumMembers is the number of live members at decision time.
	NumMembers int

	// MaxMembers is the membership limit at decision time. Zero means
	// no limit was configured.
	MaxMembers int
}

// MaxMembers returns the configured membership limit. Zero means unlimited.
func (m *Memberlist) MaxMembers() int {
	return int(m.maxMembers.Load())
}

// SetMaxMembers updates the membership limit at runtime. Values less than
// or equal to zero disable the limit. This is useful when expanding a
// cluster that is approaching its current limit.
func (m *Memberlist) SetMaxMembers(limit int) {
	if limit < 0 {
		limit = 0
	}
	m.maxMembers.Store(int32(limit))
}

// JoinEvents returns a copy of the most recent join admission decisions,
// oldest first. At most maxJoinEvents entries are retained.
func (m *Memberlist) JoinEvents() []JoinEvent {
	m.joinEventsLock.Lock()
	defer m.joinEventsLock.Unlock()

	events := make([]JoinEvent, len(m.joinEvents))
	copy(events, m.joinEvents)
	return events
}

// recordJoinEvent appends a join decision to the review log, dropping the
// oldest entry once the log is full.
func (m *Memberlist) recordJoinEvent(evt JoinEvent) {
	m.joinEventsLock.Lock()
	defer m.joinEventsLock.Unlock()

	if len(m.joinEvents) >= maxJoinEvents {
		m.joinEvents = m.joinEvents[1:]
	}
	m.joinEvents = append(m.joinEvents, evt)
}

// checkJoinCapacity reports whether admitting the given remote state from a
// join would exceed the configured membership limit. A nil error means the
// join may proceed. Rejoins by nodes already known to be alive are always
// allowed since they do not grow the cluster.
func (m *Memberlist) checkJoinCapacity(remoteNodes []pushNodeState) error {
	limit := m.MaxMembers()
	if limit <= 0 {
		return nil
	}

	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	alive := 0
	for _, n := range m.nodes {
		if !n.DeadOrLeft() {
			alive++
		}
	}
	if alive < limit {
		return nil
	}

	for _, rn := range remoteNodes {
		if rn.State != StateAlive {
			continue
		}
		existing, ok := m.nodeMap[rn.Name]
		if !ok || existing.DeadOrLeft() {
			return fmt.Errorf("%w: rejecting join from %q, members %d/%d",
				ErrClusterAtCapacity, rn.Name, alive, limit)
		}
	}
	return nil
}

// joiningNodeName makes a best effort to identify the node that initiated a
// join from the state it pushed: it is the live node we do not already know
// about, which is the node asking to be admitted.
func (m *Memberlist) joiningNodeName(remoteNodes []pushNodeState) string {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	for _, rn := range remoteNodes {
		if rn.State != StateAlive {
			continue
		}
		existing, ok := m.nodeMap[rn.Name]
		if !ok || existing.DeadOrLeft() {
			return rn.Name
		}
	}
	return ""
}
