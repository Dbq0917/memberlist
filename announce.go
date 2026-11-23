// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/go-multierror"
)

// ErrAnnounceNoTargets is returned when Announce is called without any known
// addresses to report to.
var ErrAnnounceNoTargets = errors.New("memberlist: no known addresses to announce to")

// Announce proactively reports this node's presence to the given peers by
// sending each of them a direct, best-effort alive notification. Unlike Join,
// it does not perform a state exchange and does not require the peers to run
// any newer version of the protocol: the notification is an ordinary alive
// gossip message, which every memberlist node already understands. This lets a
// newly started node become a known member immediately instead of waiting for
// the next gossip or push/pull cycle.
//
// The existing discovery flow is unchanged. Announce only adds an extra,
// explicit notification; gossip, probing and push/pull keep converging the
// membership exactly as before. Failures are non-fatal: a missed notification
// is simply retried by the background gossip, mirroring the eventual
// consistency guarantees of the rest of the protocol.
//
// It returns the number of peers that accepted the notification.
func (m *Memberlist) Announce(targets []string) (int, error) {
	if m.hasShutdown() {
		return 0, errors.New("memberlist: announce called after shutdown")
	}
	if len(targets) == 0 {
		return 0, ErrAnnounceNoTargets
	}

	msg, err := m.localAliveMessage()
	if err != nil {
		return 0, err
	}
	payload, err := encode(aliveMsg, msg, m.config.MsgpackUseNewTimeFormat)
	if err != nil {
		return 0, fmt.Errorf("memberlist: failed to encode announce message: %w", err)
	}

	numSuccess := 0
	var errs error
	for _, target := range targets {
		addrs, err := m.resolveAddr(target)
		if err != nil {
			err = fmt.Errorf("failed to resolve announce target %s: %v", target, err)
			m.logger.Printf("[WARN] memberlist: %v", err)
			errs = multierror.Append(errs, err)
			continue
		}

		for _, addr := range addrs {
			hp := joinHostPort(addr.ip.String(), addr.port)
			a := Address{Addr: hp, Name: addr.nodeName}
			if err := m.sendMsg(a, payload.Bytes()); err != nil {
				err = fmt.Errorf("failed to announce to %s: %v", hp, err)
				m.logger.Printf("[DEBUG] memberlist: %v", err)
				errs = multierror.Append(errs, err)
				continue
			}
			m.logger.Printf("[INFO] memberlist: Announced presence to %s as %s (%s)",
				hp, msg.Node, net.IP(msg.Addr))
			numSuccess++
		}
	}

	if numSuccess > 0 {
		return numSuccess, nil
	}
	return 0, errs
}

// localAliveMessage builds the alive message describing this node, identical
// to the one setAlive registers locally and to what gossip would propagate.
func (m *Memberlist) localAliveMessage() (alive, error) {
	m.nodeLock.RLock()
	state, ok := m.nodeMap[m.config.Name]
	m.nodeLock.RUnlock()
	if !ok {
		return alive{}, errors.New("memberlist: local node is not alive")
	}

	return alive{
		Incarnation: state.Incarnation,
		Node:        state.Name,
		Addr:        []byte(state.Addr),
		Port:        state.Port,
		Meta:        state.Meta,
		Vsn: []uint8{
			state.PMin, state.PMax, state.PCur,
			state.DMin, state.DMax, state.DCur,
		},
	}, nil
}

// scheduleAnnounce fires off a best-effort, background Announce against the
// configured known addresses as soon as the node starts. Retries absorb the
// common case where a peer is briefly unreachable during startup, without
// blocking Create or changing the passive discovery flow.
func (m *Memberlist) scheduleAnnounce() {
	if len(m.config.AnnounceAddrs) == 0 {
		return
	}

	interval := m.config.AnnounceInterval
	if interval <= 0 {
		interval = time.Second
	}
	attempts := m.config.AnnounceRetries
	if attempts <= 0 {
		attempts = 3
	}

	targets := append([]string(nil), m.config.AnnounceAddrs...)
	go func() {
		for attempt := 1; attempt <= attempts; attempt++ {
			if m.hasShutdown() {
				return
			}
			m.logger.Printf("[INFO] memberlist: Announcing presence to %d known peer(s), attempt %d/%d",
				len(targets), attempt, attempts)
			n, err := m.Announce(targets)
			if err == nil {
				m.logger.Printf("[INFO] memberlist: Announce succeeded, %d peer(s) notified", n)
				return
			}
			m.logger.Printf("[WARN] memberlist: Announce attempt %d/%d notified %d peer(s): %v",
				attempt, attempts, n, err)
			if attempt == attempts {
				return
			}

			select {
			case <-m.shutdownCh:
				return
			case <-time.After(interval):
			}
		}
	}()
}
