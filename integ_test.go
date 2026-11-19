// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

// CheckInteg will skip a test if integration testing is not enabled.
func CheckInteg(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}
}

// IsInteg returns a boolean telling you if we're in integ testing mode.
func IsInteg() bool {
	return os.Getenv("INTEG_TESTS") != ""
}

// Tests the memberlist by creating a cluster of 100 nodes
// and checking that we get strong convergence of changes.
func TestMemberlist_Integ(t *testing.T) {
	CheckInteg(t)

	num := 16
	var members []*Memberlist

	secret := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	eventCh := make(chan NodeEvent, num)

	addr := "127.0.0.1"
	for i := range num {
		c := DefaultLANConfig()
		c.Name = fmt.Sprintf("%s:%d", addr, 12345+i)
		c.BindAddr = addr
		c.BindPort = 12345 + i
		c.ProbeInterval = 20 * time.Millisecond
		c.ProbeTimeout = 100 * time.Millisecond
		c.GossipInterval = 20 * time.Millisecond
		c.PushPullInterval = 200 * time.Millisecond
		c.SecretKey = secret
		c.Logger = log.New(os.Stderr, c.Name, log.LstdFlags)

		if i == 0 {
			c.Events = &ChannelEventDelegate{eventCh}
		}

		m, err := Create(c)
		if err != nil {
			t.Fatalf("unexpected err: %s", err)
		}
		defer func() {
			if err := m.Shutdown(); err != nil {
				t.Fatal(err)
			}
		}()

		members = append(members, m)

		if i > 0 {
			last := members[i-1]
			num, err := m.Join([]string{last.config.Name + "/" + last.config.Name})
			if num == 0 || err != nil {
				t.Fatalf("unexpected err: %s", err)
			}
		}
	}

	// Wait and print debug info
	breakTimer := time.After(250 * time.Millisecond)
WAIT:
	for {
		select {
		case e := <-eventCh:
			if e.Event == NodeJoin {
				t.Logf("[DEBUG] Node join: %v (%d)", *e.Node, members[0].NumMembers())
			} else {
				t.Logf("[DEBUG] Node leave: %v (%d)", *e.Node, members[0].NumMembers())
			}
		case <-breakTimer:
			break WAIT
		}
	}

	for idx, m := range members {
		got := m.NumMembers()
		if got != num {
			t.Errorf("bad num members at idx %d. Expected %d. Got %d.",
				idx, num, got)
		}
	}
}

// TestMemberlist_Integ_Readiness builds a cluster, then joins an extra node
// whose application-state synchronization takes a long time (modeled by
// leaving it not-ready). The new node must appear in the member list on every
// peer while still being excluded from the serviceable set, then become
// serviceable cluster-wide once it finishes synchronizing and calls SetReady.
func TestMemberlist_Integ_Readiness(t *testing.T) {
	CheckInteg(t)

	const clusterSize = 8
	const totalSize = clusterSize + 1
	var members []*Memberlist

	secret := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	addr := "127.0.0.1"

	create := func(i int) *Memberlist {
		c := DefaultLANConfig()
		c.Name = fmt.Sprintf("%s:%d", addr, 13345+i)
		c.BindAddr = addr
		c.BindPort = 13345 + i
		c.ProbeInterval = 20 * time.Millisecond
		c.ProbeTimeout = 100 * time.Millisecond
		c.GossipInterval = 20 * time.Millisecond
		c.PushPullInterval = 200 * time.Millisecond
		c.SecretKey = secret
		c.Logger = log.New(os.Stderr, c.Name, log.LstdFlags)

		m, err := Create(c)
		if err != nil {
			t.Fatalf("unexpected err: %s", err)
		}
		t.Cleanup(func() {
			if err := m.Shutdown(); err != nil {
				t.Fatal(err)
			}
		})
		return m
	}

	for i := range clusterSize {
		m := create(i)
		members = append(members, m)
		if i > 0 {
			last := members[i-1]
			joined, err := m.Join([]string{last.config.Name + "/" + last.config.Name})
			if joined == 0 || err != nil {
				t.Fatalf("unexpected err: %s", err)
			}
		}
		if err := m.SetReady(true); err != nil {
			t.Fatalf("unexpected err: %s", err)
		}
	}

	// Wait for the base cluster to converge and become fully serviceable.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		converged := true
		for _, m := range members {
			if m.NumMembers() != clusterSize || m.NumServiceMembers() != clusterSize {
				converged = false
				break
			}
		}
		if converged {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	for idx, m := range members {
		if got := m.NumServiceMembers(); got != clusterSize {
			t.Fatalf("node %d expected %d serviceable members, got %d", idx, clusterSize, got)
		}
	}

	// Join an extra node that is still synchronizing (not ready). Membership
	// converges first, readiness lags behind.
	joining := create(clusterSize)
	last := members[clusterSize-1]
	joined, err := joining.Join([]string{last.config.Name + "/" + last.config.Name})
	if joined == 0 || err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	members = append(members, joining)

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		converged := true
		for _, m := range members {
			if m.NumMembers() != totalSize {
				converged = false
				break
			}
		}
		if converged {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Every peer sees the joining node in the member list but none routes
	// traffic to it while its synchronization is incomplete.
	for idx, m := range members {
		if got := m.NumMembers(); got != totalSize {
			t.Fatalf("node %d expected %d members, got %d", idx, totalSize, got)
		}
		if m.IsServiceable(joining.config.Name) {
			t.Fatalf("node %d routed traffic to not-ready node %s",
				idx, joining.config.Name)
		}
		if got := m.NumServiceMembers(); got != clusterSize {
			t.Fatalf("node %d expected %d serviceable during sync, got %d",
				idx, clusterSize, got)
		}
	}

	// Synchronization completes; the node becomes serviceable everywhere.
	if err := joining.SetReady(true); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		converged := true
		for _, m := range members {
			if m.NumServiceMembers() != totalSize ||
				!m.IsServiceable(joining.config.Name) {
				converged = false
				break
			}
		}
		if converged {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	for idx, m := range members {
		if got := m.NumServiceMembers(); got != totalSize {
			t.Fatalf("node %d expected %d serviceable after ready, got %d",
				idx, totalSize, got)
		}
	}
}
