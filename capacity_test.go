// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedAliveNodes injects n synthetic live members so that capacity behavior
// can be exercised at a realistic cluster scale instead of a toy two-node
// setup.
func seedAliveNodes(tb testing.TB, m *Memberlist, n int) {
	tb.Helper()
	for i := 0; i < n; i++ {
		a := alive{
			Node:        fmt.Sprintf("seed-%d", i),
			Addr:        net.IPv4(10, byte(i>>16), byte(i>>8), byte(i)).To4(),
			Port:        7946,
			Incarnation: 1,
			Vsn:         m.config.BuildVsnArray(),
		}
		m.aliveNode(&a, nil, true)
	}
}

func joinAddr(m *Memberlist) string {
	return fmt.Sprintf("%s/%s:%d", m.config.Name, m.config.BindAddr, m.config.BindPort)
}

func TestMemberlist_JoinCapacityLimit(t *testing.T) {
	// Exercise the limit at a realistic cluster scale.
	const limit = 256

	c1 := testConfig(t)
	c1.MaxMembers = limit
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, m1.Shutdown())
	}()

	// Fill the cluster to its limit: m1 itself plus limit-1 members.
	seedAliveNodes(t, m1, limit-1)
	require.Equal(t, limit, m1.NumMembers())

	c2 := testConfig(t)
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, m2.Shutdown())
	}()

	// At capacity the join must be refused with an explicit capacity
	// error, not a timeout.
	num, err := m2.Join([]string{joinAddr(m1)})
	require.Error(t, err)
	require.Equal(t, 0, num)
	require.Contains(t, err.Error(), "capacity")

	// The rejection must not disturb existing members on either side.
	require.Equal(t, limit, m1.NumMembers())
	require.Equal(t, 1, m2.NumMembers())

	// The decision must be recorded with the limit and current size.
	events := m1.JoinEvents()
	require.Len(t, events, 1)
	require.False(t, events[0].Admitted)
	require.Equal(t, c2.Name, events[0].Node)
	require.Equal(t, limit, events[0].MaxMembers)
	require.Equal(t, limit, events[0].NumMembers)
	require.Contains(t, events[0].Reason, "capacity")

	// Raising the limit admits the same node.
	m1.SetMaxMembers(2 * limit)
	num, err = m2.Join([]string{joinAddr(m1)})
	require.NoError(t, err)
	require.Equal(t, 1, num)

	require.Equal(t, limit+1, m1.NumMembers())
	events = m1.JoinEvents()
	require.Len(t, events, 2)
	require.True(t, events[1].Admitted)
	require.Equal(t, c2.Name, events[1].Node)
	require.Equal(t, 2*limit, events[1].MaxMembers)
	require.Equal(t, limit+1, events[1].NumMembers)
}

func TestMemberlist_JoinCapacityRejoin(t *testing.T) {
	// A node that is already a live member must be able to rejoin even
	// when the cluster is at its limit.
	const limit = 64

	c1 := testConfig(t)
	c1.MaxMembers = limit
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, m1.Shutdown())
	}()

	c2 := testConfig(t)
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, m2.Shutdown())
	}()

	num, err := m2.Join([]string{joinAddr(m1)})
	require.NoError(t, err)
	require.Equal(t, 1, num)

	// Fill the rest of the cluster up to the limit.
	seedAliveNodes(t, m1, limit-2)
	require.Equal(t, limit, m1.NumMembers())

	// Rejoining at the limit is allowed and does not grow the cluster.
	num, err = m2.Join([]string{joinAddr(m1)})
	require.NoError(t, err)
	require.Equal(t, 1, num)
	require.Equal(t, limit, m1.NumMembers())
}

func TestMemberlist_JoinNoLimitByDefault(t *testing.T) {
	// With no limit configured the join flow is unchanged.
	c1 := testConfig(t)
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, m1.Shutdown())
	}()
	require.Equal(t, 0, m1.MaxMembers())

	c2 := testConfig(t)
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, m2.Shutdown())
	}()

	num, err := m2.Join([]string{joinAddr(m1)})
	require.NoError(t, err)
	require.Equal(t, 1, num)
	require.Equal(t, 2, m1.NumMembers())

	events := m1.JoinEvents()
	require.Len(t, events, 1)
	require.True(t, events[0].Admitted)
	require.Equal(t, 0, events[0].MaxMembers)
}
