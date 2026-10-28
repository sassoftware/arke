// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectClientStats(t *testing.T) {
	t.Run("empty client list returns non-nil empty stats", func(t *testing.T) {
		stats := CollectClientStats(nil, func(string) *ClientStats {
			t.Fatal("collector should not be called for an empty client list")
			return nil
		})

		require.NotNil(t, stats)
		require.NotNil(t, stats.Clients)
		assert.Empty(t, stats.Clients)
	})

	t.Run("preserves identifier order and skips missing stats", func(t *testing.T) {
		first := &ClientStats{ID: "first", ActiveMessages: 1, Streams: 2, Produced: 3, Consumed: 4}
		last := &ClientStats{ID: "last", ActiveMessages: 5, Streams: 6, Produced: 7, Consumed: 8}
		var collected []string

		stats := CollectClientStats([]string{"first", "missing", "last"}, func(clientIdentifier string) *ClientStats {
			collected = append(collected, clientIdentifier)
			switch clientIdentifier {
			case "first":
				return first
			case "last":
				return last
			default:
				return nil
			}
		})

		require.Equal(t, []string{"first", "missing", "last"}, collected)
		require.Len(t, stats.Clients, 2)
		assert.Same(t, first, stats.Clients[0])
		assert.Same(t, last, stats.Clients[1])
		assert.NotContains(t, stats.Clients, (*ClientStats)(nil))
	})
}
