// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package provider

import "github.com/sassoftware/arke/internal/util"

// ClientStatsFromProvider maps metrics shared by the AMQP providers to the
// existing client stats API. Provider-specific metrics are added by callers.
func ClientStatsFromProvider(clientIdentifier string, metrics ClientStatsProvider) *ClientStats {
	return &ClientStats{
		ID:             clientIdentifier,
		ActiveMessages: metrics.GetActiveMessagesCount(),
		Produced:       metrics.GetMessagesProduced(),
		Consumed:       metrics.GetMessagesConsumed(),
	}
}

// CollectClientStats builds provider stats in client identifier order, omitting
// clients whose provider-specific stats could not be collected.
func CollectClientStats(clientIdentifiers []string, collect func(string) *ClientStats) *Stats {
	stats := &Stats{Clients: make([]*ClientStats, 0, len(clientIdentifiers))}
	for _, clientIdentifier := range clientIdentifiers {
		if clientStats := collect(clientIdentifier); clientStats != nil {
			stats.Clients = append(stats.Clients, clientStats)
		}
	}
	return stats
}

func SleepRandomReconnect() {
	util.SleepRandom(100, ReconnectDelay)
}
