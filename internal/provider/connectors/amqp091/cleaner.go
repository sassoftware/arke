// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"time"

	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

var cleanInterval = 30 * time.Second

// every 30 seconds check the list of active connections
// if a client has 0 active streams and hasn't created or
// deleted a stream in over 30 seconds, disconnect it.
// Severed client connections may hang around for up to 60
// seconds since we are checking every 30.
func connectionCleaner(ctx context.Context) {
	provy, _ := provider.GetProvider("amqp091")
	prov := provy.(*amqp091provider)
	ticker := time.NewTicker(cleanInterval)
	for {
		select {
		case <-ctx.Done():
			util.Logger.Debug("Connection cleaner received shutdown signal. Exiting.")
			ticker.Stop()
			return
		case <-ticker.C:
			for _, connID := range prov.connections.GetList() {
				if conn, ok := prov.connections.Get(connID); ok {
					bd := conn.(*BrokerDetails)
					util.Logger.Tracef("Client %v has %d open streams", connID, bd.ActiveStreams)
					lastKnown := time.Since(bd.lastPubSubEvent)
					if bd.ActiveStreams < 1 && lastKnown > cleanInterval {
						util.Logger.Debugf("Client %v has had no streams open for %v. Assuming dead. Disconnecting.", connID, lastKnown)
						prov.disconnectClientByIdentifier(connID)
					}
				}
			}
		}
	}
}
