// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package connectors

import (
	_ "github.com/sassoftware/arke/internal/provider/connectors/amqp091" // Import the AMQP091 plugin
	_ "github.com/sassoftware/arke/internal/provider/connectors/natsjs"  // Import the NATS JetStream plugin
)
