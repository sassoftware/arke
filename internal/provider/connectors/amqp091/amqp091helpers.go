// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"crypto/tls"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// contains more easily unit testable code from the shim

// NewAmqp091Connection Create a new Amqp091Connection object with a connection string and tls config
func NewAmqp091Connection(connStr string, clientIdentifier string, tlsCfg *tls.Config) amqp091ConnectionShim {
	return &amqp091Connection{connStr: connStr, tlsCfg: tlsCfg, clientIdentifier: clientIdentifier}
}

func amqpConfig(connName string, tlsCfg *tls.Config) amqp.Config {
	properties := make(amqp.Table)
	properties["connection_name"] = connName
	cfg := amqp.Config{
		TLSClientConfig: tlsCfg,
		Heartbeat:       10 * time.Second,
		Locale:          "en_US",
		Properties:      properties,
	}
	return cfg
}

func toAmqpTable(at amqp091Table) amqp.Table {
	table := make(amqp.Table)
	for key, val := range at {
		table[key] = val
	}
	return table
}

func fromAmqpTable(tab amqp.Table) amqp091Table {
	table := make(amqp091Table)
	for key, val := range tab {
		table[key] = val
	}
	return table
}

func fromTableToMap(tab amqp091Table) map[string]string {
	table := make(map[string]string)
	for key, val := range tab {
		table[key] = fmt.Sprintf("%v", val)
	}
	return table
}

func toAmqpMessage(msg *amqp091Message) amqp.Publishing {
	pub := amqp.Publishing{}
	pub.Body = msg.Body
	pub.DeliveryMode = uint8(msg.DeliveryMode) //nolint:gosec
	pub.Headers = toAmqpTable(msg.Headers)
	pub.ContentType = msg.ContentType
	pub.ContentEncoding = msg.ContentEncoding
	pub.Timestamp = time.Now()
	return pub
}

func fromAmqpMessage(del amqp.Delivery) amqp091Message {
	msg := amqp091Message{}
	msg.SetDelivery(del)
	msg.Body = del.Body
	msg.DeliveryMode = int(del.DeliveryMode)
	msg.Headers = fromAmqpTable(del.Headers)
	msg.ContentType = del.ContentType
	msg.ContentEncoding = del.ContentEncoding
	msg.DeliveryTag = del.DeliveryTag
	return msg
}
