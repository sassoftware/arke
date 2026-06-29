// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package natsjs

import (
	"testing"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

func TestStreamNameFor(t *testing.T) {
	assert.Equal(t, "arke_events_orders", streamNameFor("events.orders"))
	assert.Equal(t, "arke_events_audit", streamNameFor("events.audit"))
	// illegal subject chars are sanitized
	assert.Equal(t, "arke_a_b_c", streamNameFor("a.b*c"))
}

func TestSubjectFor(t *testing.T) {
	// dotted topic key under an exchange root
	assert.Equal(t,
		"events.orders.region.us.order.created.success",
		subjectFor("events.orders", "region.us.order.created.success"))
	// AMQP '#' -> NATS '>'
	assert.Equal(t, "events.orders.region.>", subjectFor("events.orders", "region.#"))
	// AMQP '*' stays '*'
	assert.Equal(t, "events.orders.region.*.success", subjectFor("events.orders", "region.*.success"))
	// empty routing key -> capture-all under root
	assert.Equal(t, "events.orders.>", subjectFor("events.orders", ""))
}

// TestSubjectForSanitizes covers routing keys that are legal in AMQP but would
// otherwise yield an invalid NATS subject.
func TestSubjectForSanitizes(t *testing.T) {
	// non-terminal '#' has no exact NATS equivalent ('>' is tail-only) -> '*'
	assert.Equal(t, "events.orders.a.*.b", subjectFor("events.orders", "a.#.b"))
	// terminal '#' still maps to '>'
	assert.Equal(t, "events.orders.a.b.>", subjectFor("events.orders", "a.b.#"))
	// empty tokens (double dot / trailing dot) are collapsed, not left invalid
	assert.Equal(t, "events.orders.a.b", subjectFor("events.orders", "a..b"))
	assert.Equal(t, "events.orders.a.b", subjectFor("events.orders", "a.b."))
	// a routing key of only empty tokens -> capture-all under root
	assert.Equal(t, "events.orders.>", subjectFor("events.orders", "."))
	// illegal chars embedded in a literal token are replaced with '_'
	assert.Equal(t, "events.orders.a_b.c_d", subjectFor("events.orders", "a*b.c>d"))
	assert.Equal(t, "events.orders.a_b", subjectFor("events.orders", "a b"))
}

func TestStreamSubjectsFor(t *testing.T) {
	assert.Equal(t, []string{"events.orders.>"}, streamSubjectsFor("events.orders"))
	assert.Equal(t, []string{">"}, streamSubjectsFor(""))
}

func TestDurableName(t *testing.T) {
	q := func(name string) *pb.Source {
		return &pb.Source{Name: name, Address: &pb.Address{Name: "events.orders"}}
	}

	// non-transient QUEUE (default type, not auto-delete) -> durable
	assert.Equal(t, "arke_events_orders_shipping_listener",
		durableName(q("events.orders.shipping.listener")))

	// auto-delete (transient/exclusive conversion) -> ephemeral
	ad := q("sess-abc")
	ad.AutoDelete = true
	assert.Equal(t, "", durableName(ad))

	// exclusive -> ephemeral
	ex := q("sess-xyz")
	ex.Exclusive = true
	assert.Equal(t, "", durableName(ex))

	// TEMPORARY type -> ephemeral
	tmp := q("tmp")
	tmp.Type = pb.Source_TEMPORARY
	assert.Equal(t, "", durableName(tmp))

	// SingleActiveConsumer -> durable even if otherwise plain
	sac := q("sac")
	sac.SingleActiveConsumer = true
	assert.Equal(t, "arke_sac", durableName(sac))

	// STREAM with ConsumerGroup -> durable (named after the group)
	st := q("st")
	st.Type = pb.Source_STREAM
	st.Options = map[string]string{"ConsumerGroup": "grp.a"}
	assert.Equal(t, "arke_grp_a", durableName(st))

	// STREAM without ConsumerGroup -> ephemeral
	st2 := q("st2")
	st2.Type = pb.Source_STREAM
	assert.Equal(t, "", durableName(st2))
}

func TestEvaluateFilters(t *testing.T) {
	headers := map[string]string{"event-source": "orders", "region": "us"}

	// no filters -> always pass
	assert.True(t, evaluateFilters(nil, headers))

	// ALL: every match must hit
	all := &pb.Filter{
		Type: pb.Filter_ALL,
		Matches: []*pb.Match{
			{Name: "event-source", Value: "orders"},
			{Name: "region", Value: "us"},
		},
	}
	assert.True(t, evaluateFilters([]*pb.Filter{all}, headers))

	allMiss := &pb.Filter{
		Type: pb.Filter_ALL,
		Matches: []*pb.Match{
			{Name: "event-source", Value: "orders"},
			{Name: "region", Value: "eu"},
		},
	}
	assert.False(t, evaluateFilters([]*pb.Filter{allMiss}, headers))

	// ANY: a single hit passes
	any := &pb.Filter{
		Type: pb.Filter_ANY,
		Matches: []*pb.Match{
			{Name: "event-source", Value: "nope"},
			{Name: "region", Value: "us"},
		},
	}
	assert.True(t, evaluateFilters([]*pb.Filter{any}, headers))

	// multiple filters are OR'd: second filter matches
	assert.True(t, evaluateFilters([]*pb.Filter{allMiss, all}, headers))
}
