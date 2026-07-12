// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package natsjs

import (
	"strings"
	"testing"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

func TestStreamNameFor(t *testing.T) {
	assert.Equal(t, "arke_events_orders", streamNameFor("events.orders"))
	assert.Equal(t, "arke_events_audit", streamNameFor("events.audit"))
	// illegal subject chars are sanitized
	assert.Equal(t, "arke_a_b_c", streamNameFor("a.b*c"))
	assert.Equal(t, "arke_", streamNameFor(""))
}

func TestAddressRoot(t *testing.T) {
	assert.Equal(t, "events.orders", addressRoot("events.orders"))
	assert.Equal(t, "", addressRoot(""))
	// empty tokens are kept as placeholders, not dropped, so "a..b" and "a.b"
	// stay distinct roots
	assert.Equal(t, "a._.b", addressRoot("a..b"))
	// the reserved delimiter token is sanitized out of address names
	assert.Equal(t, "a._.b", addressRoot("a.~.b"))
	// illegal subject chars inside tokens are replaced
	assert.Equal(t, "a.b_c", addressRoot("a.b*c"))
}

func TestPublishSubjectFor(t *testing.T) {
	// dotted topic key under an exchange root, behind the delimiter
	assert.Equal(t,
		"events.orders.~.region.us.order.created.success",
		publishSubjectFor("events.orders", "region.us.order.created.success"))
	// empty routing key -> the bare prefix (a concrete, publishable subject)
	assert.Equal(t, "events.orders.~", publishSubjectFor("events.orders", ""))
	// empty address -> subjects live directly under the delimiter
	assert.Equal(t, "~.a.b", publishSubjectFor("", "a.b"))
	assert.Equal(t, "~", publishSubjectFor("", ""))
	// wildcard chars are literal in a published routing key and NATS forbids
	// them in publish subjects -> sanitized
	assert.Equal(t, "events.orders.~.a._.b", publishSubjectFor("events.orders", "a.#.b"))
	assert.Equal(t, "events.orders.~.a._", publishSubjectFor("events.orders", "a.*"))
	// empty tokens (double dot / trailing dot) are collapsed, not left invalid
	assert.Equal(t, "events.orders.~.a.b", publishSubjectFor("events.orders", "a..b"))
	assert.Equal(t, "events.orders.~.a.b", publishSubjectFor("events.orders", "a.b."))
	assert.Equal(t, "events.orders.~", publishSubjectFor("events.orders", "."))
	// illegal chars embedded in a literal token are replaced with '_'
	assert.Equal(t, "events.orders.~.a_b.c_d", publishSubjectFor("events.orders", "a*b.c>d"))
	assert.Equal(t, "events.orders.~.a_b", publishSubjectFor("events.orders", "a b"))
}

func TestStreamSubjectsFor(t *testing.T) {
	assert.Equal(t,
		[]string{"events.orders.~", "events.orders.~.>"},
		streamSubjectsFor("events.orders"))
	assert.Equal(t, []string{"~", "~.>"}, streamSubjectsFor(""))
}

// TestStreamSubjectsDisjointForPrefixAddresses is the regression test for the
// address-prefix collision: without the delimiter, "events.orders" and
// "events.orders.filtered" produce overlapping stream subjects and the
// second stream can never be created (JetStream err 10065).
func TestStreamSubjectsDisjointForPrefixAddresses(t *testing.T) {
	parent := streamSubjectsFor("events.orders")
	child := streamSubjectsFor("events.orders.filtered")
	for _, p := range parent {
		for _, c := range child {
			assert.False(t, subjectsOverlap(p, c), "%q overlaps %q", p, c)
		}
	}
	// an address token spelled "~" cannot fake its way into the parent's space
	evil := streamSubjectsFor("events.orders.~")
	for _, p := range parent {
		for _, c := range evil {
			assert.False(t, subjectsOverlap(p, c), "%q overlaps %q", p, c)
		}
	}
}

// subjectsOverlap reports whether two subject patterns can match a common
// subject (token-wise; '>' matches one or more trailing tokens, '*' exactly
// one).
func subjectsOverlap(a, b string) bool {
	at, bt := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; ; i++ {
		switch {
		case i == len(at) && i == len(bt):
			return true
		case i == len(at) || i == len(bt):
			return false
		case at[i] == ">" || bt[i] == ">":
			return true
		case at[i] == "*" || bt[i] == "*":
			continue
		case at[i] != bt[i]:
			return false
		}
	}
}

func TestFilterSubjectsFor(t *testing.T) {
	src := func(subjects ...string) *pb.Source {
		return &pb.Source{Address: &pb.Address{Name: "events.orders", Subjects: subjects}}
	}

	// no patterns -> everything under the address, including the empty key
	assert.Equal(t,
		[]string{"events.orders.~", "events.orders.~.>"},
		filterSubjectsFor(src()))
	// plain patterns map one to one
	assert.Equal(t,
		[]string{"events.orders.~.created", "events.orders.~.region.*.updated"},
		filterSubjectsFor(src("created", "region.*.updated")))
	// AMQP '#' matches zero or more words -> both the '>' form and the
	// zero-word base are included
	assert.Equal(t,
		[]string{"events.orders.~.region", "events.orders.~.region.>"},
		filterSubjectsFor(src("region.#")))
	// a bare '#' pattern behaves like the empty pattern
	assert.Equal(t,
		[]string{"events.orders.~", "events.orders.~.>"},
		filterSubjectsFor(src("#")))
	// duplicates collapse
	assert.Equal(t,
		[]string{"events.orders.~", "events.orders.~.>"},
		filterSubjectsFor(src("#", "")))
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

	// SingleActiveConsumer with a ConsumerGroup -> the group is the
	// coordination identity, so it names the durable (amqp091 uses the group
	// as the single-active consumer reference the same way). Sources sharing
	// a name but split into groups must NOT collapse onto one durable.
	sacg := q("sac")
	sacg.SingleActiveConsumer = true
	sacg.Options = map[string]string{"ConsumerGroup": "grp.b"}
	assert.Equal(t, "arke_grp_b", durableName(sacg))

	// single-active STREAM source without a ConsumerGroup: no coordination
	// identity exists; Subscribe rejects it (amqp091 parity), and there is no
	// durable name for it
	sacs := q("sacs")
	sacs.SingleActiveConsumer = true
	sacs.Type = pb.Source_STREAM
	assert.Equal(t, "", durableName(sacs))

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
