// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package natsjs

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamNameFor(t *testing.T) {
	assert.Equal(t, "arke_events_orders", streamNameFor("events.orders"))
	assert.Equal(t, "arke_events_audit", streamNameFor("events.audit"))
	assert.Equal(t, "arke_", streamNameFor(""))

	// Names whose tokens contain '_' take the escaped "arke-" form so they
	// cannot collide with a dotted sibling: under the plain
	// dots-to-underscores replacement "a.b" and "a_b" would both read
	// "arke_a_b" and clobber each other's stream.
	assert.Equal(t, "arke_a_b", streamNameFor("a.b"))
	assert.Equal(t, "arke-a_ub", streamNameFor("a_b"))
	assert.Equal(t, "arke-a_ub_dc", streamNameFor("a_b.c"))
	// subject-escaped chars ('*' -> "~a") stay in the plain name form — the
	// escaped root contains '~', not '_', and '~' is JetStream-name-legal
	assert.Equal(t, "arke_a_~b~ac", streamNameFor("a.b*c"))
	// characters JetStream rejects in names but allows in subjects are escaped
	assert.Equal(t, "arke-a_sb", streamNameFor("a/b"))
	assert.Equal(t, "arke-a_bb", streamNameFor(`a\b`))

	// The mapping must be injective: distinct address roots may never share a
	// stream name, or the two addresses silently reconfigure each other's
	// stream. These are the collision pairs of the plain replacement.
	collisionProne := []string{"a.b", "a_b", "a.b.c", "a_b.c", "a.b_c", "a_b_c", "a/b", `a\b`}
	seen := map[string]string{}
	for _, addr := range collisionProne {
		name := streamNameFor(addr)
		if prev, ok := seen[name]; ok {
			t.Errorf("streamNameFor collision: %q and %q both map to %q", prev, addr, name)
		}
		seen[name] = addr
		assert.NotContains(t, name, ".", "stream name must be JetStream-legal")
		assert.NotContains(t, name, "/", "stream name must be JetStream-legal")
		assert.NotContains(t, name, `\`, "stream name must be JetStream-legal")
	}
}

func TestAddressRoot(t *testing.T) {
	assert.Equal(t, "events.orders", addressRoot("events.orders"))
	assert.Equal(t, "", addressRoot(""))
	// empty tokens are kept as escaped placeholders, not dropped, so "a..b"
	// and "a.b" stay distinct roots
	assert.Equal(t, "a.~e.b", addressRoot("a..b"))
	// the reserved delimiter token is escaped, never left bare
	assert.Equal(t, "a.~~~.b", addressRoot("a.~.b"))
	// illegal subject chars inside tokens are escaped
	assert.Equal(t, "a.~b~ac", addressRoot("a.b*c"))
	// tokens free of reserved characters — every conventional name — pass
	// through unchanged, '_' included
	assert.Equal(t, "a._.b-c", addressRoot("a._.b-c"))
}

// TestAddressRootInjective is the regression test for the root collapse: a
// lossy replacement mapped "a.~.b", "a..b", "a. .b" and "a.*.b" all onto
// "a._.b", so those distinct addresses shared one subject space and one
// stream — cross-address message leakage plus mutual stream reconfiguration.
func TestAddressRootInjective(t *testing.T) {
	collisionProne := []string{
		"a._.b", "a.~.b", "a..b", "a. .b", "a.\t.b", "a.*.b", "a.>.b",
		"a.#.b", "a.~~.b", "a.~e.b", "a b", "a_b", "a*b", "a\rb",
	}
	seen := map[string]string{}
	for _, addr := range collisionProne {
		root := addressRoot(addr)
		if prev, ok := seen[root]; ok {
			t.Errorf("addressRoot collision: %q and %q both map to %q", prev, addr, root)
		}
		seen[root] = addr
		for _, tok := range strings.Split(root, ".") {
			assert.NotEqual(t, addressDelim, tok, "no root token may equal the delimiter (%q)", addr)
			assert.NotContainsf(t, tok, " ", "root token must be subject-legal (%q)", addr)
			assert.False(t, tok == "" || tok == "*" || tok == ">",
				"root token must be a concrete subject token (%q -> %q)", addr, root)
		}
	}
	// pairwise: distinct addresses -> disjoint stream subject spaces and
	// distinct stream names (the composed guarantee the delimiter and the
	// two escape layers exist to provide)
	addrs := collisionProne
	for i := range addrs {
		for j := i + 1; j < len(addrs); j++ {
			for _, p := range streamSubjectsFor(addrs[i]) {
				for _, c := range streamSubjectsFor(addrs[j]) {
					assert.False(t, subjectsOverlap(p, c),
						"addresses %q and %q share subject space (%q vs %q)", addrs[i], addrs[j], p, c)
				}
			}
			assert.NotEqual(t, streamNameFor(addrs[i]), streamNameFor(addrs[j]),
				"addresses %q and %q share a stream name", addrs[i], addrs[j])
		}
	}
}

func TestEscapeTokenRoundTrip(t *testing.T) {
	for _, tok := range []string{
		"", "plain", "with_underscore", "~", "~~", "~e", "a b", "a\tb",
		"a*b", "a>b", "a#b", "a~b", "#", "*", ">", " ", "mixed ~*># end",
		"uni-cödé", "uni cödé",
	} {
		esc := escapeToken(tok)
		assert.Equal(t, tok, unescapeToken(esc), "round trip of %q via %q", tok, esc)
		assert.NotEqual(t, addressDelim, esc, "escaped token may not equal the delimiter")
		assert.NotContainsf(t, esc, " ", "escaped token must be subject-legal (%q)", tok)
	}
	// tokens the connector never produced pass through the decoder unchanged
	// (unknown escape codes are kept literally, best effort)
	assert.Equal(t, "plain", unescapeToken("plain"))
	assert.Equal(t, "x~q", unescapeToken("~x~q"))
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
	// them in publish subjects -> escaped (injectively: "a.#.b" and "a._.b"
	// must not merge onto one subject)
	assert.Equal(t, "events.orders.~.a.~~h.b", publishSubjectFor("events.orders", "a.#.b"))
	assert.Equal(t, "events.orders.~.a.~~a", publishSubjectFor("events.orders", "a.*"))
	// empty tokens (double dot / trailing dot) are collapsed, not left invalid
	assert.Equal(t, "events.orders.~.a.b", publishSubjectFor("events.orders", "a..b"))
	assert.Equal(t, "events.orders.~.a.b", publishSubjectFor("events.orders", "a.b."))
	assert.Equal(t, "events.orders.~", publishSubjectFor("events.orders", "."))
	// illegal chars embedded in a literal token are escaped in place
	assert.Equal(t, "events.orders.~.~a~ab.~c~gd", publishSubjectFor("events.orders", "a*b.c>d"))
	assert.Equal(t, "events.orders.~.~a~wb", publishSubjectFor("events.orders", "a b"))
	// distinct routing keys never merge: "a b" vs "a_b", "#" vs "_" vs "*"
	distinct := []string{"a b", "a_b", "#", "_", "*", "~", "~e"}
	seen := map[string]string{}
	for _, rk := range distinct {
		s := publishSubjectFor("events.orders", rk)
		if prev, ok := seen[s]; ok {
			t.Errorf("publish subject collision: %q and %q both map to %q", prev, rk, s)
		}
		seen[s] = rk
	}
}

func TestRoutingKeyFromSubject(t *testing.T) {
	// inverse of publishSubjectFor
	assert.Equal(t, "region.us.created",
		routingKeyFromSubject("events.orders.~.region.us.created"))
	// the bare prefix is the empty routing key
	assert.Equal(t, "", routingKeyFromSubject("events.orders.~"))
	// a subject with no delimiter at all is not one the connector produced
	assert.Equal(t, "", routingKeyFromSubject("events.orders"))
	// empty address: subjects live directly under the delimiter
	assert.Equal(t, "a.b", routingKeyFromSubject("~.a.b"))
	// A message routed in from a parent address keeps the subject it was
	// published under, so it is rooted at the PARENT while the source that
	// consumes it names the child. The key still has to come back: RabbitMQ's
	// DLX move preserves the original routing key, and a dead-letter here is
	// the proxy-side stand-in for that move.
	//
	// Regression: this stripped the consuming address's own prefix, which a
	// parent-rooted subject never matches, so every message routed in from a
	// parent dead-lettered under an empty key.
	assert.Equal(t, "region.us.created",
		routingKeyFromSubject(publishSubjectFor("events.parent", "region.us.created")))
	// escaped tokens decode back to the published key, so a dead-letter
	// republish of the recovered key lands on the same tokens (round trip:
	// rk -> subject -> rk for keys with reserved characters). The delimiter
	// scan must survive a key that itself contains a literal '~', which
	// escapes to "~~" and so is never mistaken for the delimiter token.
	for _, rk := range []string{"a b.c", "x.~.y", "#", "a*b", "region.us", "~"} {
		subj := publishSubjectFor("events.orders", rk)
		assert.Equal(t, rk, routingKeyFromSubject(subj), "via %q", subj)
	}
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

	// No patterns -> no bindings -> nothing selected, like amqp091 declaring
	// no binding at all. The consumer still needs one filter subject, so it
	// gets an unmatchable one.
	assert.Equal(t,
		[]string{"events.orders.~." + unmatchableToken},
		filterSubjectsFor(src()))
	// An empty pattern is a literal empty routing key on a topic address, not
	// a catch-all.
	assert.Equal(t,
		[]string{"events.orders.~"},
		filterSubjectsFor(src("")))
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
	// consecutive '#'s are one '#' in AMQP (each matches zero or more words):
	// "#.#" must still match the empty routing key, and "a.#.#" must not lose
	// zero-or-more to the non-terminal-'#' narrowing
	assert.Equal(t,
		[]string{"events.orders.~", "events.orders.~.>"},
		filterSubjectsFor(src("#.#")))
	assert.Equal(t,
		[]string{"events.orders.~.a", "events.orders.~.a.>"},
		filterSubjectsFor(src("a.#.#")))
	assert.Equal(t,
		[]string{"events.orders.~.*.b"},
		filterSubjectsFor(src("#.#.b")))
	// duplicates collapse
	assert.Equal(t,
		[]string{"events.orders.~", "events.orders.~.>"},
		filterSubjectsFor(src("#", "")))
	// redundant bindings collapse to the widest filters: JetStream rejects a
	// consumer whose filter subjects overlap, while AMQP happily routes a
	// message once however many bindings match
	assert.Equal(t,
		[]string{"events.orders.~", "events.orders.~.>"},
		filterSubjectsFor(src("#", "orders.created")))
	assert.Equal(t,
		[]string{"events.orders.~.region", "events.orders.~.region.>"},
		filterSubjectsFor(src("region.#", "region.us", "region.us.updated")))
	assert.Equal(t,
		[]string{"events.orders.~.*"},
		filterSubjectsFor(src("*", "created")))
	// intersecting patterns where neither contains the other both survive
	// (JetStream accepts them)
	assert.Equal(t,
		[]string{"events.orders.~.*.b", "events.orders.~.a.*"},
		filterSubjectsFor(src("*.b", "a.*")))
}

func TestDirectFilterSubjectsAreExact(t *testing.T) {
	src := func(subjects ...string) *pb.Source {
		return &pb.Source{Address: &pb.Address{Name: "events.direct", Type: pb.Address_QUEUE, Subjects: subjects}}
	}

	assert.Equal(t,
		[]string{"events.direct.~.~~h"},
		filterSubjectsFor(src("#")),
		"direct bindings treat # as a literal routing key, not a wildcard")
	// ...and the literal '#' binding must not also match published "_" or
	// "*" (the lossy sanitizer merged all three onto "_")
	assert.NotEqual(t, filterSubjectsFor(src("#"))[0], publishSubjectFor("events.direct", "_"))
	assert.NotEqual(t, filterSubjectsFor(src("#"))[0], publishSubjectFor("events.direct", "*"))
	assert.Equal(t, filterSubjectsFor(src("#"))[0], publishSubjectFor("events.direct", "#"))
	assert.Equal(t,
		[]string{"events.direct.~.created", "events.direct.~.region.us"},
		filterSubjectsFor(src("created", "region.us", "created")))
	// No subjects binds nothing, exactly as on a direct exchange.
	assert.Equal(t,
		[]string{"events.direct.~." + unmatchableToken},
		filterSubjectsFor(src()))
	// An explicit empty binding key selects the empty routing key only.
	assert.Equal(t,
		[]string{"events.direct.~"},
		filterSubjectsFor(src("")))
}

func TestSubjectSubsumes(t *testing.T) {
	cover := subjectSubsumes

	assert.True(t, cover("a.>", "a.b"))
	assert.True(t, cover("a.>", "a.b.c"))
	assert.True(t, cover("a.>", "a.b.>"))
	assert.True(t, cover("a.>", "a.*"))
	assert.True(t, cover("a.*", "a.b"))
	assert.True(t, cover("a.*.c", "a.b.c"))
	assert.True(t, cover(">", "anything.at.all"))

	// '>' needs at least one token, so it does not cover the bare base
	assert.False(t, cover("a.>", "a"))
	// a bounded pattern cannot cover an unbounded one
	assert.False(t, cover("a.*", "a.>"))
	assert.False(t, cover("a.b.>", "a.>"))
	// '*' does not cover a different literal position count
	assert.False(t, cover("a.*", "a.b.c"))
	assert.False(t, cover("a.b", "a"))
	assert.False(t, cover("a", "a.b"))
	// literal mismatch
	assert.False(t, cover("a.b", "a.c"))
	// a literal never covers a wildcard
	assert.False(t, cover("a.b", "a.*"))
	// intersecting but neither is a subset
	assert.False(t, cover("*.b", "a.*"))
	assert.False(t, cover("a.*", "*.b"))
}

// TestDLQSourceName: the name has to match the amqp091 connector's byte for
// byte. It is the name clients attach to when reading a source's dead letters,
// so a mismatch means a reader that works on one broker finds nothing on the
// other — silently, since an absent queue just delivers no messages.
func TestDLQSourceName(t *testing.T) {
	assert.Equal(t, "events.orders.listener.dlq",
		dlqSourceName("events.orders.listener"))

	// The '.quorum' suffix is stripped: dead letters belong to the logical
	// source, not to the queue type the other connector encodes in the name.
	assert.Equal(t, "events.orders.listener.dlq",
		dlqSourceName("events.orders.listener.quorum"))

	// Only the first occurrence, matching amqp091's strings.Replace(..., 1).
	assert.Equal(t, "a.quorum.b.dlq", dlqSourceName("a.quorum.quorum.b"))

	assert.Equal(t, ".dlq", dlqSourceName(""))
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

	// durable names inherit streamNameFor's injectivity: source names "grp.a"
	// and "grp_a" must not share a durable, or the two sources silently
	// become competing consumers on one queue instead of independent queues
	assert.NotEqual(t, durableName(q("grp.a")), durableName(q("grp_a")))
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

	// ANY where NOTHING hits -> fail. A present header with the wrong value does
	// not count, exactly as a RabbitMQ headers exchange with x-match=any requires.
	anyMiss := &pb.Filter{
		Type: pb.Filter_ANY,
		Matches: []*pb.Match{
			{Name: "event-source", Value: "nope"},
			{Name: "region", Value: "eu"},   // present but wrong value
			{Name: "absent", Value: "there"}, // not present at all
		},
	}
	assert.False(t, evaluateFilters([]*pb.Filter{anyMiss}, headers))

	// A filter with NO matches passes everything: amqp091 still appends its
	// (empty) binding table, and a headers binding with no arguments matches
	// every message. Also the only thing that exercises the len(matches)==0
	// short-circuit in filterMatches.
	assert.True(t, evaluateFilters([]*pb.Filter{{Type: pb.Filter_ALL}}, headers))
	assert.True(t, evaluateFilters([]*pb.Filter{{Type: pb.Filter_ANY}}, headers))

	// multiple filters are OR'd: second filter matches
	assert.True(t, evaluateFilters([]*pb.Filter{allMiss, all}, headers))
}

// TestDecompressBody exercises decompressBody's three outcomes directly: a
// valid gzip body round-trips; a body that is not gzip at all fails at the
// reader header; and a body with a valid gzip header but a truncated payload
// fails during the read, not at construction. The delivery-path test
// (TestStreamGzipBodyIsDecompressed) only ever drove the success case, and the
// stream mislabel test only the bad-header case.
func TestDecompressBody(t *testing.T) {
	plain := []byte("payload worth compressing, repeated payload worth compressing")
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(plain)
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	gzipped := buf.Bytes()

	got, err := decompressBody(gzipped)
	require.NoError(t, err)
	assert.Equal(t, plain, got)

	_, err = decompressBody([]byte("not gzip at all"))
	assert.Error(t, err, "a non-gzip body must error at the reader header")

	// Valid 10-byte header, payload cut short: NewReader succeeds, the inflate
	// fails partway — the io.ReadAll error branch the success case never reaches.
	_, err = decompressBody(gzipped[:len(gzipped)-6])
	assert.Error(t, err, "a truncated gzip stream must error during the read")
}

// TestFilterSubjectsForHeadersAddress pins that a headers address selects its
// whole subject space regardless of the subjects it declares: RabbitMQ's
// headers exchange routes on header arguments alone and never reads the
// routing key, so binding subjects decide nothing there. evaluateFilters is
// the connector's stand-in for the header arguments.
func TestFilterSubjectsForHeadersAddress(t *testing.T) {
	whole := []string{"events.hdr.~", "events.hdr.~.>"}

	withSubjects := &pb.Source{Type: pb.Source_QUEUE, Address: &pb.Address{
		Name: "events.hdr", Type: pb.Address_FILTER, Subjects: []string{"bound.one", "bound.two"}}}
	assert.Equal(t, whole, filterSubjectsFor(withSubjects),
		"subjects on a headers address must not narrow the filter")

	// Already the behaviour when it carries no subjects but has filters, and
	// the two must agree.
	noSubjects := &pb.Source{Type: pb.Source_QUEUE,
		Address: &pb.Address{Name: "events.hdr", Type: pb.Address_FILTER},
		Filters: []*pb.Filter{{Type: pb.Filter_ALL,
			Matches: []*pb.Match{{Name: "tenant", Value: "acme"}}}}}
	assert.Equal(t, whole, filterSubjectsFor(noSubjects))

	// A topic address with the same subjects still narrows — the widening is
	// scoped to headers addresses.
	topic := &pb.Source{Type: pb.Source_QUEUE, Address: &pb.Address{
		Name: "events.hdr", Type: pb.Address_TOPIC, Subjects: []string{"bound.one"}}}
	assert.Equal(t, []string{"events.hdr.~.bound.one"}, filterSubjectsFor(topic))
}

// TestFilterSubjectsForStreamSourceNoSubjects: a STREAM source with no subjects
// reads the whole log — amqp091's streamSubscribe goes straight to the stream
// by name and declares no binding, so its reader sees everything. Selecting the
// whole address's captured subjects is the equivalent, and is the ordinary way
// to read a stream (a QUEUE/topic source with no subjects instead gets an
// unmatchable filter, per TestFilterSubjectsFor).
func TestFilterSubjectsForStreamSourceNoSubjects(t *testing.T) {
	src := &pb.Source{Type: pb.Source_STREAM, Address: &pb.Address{Name: "events.orders"}}
	assert.Equal(t, []string{"events.orders.~", "events.orders.~.>"}, filterSubjectsFor(src))
}

// TestParentBindingSubjectsHeadersParent: the binding keys of an
// address-to-address binding are matched by the PARENT exchange's type, so a
// headers parent ignores them too — amqp091's ExchangeBind passes the child's
// subjects with no arguments, which a headers exchange matches unconditionally.
func TestParentBindingSubjectsHeadersParent(t *testing.T) {
	addr := &pb.Address{
		Name:          "events.child",
		Subjects:      []string{"bound.one"},
		ParentAddress: &pb.Address{Name: "events.parent", Type: pb.Address_FILTER},
	}
	assert.Equal(t, []string{"events.parent.~", "events.parent.~.>"}, parentBindingSubjects(addr))
}

// TestHeaderRoundTripPreservesEverything: the NATS wire format silently drops
// header names that are not HTTP tokens and trims whitespace off values (see
// the header-mapping comment in helpers.go). Every one of these round-trips
// through a real RabbitMQ unchanged, so the escaping has to make them survive
// here too. The punctuation set is the one measured against a live broker of
// each kind, where an unescaped connector delivered 15 of the 38 names.
func TestHeaderRoundTripPreservesEverything(t *testing.T) {
	in := map[string]string{
		// conventional headers — must pass through unescaped
		"Content-Type":    "application/vnd.example.event+json",
		"__namespace":     "tenant-a",
		"x-event-address": "events.orders",
		"traceparent":     "00-abc-def-01",
		"X-B3-TraceId":    "abc",
		"x-retry-count":   "3",
		// values the writer would rewrite
		"empty-value":        "",
		"leading-space":      "  v",
		"trailing-space":     "v  ",
		"surrounding-tab":    "\tv\t",
		"embedded-newline":   "a\nb",
		"embedded-crlf":      "a\r\nb",
		"only-whitespace":    "   ",
		"internal-space-ok":  "a b c",
		"unicode-value":      "héllo wörld",
		"looks-like-escaped": headerEscapePrefix + "Zm9v",
	}
	// names built from every character the live probe exercised
	for _, c := range "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~ \tàé€中" {
		in[string([]rune{'h', 'd', 'r', c, 'k', 'e', 'y'})] = "v"
	}

	got := natsToPbHeader(pbToNatsHeader(in))
	assert.Equal(t, in, got, "every header must survive the round trip byte-for-byte")
}

// TestNatsToPbHeaderSkipsEmptyValueSlice: a raw NATS header can carry a key
// with no values (nats.go's Header is map[string][]string, and a foreign
// publisher or library may leave a key present with an empty slice).
// natsToPbHeader must skip it rather than index vals[0] out of range, while
// keeping the well-formed entries alongside it. pbToNatsHeader never produces
// this shape (it always Sets a single value), so the round-trip tests never
// reach the guard.
func TestNatsToPbHeaderSkipsEmptyValueSlice(t *testing.T) {
	in := nats.Header{
		"Present": []string{"here"},
		"Empty":   []string{},
	}
	assert.Equal(t, map[string]string{"Present": "here"}, natsToPbHeader(in))
}

// TestHeaderEscapingOnlyWhereNeeded: conventional names and values reach the
// broker verbatim, so ordinary traffic stays readable to a native NATS
// consumer and only the entries NATS cannot carry are encoded.
func TestHeaderEscapingOnlyWhereNeeded(t *testing.T) {
	h := pbToNatsHeader(map[string]string{
		"Content-Type": "application/json",
		"x-event-src":  "inventory",
		"has space":    "v",
		"trailing":     "v ",
	})

	assert.Equal(t, "application/json", h.Get("Content-Type"))
	assert.Equal(t, "inventory", h.Get("x-event-src"))
	// the two damaged entries are carried under the escape prefix instead
	assert.Empty(t, h.Get("has space"))
	assert.Empty(t, h.Get("trailing"))
	escaped := 0
	for k := range h {
		if strings.HasPrefix(k, headerEscapePrefix) {
			escaped++
		}
	}
	assert.Equal(t, 2, escaped, "exactly the unrepresentable entries are escaped")
}

// TestHeaderEscapingSurvivesTheRealWriter drives the escaped form through the
// actual lossy component — net/http's Header.Write, which is what nats.go uses
// in Msg.headerBytes — and reads it back the way nats.go parses it. Asserting
// against the real writer rather than against tchar (our restatement of its
// rule) is the point: if the stdlib rule and ours ever diverge, this fails
// while a test written against tchar alone would keep passing.
func TestHeaderEscapingSurvivesTheRealWriter(t *testing.T) {
	in := map[string]string{
		"hdr key\twith\r\neverything: €": "  spaced\r\nvalue  ",
		"Content-Type":                   "application/json",
		"plain":                          "v",
		"trailing":                       "v  ",
	}

	var buf bytes.Buffer
	require.NoError(t, http.Header(pbToNatsHeader(in)).Write(&buf))
	buf.WriteString("\r\n")

	// Parse back preserving original case, as nats.go's readMIMEHeader does.
	onWire := nats.Header{}
	for _, line := range strings.Split(buf.String(), "\r\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		onWire[name] = append(onWire[name], strings.TrimLeft(value, " \t"))
	}

	assert.Equal(t, in, natsToPbHeader(onWire),
		"headers must survive the real http.Header.Write round trip")
}

// TestHeaderDecodeLeavesForeignEntriesAlone: a publisher that is not this
// connector can put anything on a subject, including a name that merely looks
// escaped. Such an entry is passed through rather than dropped or mangled.
func TestHeaderDecodeLeavesForeignEntriesAlone(t *testing.T) {
	got := natsToPbHeader(nats.Header{
		headerEscapePrefix + "not!valid!base64": []string{"dg"},
		headerEscapePrefix + "Zm9v":             []string{"not!base64"},
		"ordinary":                              []string{"v"},
	})
	assert.Equal(t, map[string]string{
		headerEscapePrefix + "not!valid!base64": "dg",
		headerEscapePrefix + "Zm9v":             "not!base64",
		"ordinary":                              "v",
	}, got)
}

// TestHeaderNameSurvives pins the RFC 7230 token rule the escaping keys on.
func TestHeaderNameSurvives(t *testing.T) {
	for _, ok := range []string{"Content-Type", "__namespace", "x-retry-count",
		"a.b.c", "A1", "!#$%&'*+-.^_`|~"} {
		assert.True(t, headerNameSurvives(ok), "%q should pass through", ok)
	}
	for _, bad := range []string{"", "has space", "has:colon", "has/slash",
		"has@at", "has,comma", "has=eq", "has[br]", "unicodeé",
		headerEscapePrefix + "x"} {
		assert.False(t, headerNameSurvives(bad), "%q must be escaped", bad)
	}
}
