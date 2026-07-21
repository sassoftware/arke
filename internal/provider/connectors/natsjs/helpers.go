// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package natsjs

import (
	"slices"
	"strings"

	"github.com/nats-io/nats.go"
	pb "github.com/sassoftware/arke/api"
)

// Subject / topology mapping.
//
// AMQP (RabbitMQ) routes via exchange + routing-key bindings. NATS routes via
// subjects with token wildcards. We map:
//
//	exchange/address name  -> subject root (dots kept; it is a multi-token prefix)
//	"~"                    -> reserved delimiter token, always inserted after the root
//	routing key            -> appended subject tokens
//	AMQP '#' (zero+ words) -> NATS '>'   (NATS only allows '>' as the final token)
//	AMQP '*' (one word)    -> NATS '*'
//
// A message published to address "events.orders" with routing key
// "region.us.created" travels on subject "events.orders.~.region.us.created".
// A JetStream stream captures "<root>.~" and "<root>.~.>" and consumers filter
// on the mapped source subjects.
//
// The "~" delimiter exists because each address gets its own stream, and
// JetStream requires every stream's subject space to be disjoint. Address
// names themselves are dotted, so two addresses can sit in a prefix
// relationship — "events.orders" and "events.orders.filtered" — and without a
// delimiter their capture wildcards ("events.orders.>" vs
// "events.orders.filtered.>") overlap: whichever stream is created first wins
// and the other fails forever with "subjects overlap with an existing stream"
// (err 10065). The delimiter makes any two distinct roots disjoint: the token
// after "events.orders" is "~" for its own traffic but "filtered" for the
// other address, and token escaping (escapeToken) guarantees no address token
// can ever equal the delimiter. It also keeps the two addresses' messages
// apart (without it, publishing "filtered.x" to "events.orders" would be
// indistinguishable from publishing "x" to "events.orders.filtered"), and
// gives the empty routing key a concrete subject ("<root>.~").
//
// This is a faithful mapping for the dotted, topic-style keys AMQP topic
// exchanges use (e.g. orders.region.us.created.*). It does NOT reproduce AMQP
// headers-exchange routing — that is handled proxy-side in evaluateFilters
// (see Subscribe).

// addressDelim is the reserved token inserted between the address root and the
// routing-key tokens. Token escaping guarantees no address token equals it.
const addressDelim = "~"

// tokenEscapes encodes, inside an escaped subject token (see escapeToken),
// the characters a token cannot carry literally: the whitespace and wildcard
// characters NATS forbids in a subject token, '#' (so a literal AMQP '#'
// stays distinct from every other token on both the publish and binding
// sides), and '~', which the connector reserves as its address delimiter and
// as the escape marker itself. Every code is two characters starting with
// '~' and no literal character inside an escaped token is ever '~', so an
// escaped token decodes unambiguously left to right — which is what makes
// the encoding injective.
var tokenEscapes = map[rune]string{
	'~':  "~~",
	' ':  "~w",
	'\t': "~t",
	'\r': "~r",
	'\n': "~n",
	'\f': "~f",
	'*':  "~a",
	'>':  "~g",
	'#':  "~h",
}

// tokenUnescapes inverts tokenEscapes (second code byte -> original byte).
var tokenUnescapes = map[byte]byte{
	'~': '~', 'w': ' ', 't': '\t', 'r': '\r', 'n': '\n', 'f': '\f',
	'a': '*', 'g': '>', 'h': '#',
}

// tokenEscapeTriggers are the characters that force a token into the escaped
// form; a token free of them passes through literally.
const tokenEscapeTriggers = "~ \t\r\n\f*>#" //nolint:gosec // G101 false positive: NATS subject reserved characters, not a credential

// escapeToken maps one address or routing-key token onto a NATS-legal
// subject token, injectively: distinct tokens never yield the same output. A
// lossy replacement here (the obvious "swap illegal characters for '_'")
// would merge distinct AMQP names — "a b" and "a_b", or an address token
// "~" and "_" — onto one subject, and for addresses that is the worst
// failure mode available: two addresses sharing a subject space share a
// stream, leak each other's messages, and reconfigure each other's topology.
//
// Tokens free of reserved characters pass through unchanged, so every
// conventional dotted name keeps its readable, historical form. Any other
// token takes an escaped form marked by a leading '~' — which no plain token
// can start with, since '~' itself is reserved — followed by the token with
// each reserved character replaced by its two-character code (tokenEscapes).
// The empty token (AMQP allows consecutive dots) becomes the fixed marker
// "~e", which cannot collide with an escaped non-empty token: those always
// contain a second '~', because only tokens containing a reserved character
// are escaped and every reserved character encodes to a '~' pair.
func escapeToken(t string) string {
	if t == "" {
		return "~e"
	}
	if !strings.ContainsAny(t, tokenEscapeTriggers) {
		return t
	}
	var b strings.Builder
	b.WriteByte('~')
	for _, r := range t {
		if esc, ok := tokenEscapes[r]; ok {
			b.WriteString(esc)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// unescapeToken inverts escapeToken. Tokens the connector did not escape
// come back unchanged; an escape code the connector never produces is kept
// literally (best effort — such tokens do not occur in subjects the
// connector itself built). Escape codes are pure ASCII and '~' (0x7E) never
// occurs inside a UTF-8 multi-byte sequence, so byte-wise scanning is safe.
func unescapeToken(t string) string {
	if !strings.HasPrefix(t, "~") {
		return t
	}
	if t == "~e" {
		return ""
	}
	body := t[1:]
	var b strings.Builder
	for i := 0; i < len(body); i++ {
		c := body[i]
		if c != '~' || i+1 == len(body) {
			b.WriteByte(c)
			continue
		}
		i++
		if orig, ok := tokenUnescapes[body[i]]; ok {
			b.WriteByte(orig)
		} else {
			b.WriteByte('~')
			b.WriteByte(body[i])
		}
	}
	return b.String()
}

// addressRoot maps an address name onto a valid dotted subject prefix.
// Tokens are escaped rather than lossily replaced or dropped, so distinct
// address names always keep distinct roots — two addresses may never share a
// subject space (see escapeToken).
func addressRoot(addressName string) string {
	if addressName == "" {
		return ""
	}
	tokens := strings.Split(addressName, ".")
	for i, t := range tokens {
		tokens[i] = escapeToken(t)
	}
	return strings.Join(tokens, ".")
}

// subjectPrefix returns the subject an empty routing key maps to, which is also
// the prefix of every subject under the address.
func subjectPrefix(addressName string) string {
	if root := addressRoot(addressName); root != "" {
		return root + "." + addressDelim
	}
	return addressDelim
}

// nameEscapes encodes, inside the escaped name form (see streamNameFor), the
// characters JetStream rejects in stream/consumer names plus '_' itself.
// Every code is two characters starting with '_' and no literal character
// maps to '_', so an escaped name decodes unambiguously left to right —
// which is what makes the encoding injective. (This is the name-level
// counterpart of tokenEscapes: subjects reserve '~', names reserve '_'.)
var nameEscapes = map[rune]string{
	'.':  "_d",
	'_':  "_u",
	'/':  "_s",
	'\\': "_b",
	'\r': "_r",
	'\n': "_n",
	'\f': "_f",
}

// nameEscapeTriggers are the characters that force a root into the escaped
// name form: '_' because the plain form could not tell it apart from an
// encoded '.', and the rest because JetStream rejects them in names outright
// (they are legal in subjects, so sanitization keeps them in the root).
const nameEscapeTriggers = "_/\\\r\n\f"

// streamNameFor derives a JetStream-legal name from an address (or consumer
// group / source) name; durable consumer names use it too, as JetStream
// validates both identically. Names may not contain whitespace, '.', '*',
// '>', '/' or '\'. Derived from the sanitized root so every address that
// shares a root shares a stream.
//
// The common case — roots free of '_' and of characters JetStream rejects
// in names — keeps the readable historical form, dots swapped for
// underscores ("events.orders" -> "arke_events_orders"). That replacement is
// only unambiguous while no token contains '_' of its own: "a.b" and "a_b"
// would otherwise both read "arke_a_b", and two addresses colliding onto one
// stream name silently reconfigure each other's stream (each re-ensure
// flips the stream's subjects to its own address). Such roots instead take
// an escaped form under the disjoint "arke-" prefix, where every '_' starts
// a two-character escape code (nameEscapes), so distinct roots always yield
// distinct names across both forms. Roots are already injective per address
// (escapeToken), so the composed address -> stream-name mapping is too.
func streamNameFor(addressName string) string {
	root := addressRoot(addressName)
	if !strings.ContainsAny(root, nameEscapeTriggers) {
		return "arke_" + strings.ReplaceAll(root, ".", "_")
	}
	var b strings.Builder
	b.WriteString("arke-")
	for _, r := range root {
		if esc, ok := nameEscapes[r]; ok {
			b.WriteString(esc)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// publishSubjectFor maps an address + routing key onto the concrete subject a
// message is published to. Routing keys are literal on the publish side —
// AMQP only gives '*' / '#' wildcard meaning in bindings — and NATS forbids
// wildcard tokens in published subjects, so every token is escaped
// (injectively: distinct routing keys never merge onto one subject, see
// escapeToken). Empty tokens (consecutive or trailing dots) are dropped;
// binding patterns drop them identically, so both sides agree.
func publishSubjectFor(addressName, routingKey string) string {
	prefix := subjectPrefix(addressName)
	tokens := strings.Split(routingKey, ".")
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if t == "" {
			continue
		}
		out = append(out, escapeToken(t))
	}
	if len(out) == 0 {
		return prefix
	}
	return prefix + "." + strings.Join(out, ".")
}

// translateWildcards converts an AMQP topic routing key into a valid NATS subject
// fragment. NATS is stricter than AMQP about subjects, so besides translating the
// wildcards it also sanitizes each token. Mapping (per dot-delimited token):
//
//	"*"               -> "*"   single-token wildcard (same meaning)
//	"#" (last token)  -> ">"   tail wildcard (same meaning)
//	"#" (mid token)   -> "*"   NATS '>' is tail-only, so a non-terminal AMQP '#'
//	                           has no exact equivalent; '*' keeps the subject valid
//	                           but narrows zero-or-more to exactly-one.
//	""                -> dropped (collapses "a..b" and leading/trailing dots, which
//	                           would otherwise be empty — and thus invalid — tokens;
//	                           publishSubjectFor drops them identically)
//	other             -> escaped like a published token (escapeToken), so a
//	                           literal binding token matches exactly the
//	                           published keys it matches on RabbitMQ
//
// Runs of consecutive '#' are collapsed into one before translation — they
// are equivalent in AMQP ('#' matches zero or more words) — so "#.#" gets
// the exact tail mapping instead of a first-'#' narrowed to '*'.
//
// The result is always a valid NATS subject fragment (possibly empty if every
// token was empty; filterSubjectsFor handles that).
func translateWildcards(key string) string {
	raw := strings.Split(key, ".")
	// Drop empty tokens, and collapse runs of consecutive '#' into one: AMQP's
	// '#' matches zero or more words, so adjacent '#'s are equivalent to a
	// single one ("a.#.#" ≡ "a.#"). Collapsing has to happen before the
	// position-based translation below, or a trailing run like "#.#" would put
	// its first '#' in a non-terminal position and take the lossy '*' mapping —
	// losing the zero-word match a lone trailing '#' keeps.
	tokens := make([]string, 0, len(raw))
	for _, t := range raw {
		if t == "" || (t == "#" && len(tokens) > 0 && tokens[len(tokens)-1] == "#") {
			continue
		}
		tokens = append(tokens, t)
	}
	out := make([]string, 0, len(tokens))
	for i, t := range tokens {
		switch t {
		case "*":
			out = append(out, "*")
		case "#":
			if i == len(tokens)-1 {
				out = append(out, ">")
			} else {
				out = append(out, "*")
			}
		default:
			out = append(out, escapeToken(t))
		}
	}
	return strings.Join(out, ".")
}

// routingKeyFromSubject recovers the routing key a delivered message was
// published with by dropping everything up to and including the address
// delimiter and decoding each escaped token — the inverse of publishSubjectFor
// for subjects the connector itself produced (the decode is what keeps a
// dead-letter republish of the recovered key on the same tokens instead of
// escaping them a second time). A subject that is just a prefix (an empty
// routing key) yields "", as does one carrying no delimiter at all.
//
// The delimiter is located rather than a specific address's prefix stripped,
// because a message can legitimately arrive under a subject rooted at a
// DIFFERENT address than the one the source names: an address-to-address
// binding sources the parent's messages into the child's stream keeping the
// subject they were published under (see ensureStreamFor), and the child's
// consumer selects exactly those subjects (filterSubjectsFor). Stripping the
// child's prefix would fail to match and report an empty routing key for every
// message routed in from a parent — dead-lettering it under the wrong key,
// where RabbitMQ's broker-side DLX move preserves the original.
//
// Scanning for the delimiter is unambiguous: escapeToken never emits a bare
// "~" token (an escaped token is "~e", or '~' followed by one or more
// two-character codes, so at least three characters), and subjectPrefix
// inserts exactly one. So the first token equal to the delimiter is always the
// one the connector put there.
func routingKeyFromSubject(subject string) string {
	tokens := strings.Split(subject, ".")
	delim := slices.Index(tokens, addressDelim)
	if delim < 0 || delim == len(tokens)-1 {
		return ""
	}
	rk := tokens[delim+1:]
	out := make([]string, 0, len(rk))
	for _, t := range rk {
		out = append(out, unescapeToken(t))
	}
	return strings.Join(out, ".")
}

// streamSubjectsFor returns the subject set a stream captures for the given
// address: the bare prefix (empty routing key) and everything under it. The
// two are disjoint from each other ('>' needs at least one more token) and,
// thanks to the delimiter, from every other address's set.
func streamSubjectsFor(addressName string) []string {
	prefix := subjectPrefix(addressName)
	return []string{prefix, prefix + ".>"}
}

// filterSubjectsFor maps a source's routing-key patterns to NATS consumer
// filter subjects.
//
// A source whose address has a parent also selects the subjects its address is
// bound to in the *parent's* space: messages routed in from the parent are
// sourced into this address's stream keeping the subject they were published
// under (see parentBindingSubjects and ensureStreamFor).
func filterSubjectsFor(source *pb.Source) []string {
	own := ownFilterSubjectsFor(source)
	parents := parentBindingSubjects(source.GetAddress())
	if len(parents) == 0 {
		return own
	}
	return pruneSubsumed(append(own, parents...))
}

// parentBindingSubjects gives the subjects an address's bindings select in its
// parent address's subject space, or nil if it has no parent.
//
// An address-to-address binding uses the child's own subjects as the binding
// keys (amqp091's declareExchange binds each of them from the child exchange
// to the parent), and those keys are matched by the *parent* exchange's type —
// so the mapping is exactly the ordinary binding translation, run against the
// parent's name and type. With no subjects there are no bindings and so
// nothing is routed in, which is why this returns nil rather than asking
// ownFilterSubjectsFor for an unmatchable subject.
func parentBindingSubjects(addr *pb.Address) []string {
	parent := addr.GetParentAddress()
	if parent.GetName() == "" || len(addr.GetSubjects()) == 0 {
		return nil
	}
	return ownFilterSubjectsFor(&pb.Source{
		Address: &pb.Address{
			Name:     parent.GetName(),
			Type:     parent.GetType(),
			Subjects: addr.GetSubjects(),
		},
	})
}

// ownFilterSubjectsFor maps a source's routing-key patterns to the filter
// subjects of its own address. A pattern whose AMQP '#' tail became '>' also
// gets the zero-word variant: AMQP '#' matches zero or more words while NATS
// '>' matches one or more, so binding "a.#" must match routing key "a" too.
//
// A source with no patterns declares no bindings and so receives nothing —
// see boundNothingSubjects for the exceptions and for how "nothing" is
// expressed to a consumer that must carry at least one filter.
func ownFilterSubjectsFor(source *pb.Source) []string {
	if len(source.GetAddress().GetSubjects()) == 0 {
		return boundNothingSubjects(source)
	}
	// A headers address routes on headers alone: RabbitMQ's headers exchange
	// never looks at the routing key, so the subjects a source declares on one
	// are binding keys the broker discards (amqp091 passes them to QueueBind /
	// ExchangeBind all the same, and they decide nothing). Selecting the whole
	// address and letting evaluateFilters decide is the equivalent — filtering
	// by subject here would drop messages RabbitMQ delivers, which is the same
	// reasoning boundNothingSubjects already applies to a headers address that
	// carries no subjects at all.
	if source.GetAddress().GetType() == pb.Address_FILTER {
		return wholeAddressSubjects(source.GetAddress().GetName())
	}
	if source.GetAddress().GetType() == pb.Address_QUEUE {
		return directFilterSubjectsFor(source)
	}

	prefix := subjectPrefix(source.GetAddress().GetName())
	pats := source.GetAddress().GetSubjects()
	out := make([]string, 0, len(pats)+1)
	seen := make(map[string]bool, len(pats)+1)
	add := func(s string) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, p := range pats {
		pat := translateWildcards(p)
		switch {
		case pat == "":
			// An empty binding key is a literal, not a wildcard: on a topic
			// exchange it matches the empty routing key and nothing else.
			add(prefix)
		case strings.HasSuffix(pat, ">"):
			base := strings.TrimSuffix(strings.TrimSuffix(pat, ">"), ".")
			if base == "" {
				add(prefix)
			} else {
				add(prefix + "." + base)
			}
			add(prefix + "." + pat)
		default:
			add(prefix + "." + pat)
		}
	}
	return pruneSubsumed(out)
}

// pruneSubsumed drops every subject another subject in the set already covers.
// An AMQP binding set may be redundant — "orders.#" alongside "orders.created"
// — which is harmless on a broker that routes a message to a queue once no
// matter how many of its bindings match. JetStream instead rejects a consumer
// whose filter subjects overlap (one being a subset of another). The surviving
// wider filter matches everything the dropped one did. Duplicates are dropped
// too, since a subject trivially covers itself.
func pruneSubsumed(subjects []string) []string {
	uniq := make([]string, 0, len(subjects))
	seen := make(map[string]bool, len(subjects))
	for _, s := range subjects {
		if !seen[s] {
			seen[s] = true
			uniq = append(uniq, s)
		}
	}
	kept := make([]string, 0, len(uniq))
	for _, s := range uniq {
		subsumed := false
		for _, t := range uniq {
			if t != s && subjectSubsumes(t, s) {
				subsumed = true
				break
			}
		}
		if !subsumed {
			kept = append(kept, s)
		}
	}
	return kept
}

// unmatchableToken is a subject token no published subject can contain, so a
// filter subject ending in it selects nothing while still being a legal subset
// of the address's stream — which is how a consumer, obliged to carry at least
// one filter, expresses "bound to nothing".
//
// It is unreachable by construction: escapeToken emits either a token free of
// reserved characters (never starting with '~', since '~' is itself reserved),
// or "~e" for the empty token, or '~' followed by one or more *two*-character
// escape codes — three characters at minimum. No output is ever a '~' plus a
// single character other than "~e".
const unmatchableToken = "~x"

// wholeAddressSubjects selects everything an address's stream captures — the
// bare prefix (empty routing key) and every subject under it. It is the filter
// set for a source bound to the address as a whole rather than to particular
// routing keys.
func wholeAddressSubjects(addressName string) []string {
	prefix := subjectPrefix(addressName)
	return []string{prefix, prefix + ".>"}
}

// boundNothingSubjects gives the filter subjects for a source whose address
// carries no routing-key patterns. amqp091 declares no binding at all in that
// case (declareBinding iterates an empty subject list), so the source receives
// nothing — with two exceptions that both mean "the whole address".
func boundNothingSubjects(source *pb.Source) []string {
	prefix := subjectPrefix(source.GetAddress().GetName())
	whole := wholeAddressSubjects(source.GetAddress().GetName())
	// A stream source is not bound to its address at all. amqp091 reads a
	// RabbitMQ stream by name — streamSubscribe goes straight to the stream
	// connection and never declares an exchange or a binding — so its reader
	// sees the whole log. Selecting everything the address's stream captures
	// is the equivalent. This is not a corner case: a Source_STREAM on an
	// Address_STREAM with no subjects is the ordinary way to read a stream,
	// so reading it as "no bindings" would silently deliver nothing.
	if source.GetType() == pb.Source_STREAM {
		return whole
	}
	// A headers address with filters: amqp091 binds a single "" key purely to
	// have a binding to hang the header arguments on (declareBinding fakes the
	// subject in when the address carries none). A headers exchange ignores
	// routing keys, so that binding matches every message and the header match
	// decides. evaluateFilters is this connector's stand-in for those
	// arguments, so select the whole address and let it do the deciding.
	if source.GetAddress().GetType() == pb.Address_FILTER && len(source.GetFilters()) > 0 {
		return whole
	}
	return []string{prefix + "." + unmatchableToken}
}

func directFilterSubjectsFor(source *pb.Source) []string {
	pats := source.GetAddress().GetSubjects()
	out := make([]string, 0, len(pats))
	seen := make(map[string]bool, len(pats))
	for _, p := range pats {
		s := publishSubjectFor(source.GetAddress().GetName(), p)
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// subjectSubsumes reports whether every subject matched by narrow is also
// matched by wide (both may contain NATS wildcards): a wide '>' covers any
// one or more remaining tokens, and a wide '*' covers a literal token or
// another '*' but never '>'. This is the subset relation JetStream applies
// when it rejects a consumer's filter subjects as overlapping.
func subjectSubsumes(wide, narrow string) bool {
	w := strings.Split(wide, ".")
	n := strings.Split(narrow, ".")
	for i, wt := range w {
		if i >= len(n) {
			return false
		}
		if wt == ">" {
			return true
		}
		nt := n[i]
		if nt == ">" || (nt == "*" && wt != "*") {
			return false
		}
		if wt != "*" && wt != nt {
			return false
		}
	}
	return len(w) == len(n)
}

// durableName returns the JetStream durable consumer name for a source, or ""
// if the source should use an ephemeral consumer.
//
// Durable (work-queue) consumers persist across client reconnects, so a backlog
// published while the consumer is disconnected is redelivered on reconnect —
// matching RabbitMQ durable-queue semantics. Transient sources stay ephemeral
// and auto-expire (InactiveThreshold).
//
// Clients commonly mark per-instance/transient consumers by converting an
// Exclusive queue into AutoDelete + a UUID-suffixed name, so
// auto-delete/exclusive/TEMPORARY sources are treated as the transient ones. A
// non-transient QUEUE (a durable named listener) gets a durable consumer, as does
// a STREAM source with a ConsumerGroup, or any SingleActiveConsumer.
func durableName(source *pb.Source) string {
	if source.GetAutoDelete() || source.GetExclusive() || source.GetType() == pb.Source_TEMPORARY {
		return ""
	}
	if source.GetSingleActiveConsumer() {
		// Single-active instances coordinate by attaching to one shared
		// consumer, so the durable must be named for the coordination
		// identity, not the individual subscriber. That identity is the
		// ConsumerGroup option when set — amqp091 uses it as the consumer
		// reference for single-active stream sources for the same reason —
		// which also lets sources that share one name but separate their
		// instances only by group (one group per partition/tenant/shard of a
		// shared stream) get one durable per group instead of collapsing
		// onto a single pinned consumer that starves every other group.
		if cg := source.GetOptions()["ConsumerGroup"]; cg != "" {
			return streamNameFor(cg)
		}
		// A stream source with no group has no coordination identity —
		// Subscribe rejects it, like amqp091. A queue source falls back to
		// the queue's own name: all instances of a single-active queue share
		// it by definition (RabbitMQ's x-single-active-consumer is a queue
		// property).
		if source.GetType() == pb.Source_STREAM {
			return ""
		}
		return streamNameFor(source.GetName())
	}
	switch source.GetType() {
	case pb.Source_QUEUE:
		return streamNameFor(source.GetName())
	case pb.Source_STREAM:
		if cg := source.GetOptions()["ConsumerGroup"]; cg != "" {
			return streamNameFor(cg)
		}
		// A group-less stream source is ordinarily an independent ephemeral
		// reader, but "continue" asks to resume where this source left off,
		// which only a position the broker kept between subscriptions can
		// answer. amqp091 reads one from RabbitMQ Streams' server-side offset
		// tracking, keyed by the consumer name; JetStream keeps a position per
		// durable consumer, so name a durable after the source. Every other
		// offset positions the reader from the log itself and stays ephemeral,
		// so same-named readers still read independently.
		if wantsStoredPosition(source) {
			return streamNameFor(source.GetName())
		}
	case pb.Source_TEMPORARY:
		// Transient; already returned above via the TEMPORARY guard.
	}
	return ""
}

// wantsStoredPosition reports whether a source's Offset asks to resume from a
// position the broker remembers, rather than one derived from the log itself.
// Only "continue" does.
func wantsStoredPosition(source *pb.Source) bool {
	return strings.EqualFold(strings.TrimSpace(source.GetOptions()["Offset"]), "continue")
}

// pbToNatsHeader converts Arke's flat string headers to a NATS header.
func pbToNatsHeader(in map[string]string) nats.Header {
	if len(in) == 0 {
		return nil
	}
	h := nats.Header{}
	for k, v := range in {
		h.Set(k, v)
	}
	return h
}

// copyHeader shallow-copies a NATS header so options applied to the copy at
// publish time (e.g. a message id) do not mutate the source message's header
// map.
func copyHeader(in nats.Header) nats.Header {
	if len(in) == 0 {
		return nil
	}
	out := make(nats.Header, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// natsToPbHeader converts a NATS header to Arke's flat string headers, keeping
// the first value of each key.
func natsToPbHeader(in nats.Header) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, vals := range in {
		if len(vals) > 0 {
			out[k] = vals[0]
		}
	}
	return out
}

// evaluateFilters reproduces RabbitMQ headers-exchange matching proxy-side.
//
// Multiple Filters are OR'd together (each corresponds to a separate AMQP
// binding). Within a single Filter, matches are combined per Filter.Type
// (ALL = and, ANY = or). No filters means the message always passes.
func evaluateFilters(filters []*pb.Filter, headers map[string]string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, f := range filters {
		if filterMatches(f, headers) {
			return true
		}
	}
	return false
}

func filterMatches(f *pb.Filter, headers map[string]string) bool {
	matches := f.GetMatches()
	if len(matches) == 0 {
		return true
	}
	any := f.GetType() == pb.Filter_ANY
	for _, m := range matches {
		got, ok := headers[m.GetName()]
		hit := ok && got == m.GetValue()
		if any && hit {
			return true
		}
		if !any && !hit {
			return false
		}
	}
	// ALL: every match passed; ANY: none passed.
	return !any
}
