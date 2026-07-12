// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package natsjs

import (
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
// other address, and "~" is stripped from address tokens by sanitization, so
// no address token can ever equal the delimiter. It also keeps the two
// addresses' messages apart (without it, publishing "filtered.x" to
// "events.orders" would be indistinguishable from publishing "x" to
// "events.orders.filtered"), and gives the empty routing key a concrete
// subject ("<root>.~").
//
// This is a faithful mapping for the dotted, topic-style keys AMQP topic
// exchanges use (e.g. orders.region.us.created.*). It does NOT reproduce AMQP
// headers-exchange routing — that is handled proxy-side in evaluateFilters
// (see Subscribe).

// addressDelim is the reserved token inserted between the address root and the
// routing-key tokens. Sanitization guarantees no address token equals it.
const addressDelim = "~"

// addressRoot sanitizes an address name into a valid dotted subject prefix.
// Empty tokens and tokens equal to the reserved delimiter are replaced rather
// than dropped so distinct address names keep distinct roots where possible.
func addressRoot(addressName string) string {
	if addressName == "" {
		return ""
	}
	tokens := strings.Split(addressName, ".")
	for i, t := range tokens {
		t = tokenSanitizer.Replace(t)
		if t == "" || t == addressDelim {
			t = "_"
		}
		tokens[i] = t
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

// streamNameFor derives a JetStream-legal stream name from an address name.
// Stream names may not contain '.', ' ', '*' or '>'. Derived from the
// sanitized root so every address that shares a root shares a stream.
func streamNameFor(addressName string) string {
	return "arke_" + strings.ReplaceAll(addressRoot(addressName), ".", "_")
}

// publishSubjectFor maps an address + routing key onto the concrete subject a
// message is published to. Routing keys are literal on the publish side —
// AMQP only gives '*' / '#' wildcard meaning in bindings — and NATS forbids
// wildcard tokens in published subjects, so they are sanitized like any other
// illegal character.
func publishSubjectFor(addressName, routingKey string) string {
	prefix := subjectPrefix(addressName)
	tokens := strings.Split(routingKey, ".")
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if t == "" {
			continue
		}
		out = append(out, publishSanitizer.Replace(t))
	}
	if len(out) == 0 {
		return prefix
	}
	return prefix + "." + strings.Join(out, ".")
}

// publishSanitizer is tokenSanitizer plus '#': on the publish side wildcard
// characters carry no meaning and must not appear in the subject.
var publishSanitizer = strings.NewReplacer(" ", "_", "\t", "_", "*", "_", ">", "_", "#", "_")

// tokenSanitizer replaces characters that are illegal inside a NATS subject
// token (space/tab, and the wildcard chars '*'/'>') so a literal AMQP token that
// happens to embed one of them still yields a valid subject.
var tokenSanitizer = strings.NewReplacer(" ", "_", "\t", "_", "*", "_", ">", "_")

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
//	                           would otherwise be empty — and thus invalid — tokens)
//	other             -> illegal chars (space/tab/'*'/'>') replaced with '_'
//
// The result is always a valid NATS subject fragment (possibly empty if every
// token was empty; filterSubjectsFor handles that).
func translateWildcards(key string) string {
	tokens := strings.Split(key, ".")
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
		case "":
			// drop empty token
		default:
			out = append(out, tokenSanitizer.Replace(t))
		}
	}
	return strings.Join(out, ".")
}

// routingKeyFromSubject recovers the routing key a delivered message was
// published with by stripping the address's subject prefix — the inverse of
// publishSubjectFor for subjects the connector itself produced. The bare
// prefix (an empty routing key) and any subject not under the prefix (which
// a consumer's filter subjects make impossible) yield "".
func routingKeyFromSubject(addressName, subject string) string {
	if rk, ok := strings.CutPrefix(subject, subjectPrefix(addressName)+"."); ok {
		return rk
	}
	return ""
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
// filter subjects. An empty pattern set means everything under the address.
// A pattern whose AMQP '#' tail became '>' also gets the zero-word variant:
// AMQP '#' matches zero or more words while NATS '>' matches one or more, so
// binding "a.#" must match routing key "a" too.
func filterSubjectsFor(source *pb.Source) []string {
	prefix := subjectPrefix(source.GetAddress().GetName())
	pats := source.GetAddress().GetSubjects()
	if len(pats) == 0 {
		pats = []string{""}
	}
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
			add(prefix)
			add(prefix + ".>")
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
	// An AMQP binding set may be redundant — "orders.#" alongside
	// "orders.created" — which is harmless on a broker that routes a message
	// to a queue once no matter how many of its bindings match. JetStream
	// instead rejects a consumer whose filter subjects overlap (one being a
	// subset of another), so drop every filter another filter already covers.
	// The surviving wider filter matches everything the dropped one did.
	kept := make([]string, 0, len(out))
	for _, s := range out {
		subsumed := false
		for _, t := range out {
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
	case pb.Source_TEMPORARY:
		// Transient; already returned above via the TEMPORARY guard.
	}
	return ""
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
