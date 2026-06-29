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
//	exchange/address name  -> subject root (dots kept; it is just a multi-token prefix)
//	routing key            -> appended subject tokens
//	AMQP '#' (zero+ words) -> NATS '>'   (NATS only allows '>' as the final token)
//	AMQP '*' (one word)    -> NATS '*'
//
// A JetStream stream captures "<root>.>" and consumers filter on the mapped
// source subjects. This is a faithful mapping for the dotted, topic-style keys
// AMQP topic exchanges use (e.g. orders.region.us.created.*). It does NOT
// reproduce AMQP headers-exchange routing — that is handled proxy-side in
// evaluateFilters (see Subscribe).

// streamNameFor derives a JetStream-legal stream name from an address name.
// Stream names may not contain '.', ' ', '*' or '>'.
func streamNameFor(addressName string) string {
	r := strings.NewReplacer(".", "_", " ", "_", "*", "_", ">", "_")
	return "arke_" + r.Replace(addressName)
}

// subjectFor joins an address root with a routing key / pattern, translating
// AMQP wildcards to NATS wildcards.
func subjectFor(addressName, routingKey string) string {
	root := addressName
	if routingKey == "" {
		if root == "" {
			return ">"
		}
		return root + ".>"
	}
	rk := translateWildcards(routingKey)
	if rk == "" {
		// The routing key was made up entirely of empty tokens (e.g. ".",
		// trailing/leading dots): treat it as capture-all under the root.
		if root == "" {
			return ">"
		}
		return root + ".>"
	}
	if root == "" {
		return rk
	}
	return root + "." + rk
}

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
// token was empty; subjectFor handles that).
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

// streamSubjectsFor returns the wildcard subject set a stream should capture for
// the given address.
func streamSubjectsFor(addressName string) []string {
	if addressName == "" {
		return []string{">"}
	}
	return []string{addressName + ".>"}
}

// filterSubjectsFor maps a source's subjects to NATS consumer filter subjects.
func filterSubjectsFor(source *pb.Source) []string {
	addr := source.GetAddress().GetName()
	subs := source.GetAddress().GetSubjects()
	if len(subs) == 0 {
		return []string{subjectFor(addr, "")}
	}
	out := make([]string, 0, len(subs))
	for _, s := range subs {
		out = append(out, subjectFor(addr, s))
	}
	return out
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
