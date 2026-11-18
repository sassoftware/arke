// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"fmt"
	"strconv"
	"strings"

	streamclient "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func parseOffset(offset string, lastOffset int64) (streamclient.OffsetSpecification, error) {
	switch strings.ToLower(offset) {
	case "first":
		return streamclient.OffsetSpecification{}.First(), nil
	case "continue":
		return streamclient.OffsetSpecification{}.Offset(lastOffset + 1), nil
	case "last":
		return streamclient.OffsetSpecification{}.Last(), nil
	case "next", "":
		return streamclient.OffsetSpecification{}.Next(), nil
	}
	parsedOffset, err := strconv.Atoi(offset)
	if err == nil {
		return streamclient.OffsetSpecification{}.Offset(int64(parsedOffset)), nil
	}
	return streamclient.OffsetSpecification{}, fmt.Errorf("invalid offset: %s", offset)
}
