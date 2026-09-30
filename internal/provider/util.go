// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package provider

import "github.com/sassoftware/arke/internal/util"

func SleepRandomReconnect() {
	util.SleepRandom(100, ReconnectDelay)
}
