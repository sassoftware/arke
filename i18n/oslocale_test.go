// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package i18n

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type detectLocaleTestCase struct {
	name     string
	Lang     string
	LcAll    string
	args     []string
	expected string
}

func TestArgsInit(t *testing.T) {
	cases := []detectLocaleTestCase{
		{
			name:     "default",
			Lang:     "",
			LcAll:    "",
			args:     []string{"a"},
			expected: "en",
		},
		{
			name:     "cli args override",
			Lang:     localeEnUS,
			LcAll:    "",
			args:     []string{"a", "--locale", "zh"},
			expected: "zh",
		},
		{
			name:     EnvLang,
			Lang:     localeEnUS,
			LcAll:    "",
			args:     []string{"a"},
			expected: localeEnUS,
		},
		{
			name:     EnvLcAll,
			Lang:     "",
			LcAll:    localeDeDE,
			args:     []string{"a"},
			expected: localeDeDE,
		},
		{
			name:     EnvLcAll + " priority",
			Lang:     "zh-Hans",
			LcAll:    localeDeDE,
			args:     []string{"a"},
			expected: localeDeDE,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(EnvLang, c.Lang)
			t.Setenv(EnvLcAll, c.LcAll)
			os.Args = c.args
			l10Messages = nil
			SystemLocale = ""
			InitializeSystemLocale()
			assert.Equal(t, SystemLocale, c.expected, "Incorrect default locale.")
		})
	}
}
