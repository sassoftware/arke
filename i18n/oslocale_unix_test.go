// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build darwin || freebsd || linux || netbsd || openbsd
// +build darwin freebsd linux netbsd openbsd

package i18n

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectIETF_LC_ALL(t *testing.T) {
	t.Setenv(EnvLcAll, "fr_FR.UTF-8")
	result, _ := DetectIETF()
	assert.Equal(t, result, "fr-FR")
}

func TestDetectIETF_LANG(t *testing.T) {
	t.Setenv(EnvLang, "fr_FR.UTF-8")
	result, _ := DetectIETF()
	assert.Equal(t, result, "fr-FR")
}

func TestDetectIETF_Blank(t *testing.T) {
	t.Setenv(EnvLcAll, "")
	t.Setenv(EnvLang, "")
	result, _ := DetectIETF()
	assert.Equal(t, result, "")
}

func TestStandardInit(t *testing.T) {
	t.Setenv(EnvLang, "")
	t.Setenv(EnvLcAll, "")
	locale, _ := DetectIETF()
	if locale == "" {
		locale = defaultLanguage
	}
	assert.Equal(t, locale, "en", "Incorrect default locale.")
}

func TestLangC(t *testing.T) {
	t.Setenv(EnvLcAll, "")
	t.Setenv(EnvLang, "C")
	locale, err := DetectIETF()
	assert.NotNil(t, err)
	assert.Equal(t, "", locale)

	t.Setenv(EnvLang, "C.UTF-8")
	locale, err = DetectIETF()
	assert.NotNil(t, err)
	assert.Equal(t, "", locale)
}

func TestInvalidLang(t *testing.T) {
	t.Setenv(EnvLcAll, "")
	t.Setenv(EnvLang, "invalid")
	locale, err := DetectIETF()
	assert.NotNil(t, err)
	assert.Equal(t, "", locale)
}
