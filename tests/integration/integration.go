// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"embed"
	"fmt"
	"os"
	"path"
)

//go:embed *_test.go
var testFiles embed.FS

func WriteIntegrationTestFiles(outDir string) error {
	err := os.MkdirAll(outDir, 0775)
	if err != nil {
		return err
	}

	files, err := testFiles.ReadDir(".")
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if path.Ext(file.Name()) == ".go" {
			err := writeTestFile(file.Name(), outDir)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func writeTestFile(fn, outDir string) error {
	outfile := path.Join(outDir, fn)
	contents, err := testFiles.ReadFile(fn)
	if err != nil {
		return err
	}
	fmt.Println("Writing test file:", outfile)
	return os.WriteFile(outfile, contents, 0644)
}
