// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	"github.com/sassoftware/arke/i18n"
	_ "github.com/sassoftware/arke/internal/provider/connectors"
	"github.com/sassoftware/arke/internal/server/ratelimiter"
	"github.com/sassoftware/arke/internal/util"
	"github.com/sassoftware/arke/pkg/arke"
)

const (
	EnvCertFile = "ARKE_CERT_FILE"
	EnvCertKey  = "ARKE_CERT_KEY"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var tlsSkipVerify = flag.Bool("tls-skip-verify", false, "Force TLS, but always skip verification")

func run(ctx context.Context) error {
	// Set up cpu and memory profiling if passed in as args
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(filepath.Clean(*cpuprofile))
		if err != nil {
			util.Logger.Fatal(i18n.CPUProfileError, err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			util.Logger.Fatal(i18n.CPUProfileError, err)
		}
		defer pprof.StopCPUProfile()
	}

	defer func() {
		if *memprofile != "" {
			f, err := os.Create(filepath.Clean(*memprofile))
			if err != nil {
				util.Logger.Fatal(i18n.MemProfileError, err)
			}
			defer f.Close()
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				util.Logger.Fatal(i18n.MemProfileError, err)
			}
		}
	}()

	certFile := os.Getenv(EnvCertFile)
	certKey := os.Getenv(EnvCertKey)

	rateLimitEnforced := os.Getenv(ratelimiter.EnvRateLimitEnforced)
	bsEnv := os.Getenv(ratelimiter.EnvRateLimitBucketSize)
	maxAgeDuration := os.Getenv(ratelimiter.EnvRateLimitMaxAge)
	refillDuration := os.Getenv(ratelimiter.EnvRateLimitRefill)

	rlp, err := arke.GetRateLimitParameters(bsEnv, refillDuration, maxAgeDuration, rateLimitEnforced)
	if err != nil {
		util.Logger.Warn(i18n.InvalidRateParameters, err)
	}

	svr := arke.DefaultArkeServer().
		WithTLSSkipVerify(*tlsSkipVerify).
		WithCertFilePath(certFile).
		WithCertKeyPath(certKey).
		WithPrometheus().
		WithRateLimit(rlp).
		Build()

	err = svr.Serve(ctx)
	if err != nil {
		switch err.(type) {
		case *net.OpError:
			return nil
		default:
			util.Logger.Fatal(i18n.GenericError, err)
		}
	}
	return err
}

func main() {
	ctx := context.Background()
	run(ctx) //nolint:errcheck
}
