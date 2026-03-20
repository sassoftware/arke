// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/pkg/arke"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func testHealth(port int) error {
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewHealthzClient(conn)
	ctx := context.Background()
	_, err = c.Check(ctx)
	return err
}

func TestMonitorProcessStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50058)
		assert.Nil(t, err)
		cancel()
	}()
	os.Setenv(arke.EnvPort, "50058")
	defer os.Unsetenv(arke.EnvPort)
	err := run(ctx)
	assert.Nil(t, err)
}
func TestRunWithCPUProfile(t *testing.T) {
	tmpFile, err := os.CreateTemp(t.TempDir(), "cpuprofile-*.prof")
	assert.Nil(t, err)
	tmpFile.Close()

	*cpuprofile = tmpFile.Name()
	defer func() { *cpuprofile = "" }()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50059)
		assert.Nil(t, err)
		cancel()
	}()
	os.Setenv(arke.EnvPort, "50059")
	defer os.Unsetenv(arke.EnvPort)

	err = run(ctx)
	assert.Nil(t, err)

	info, err := os.Stat(tmpFile.Name())
	assert.Nil(t, err)
	assert.Greater(t, info.Size(), int64(0))
}

func TestRunWithMemProfile(t *testing.T) {
	tmpFile, err := os.CreateTemp(t.TempDir(), "memprofile-*.prof")
	assert.Nil(t, err)
	tmpFile.Close()

	*memprofile = tmpFile.Name()
	defer func() { *memprofile = "" }()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50060)
		assert.Nil(t, err)
		cancel()
	}()
	os.Setenv(arke.EnvPort, "50060")
	defer os.Unsetenv(arke.EnvPort)

	err = run(ctx)
	assert.Nil(t, err)

	info, err := os.Stat(tmpFile.Name())
	assert.Nil(t, err)
	assert.Greater(t, info.Size(), int64(0))
}

func TestRunWithCPUAndMemProfile(t *testing.T) {
	tmpDir := t.TempDir()

	cpuFile, err := os.CreateTemp(tmpDir, "cpuprofile-*.prof")
	assert.Nil(t, err)
	cpuFile.Close()

	memFile, err := os.CreateTemp(tmpDir, "memprofile-*.prof")
	assert.Nil(t, err)
	memFile.Close()

	*cpuprofile = cpuFile.Name()
	*memprofile = memFile.Name()
	defer func() {
		*cpuprofile = ""
		*memprofile = ""
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50061)
		assert.Nil(t, err)
		cancel()
	}()
	os.Setenv(arke.EnvPort, "50061")
	defer os.Unsetenv(arke.EnvPort)

	err = run(ctx)
	assert.Nil(t, err)

	cpuInfo, err := os.Stat(cpuFile.Name())
	assert.Nil(t, err)
	assert.Greater(t, cpuInfo.Size(), int64(0))

	memInfo, err := os.Stat(memFile.Name())
	assert.Nil(t, err)
	assert.Greater(t, memInfo.Size(), int64(0))
}

func TestRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50062)
		assert.Nil(t, err)
		cancel()
	}()
	os.Setenv(arke.EnvPort, "50062")
	defer os.Unsetenv(arke.EnvPort)

	err := run(ctx)
	assert.Nil(t, err)
}

func TestRun_CtxCancelledNoError(t *testing.T) {

	os.Setenv(arke.EnvPort, "50065")
	defer os.Unsetenv(arke.EnvPort)

	ctx, cancel := context.WithCancel(context.Background())
	// sleep for a second then cancel the context
	go func() {
		time.Sleep(1 * time.Second)
		cancel()
	}()
	err := run(ctx)
	assert.Nil(t, err)
}
