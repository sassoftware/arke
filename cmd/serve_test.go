// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"net"
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
	run(ctx)
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

	run(ctx)

	cpuInfo, err := os.Stat(cpuFile.Name())
	assert.Nil(t, err)
	assert.Greater(t, cpuInfo.Size(), int64(0))

	memInfo, err := os.Stat(memFile.Name())
	assert.Nil(t, err)
	assert.Greater(t, memInfo.Size(), int64(0))
}

func TestCheckErr(t *testing.T) {
	isFatal := checkErr(fmt.Errorf("some error"))
	assert.True(t, isFatal)

	isFatal = checkErr(&net.OpError{})
	assert.False(t, isFatal)
}
