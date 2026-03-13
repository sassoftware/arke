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
