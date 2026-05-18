// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"crypto/sha1" //nolint:gosec
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var clientMap = NewConcurrentMap()

func SetClientIdentifier(ctx context.Context, name string) (string, error) {
	clientAddr, err := GetClientAddr(ctx)
	if err != nil {
		return "", err
	}
	// deepcode ignore InsecureHash: no sensitive data-only hashing for a unique client identifier
	h := fmt.Sprintf("%x", sha1.Sum([]byte(clientAddr)))[:8] //nolint:gosec
	clientIdentifier := fmt.Sprintf("%s-%s", name, h)
	clientMap.Add(clientAddr, clientIdentifier)
	return clientIdentifier, err
}

func RemoveClientIdentifier(ctx context.Context) {
	clientAddr, _ := GetClientAddr(ctx)
	clientMap.Delete(clientAddr)
}

// GetClientIdentifier retrieves or generates the client identifier
func GetClientIdentifier(ctx context.Context) (string, error) {
	clientAddr, err := GetClientAddr(ctx)

	if err != nil {
		return "", errors.New("could not retrieve client-id from context")
	}

	if identifier, found := clientMap.Get(clientAddr); found {
		return identifier.(string), nil
	}

	return "", errors.New("could not find client identifier")
}

// ServceNameFromClientAddr returns the service name from the client address.
// The client address typically looks like <service-name>-<random-string>.
// The random string almost always contain numbers, so we can use that to
// to determine the service it came from.
func ServceNameFromClientAddr(clientAddr string) string {
	var serviceName []string
	for token := range strings.SplitSeq(clientAddr, "-") {
		if strings.ContainsAny(token, "0123456789") {
			break
		}
		serviceName = append(serviceName, token)
	}
	return strings.Join(serviceName, "-")
}

// GetClientAddr gets the client-id from the context metadata
func GetClientAddr(ctx context.Context) (string, error) {
	if client, ok := peer.FromContext(ctx); ok {
		if client.Addr.String() == "" {
			return "", errors.New("could not retrieve address info from peer")
		}
		return client.Addr.String(), nil
	}
	return "", errors.New("could not retrieve peer info")
}

func RecoverPanic() {
	if err := recover(); err != nil {
		Logger.Warn(fmt.Sprintf("%v", err))
		return
	}
}

// GenUUID Generate a UUID and return the string representation
func GenUUID() string {
	uuidRaw := uuid.New()
	return uuidRaw.String()
}

func NewTimestampPB() *timestamppb.Timestamp {
	return timestamppb.New(time.Now())
}

func SleepRandom(sleepMin int, sleepMax int) {
	if sleepMax <= sleepMin {
		time.Sleep(time.Duration(sleepMin) * time.Millisecond)
		return
	}
	rn := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec

	splay := time.Duration(rn.Intn(sleepMax-sleepMin)+sleepMin) * time.Millisecond
	time.Sleep(splay)
}

func GetConfig(key string, def interface{}) interface{} {
	val := os.Getenv(key)
	// Can't find the env var, return default value
	if val == "" {
		return def
	}
	// We assume the environment variable is the same type
	// as the default value passed in.
	switch def.(type) {
	case bool:
		boolVal, err := strconv.ParseBool(val)
		if err == nil {
			return boolVal
		}
	case int:
		intVal, err := strconv.Atoi(val)
		if err == nil {
			return intVal
		}
	case string:
		return val
	}
	// If we don't have a case for the type or if we have
	// an error parsing, then return the default value
	return def
}
