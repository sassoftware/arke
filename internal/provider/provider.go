// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/util"
)

// Provider interface is for broker providers. For instance, to access
// a message broker using amqp091 (such as RabbitMQ), an amqp091 provider
// would implement this interface.
type Provider interface {
	Publish(context.Context, <-chan *pb.Message, chan<- *pb.Error) *pb.Error
	PublishOne(context.Context, *pb.Message) *pb.Error
	Subscribe(context.Context, *pb.Source, chan<- *pb.Message) *pb.Error
	Ack(context.Context, string) *pb.Error
	Nack(context.Context, string) *pb.Error
	Retry(context.Context, *pb.Source, string, int32) *pb.Error
	DeadLetter(context.Context, *pb.Source, string) *pb.Error
	Connect(context.Context, *pb.ConnectionConfiguration, bool) *pb.Error
	Disconnect(context.Context)
	SupportedSourceOptions() map[string]bool
	WaitForConnect(context.Context) bool
	Stats() *Stats
	ClientExists(string) bool
	SourceStats(context.Context, *pb.Source) *pb.SourceStats
}

// Factory method for creating a specific provider
type Factory func() Provider

// Map of our registered Provider types
var registeredProviderTypes = util.NewConcurrentMap()

// Map of registered Providers
var registeredProviders = util.NewConcurrentMap()

type providerOnce struct {
	m    sync.Mutex
	done uint32
}

// ClientStats stats for each connected client
type ClientStats struct {
	sync.Mutex
	ID             string
	ActiveMessages int
	Streams        int
	Produced       int
	Consumed       int
}

// Stats metrics for the provider
type Stats struct {
	Clients []*ClientStats
}

var providerVault = util.NewConcurrentMap()

func (po *providerOnce) Do(f func() Provider) Provider {
	if atomic.LoadUint32(&po.done) == 1 {
		return nil
	}
	// Slow-path.
	po.m.Lock()
	defer po.m.Unlock()
	if po.done == 0 {
		defer atomic.StoreUint32(&po.done, 1)
		return f()
	}
	return nil
}

// NewProvider creates a new provider of the given type
func NewProvider(providerType string) (Provider, error) {
	pf, ok := registeredProviderTypes.Get(providerType)
	if !ok {
		providerList := registeredProviderTypes.GetList()
		return nil, fmt.Errorf("invalid provider name. Must be one of: %s", strings.Join(providerList, ","))
	}
	var provOnce providerOnce
	// store provOnce in a map so it doesn't get gc'd
	providerVault.Add(providerType, &provOnce)
	providerFactory := pf.(Factory)
	// This ensures we only generate one provider of providerType
	provider := provOnce.Do(func() Provider { return providerFactory() })
	return provider, nil
}

// GetProvider returns a provider of the given type. If the provider is not
// already created, it will create a new one.
func GetProvider(providerType string) (Provider, error) {
	_, registered := registeredProviders.Get(providerType)

	if !registered {
		util.Logger.Debugf("Provider %s not found in cache, creating new provider\n", providerType)
		newProv, newProvErr := NewProvider(providerType)
		if newProv != nil {
			registeredProviders.Add(providerType, newProv)
		}
		if newProvErr != nil {
			return nil, newProvErr
		}
	}
	prov, _ := registeredProviders.Get(providerType)

	return prov.(Provider), nil
}

// Register registers a provider's name with the factory method to create it
func Register(name string, factory Factory) {
	if factory == nil {
		util.Logger.Debugf("Provider factory %s can not be nil.", name)
	} else {
		_, registered := registeredProviderTypes.Get(name)
		if registered {
			util.Logger.Debugf("Provider factory %s already registered. Ignoring.", name)
		} else {
			registeredProviderTypes.Add(name, factory)
			util.Logger.Debugf("Registering Provider %s.", name)
		}
	}
}

func RegisteredProviders() *util.ConcurrentMap {
	return registeredProviders
}
