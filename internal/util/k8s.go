// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"os"
	"path/filepath"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	v1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const (
	EnvK8SNamespace = "NAMESPACE"
)

var namespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
var inClusterConfig = rest.InClusterConfig

// MonitorHPA monitors the HPA for changes and sends a GOAWAY signal to the health check
// channel when the replica count is increased
func MonitorHPA(healthChan chan pb.HealthStatus_Code, arkeHpaName string) {
	currentReplicaCount := int32(-1)
	var namespace string
	data, err := os.ReadFile(namespaceFile)
	if err != nil {
		namespace = os.Getenv(EnvK8SNamespace)
	} else {
		namespace = string(data)
	}

	if namespace == "" {
		Logger.Debug("No kubernetes namespace detected, not monitoring HPA for changes")
		return
	}

	config, err := inClusterConfig()
	if err == rest.ErrNotInCluster {
		home := homedir.HomeDir()
		kubeconfig := filepath.Join(home, ".kube", "config")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			Logger.Debugf("Could not configure HPA cluster monitoring: %s", err.Error())
			return
		}
	} else if err != nil {
		Logger.Debugf("Could not configure HPA cluster monitoring: %s", err.Error())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		Logger.Debugf("Could not configure HPA cluster monitoring: %s", err.Error())
		return
	}

	defer func() {
		// protect from send on closed channel
		if err := recover(); err != nil {
			Logger.Debugf("Error monitoring HPA: %s", err)
			return
		}
	}()

	for {
		// hpa := clientset.AutoscalingV1().HorizontalPodAutoscalers()
		watcher, err := clientset.AutoscalingV1().HorizontalPodAutoscalers(namespace).Watch(ctx, metav1.ListOptions{})
		if err != nil {
			Logger.Debugf("Could not get HPA watcher: %s", err)
		}
		if watcher == nil {
			return
		}
		for event := range watcher.ResultChan() {
			hpa := event.Object.(*v1.HorizontalPodAutoscaler)
			if hpa.GetName() != arkeHpaName {
				continue
			}

			newReplicaCount := currentReplicaCount //nolint:wastedassign,ineffassign
			switch event.Type {                    //nolint:exhaustive
			case watch.Modified:
				newReplicaCount = hpa.Status.CurrentReplicas
			default:
				continue
			}

			if currentReplicaCount > 0 && newReplicaCount > currentReplicaCount {
				Logger.Info(i18n.Scaled, arkeHpaName, currentReplicaCount, newReplicaCount)

				// slight delay to let the service start load balancing
				time.Sleep(10 * time.Second)
				healthChan <- pb.HealthStatus_GOAWAY
			}

			currentReplicaCount = newReplicaCount
		}
	}
}
