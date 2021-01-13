// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubernetes

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AliyunContainerService/kube-eventer/common/kubernetes"
	"github.com/AliyunContainerService/kube-eventer/core"
	kubeapi "k8s.io/api/core/v1"
	kubev1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/klog"
)

const (
	// Number of object pointers. Big enough so it won't be hit anytime soon with reasonable GetNewEvents frequency.
	LocalEventsBufferSize = 100000
)

var (
	// Last time of event since unix epoch in seconds
	lastEventTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "eventer",
			Subsystem: "scraper",
			Name:      "last_time_seconds",
			Help:      "Last time of event since unix epoch in seconds.",
		})
	totalEventsNum = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "eventer",
			Subsystem: "scraper",
			Name:      "events_total_number",
			Help:      "The total number of events.",
		})
	scrapEventsDuration = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace: "eventer",
			Subsystem: "scraper",
			Name:      "duration_milliseconds",
			Help:      "Time spent scraping events in milliseconds.",
		})
)

func init() {
	prometheus.MustRegister(lastEventTimestamp)
	prometheus.MustRegister(totalEventsNum)
	prometheus.MustRegister(scrapEventsDuration)
}

type KubernetesEventSource struct {
	// Large local buffer, periodically read.
	localEventsBuffer chan *kubeapi.Event
	informer          cache.SharedIndexInformer
	stop              chan struct{}
	eventClient       kubev1core.EventInterface
}

func (this *KubernetesEventSource) GetNewEvents() *core.EventBatch {
	startTime := time.Now()
	defer func() {
		lastEventTimestamp.Set(float64(time.Now().Unix()))
		scrapEventsDuration.Observe(float64(time.Since(startTime)) / float64(time.Millisecond))
	}()
	result := core.EventBatch{
		Timestamp: time.Now(),
		Events:    []*kubeapi.Event{},
	}
	// Get all data from the buffer.
event_loop:
	for {
		select {
		case event := <-this.localEventsBuffer:
			result.Events = append(result.Events, event)
		default:
			break event_loop
		}
	}

	totalEventsNum.Add(float64(len(result.Events)))

	return &result
}

func (this *KubernetesEventSource) watch() {

	this.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			event := obj.(*kubeapi.Event)
			isNewEvent, _ := compareWithLastResourceVersion(this.eventClient, event.ResourceVersion)
			if isNewEvent {
				this.localEventsBuffer <- event
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			event := newObj.(*kubeapi.Event)
			isNewEvent, _ := compareWithLastResourceVersion(this.eventClient, event.ResourceVersion)
			if isNewEvent {
				this.localEventsBuffer <- event
			}
		},
		DeleteFunc: func(obj interface{}) {
			// delete event not send
		},
	})
	this.informer.Run(this.stop)
}

func NewKubernetesSource(uri *url.URL) (*KubernetesEventSource, error) {
	kubeClient, err := kubernetes.GetKubernetesClient(uri)
	if err != nil {
		klog.Errorf("Failed to create kubernetes client,because of %v", err)
		return nil, err
	}

	eventClient := kubeClient.CoreV1().Events(kubeapi.NamespaceAll)

	factory := informers.NewSharedInformerFactoryWithOptions(kubeClient, 30*time.Second, informers.WithNamespace(kubeapi.NamespaceAll))
	informer := factory.Core().V1().Events().Informer()

	k8sSource := &KubernetesEventSource{
		localEventsBuffer: make(chan *kubeapi.Event, LocalEventsBufferSize),
		informer:          informer,
		stop:              make(chan struct{}),
		eventClient:       eventClient,
	}

	go k8sSource.watch()
	return k8sSource, nil
}

func compareWithLastResourceVersion(client kubev1core.EventInterface, rv string) (bool, error) {
	lists, err := client.List(metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to load events: %v", err)
		return false, err
	}
	return rv > lists.ResourceVersion, nil
}
