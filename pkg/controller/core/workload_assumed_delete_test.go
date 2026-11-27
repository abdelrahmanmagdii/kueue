/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package core

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/core/workload"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

// Deleting an assumed Workload after its LocalQueue must still request its
// ClusterQueue, or the finalizer stays until an unrelated event. A Workload
// moved to another LocalQueue after the assumption must request both queues.
//
// This test stays in package core because it reads the unexported update
// channels of the LocalQueue and ClusterQueue controllers. The workload
// package cannot import core.
func TestDeleteAssumedWorkloadRequestsClusterQueue(t *testing.T) {
	cases := map[string]struct {
		localQueueDeleted bool
		movedToQueue      kueue.LocalQueueName
		wantCQs           []kueue.ClusterQueueReference
		wantLQs           []kueue.LocalQueueName
	}{
		"LocalQueue present": {
			wantCQs: []kueue.ClusterQueueReference{"cq"},
			wantLQs: []kueue.LocalQueueName{"lq"},
		},
		"LocalQueue deleted": {
			localQueueDeleted: true,
			wantCQs:           []kueue.ClusterQueueReference{"cq"},
			wantLQs:           []kueue.LocalQueueName{"lq"},
		},
		"Workload moved to another LocalQueue": {
			movedToQueue: "other-lq",
			wantCQs:      []kueue.ClusterQueueReference{"cq", "other-cq"},
			wantLQs:      []kueue.LocalQueueName{"lq", "other-lq"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, log := utiltesting.ContextWithLog(t)
			cl := utiltesting.NewClientBuilder().Build()
			cache := schdcache.New(cl)
			queues := qcache.NewManagerForUnitTests(cl, cache, qcache.WithPreemptionExpectations(preemptexpectations.New()))
			setupAssumedClusterQueue(ctx, t, cl, queues, cache, utiltestingapi.MakeClusterQueue("cq").Active(metav1.ConditionTrue).Obj())
			setupAssumedClusterQueue(ctx, t, cl, queues, cache, utiltestingapi.MakeClusterQueue("other-cq").Active(metav1.ConditionTrue).Obj())
			if !tc.localQueueDeleted {
				setupAssumedLocalQueue(ctx, t, cl, queues, utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue("cq").Obj())
			}
			setupAssumedLocalQueue(ctx, t, cl, queues, utiltestingapi.MakeLocalQueue("other-lq", "ns").ClusterQueue("other-cq").Obj())
			lqReconciler := NewLocalQueueReconciler(cl, queues, cache)
			cqReconciler := NewClusterQueueReconciler(cl, queues, cache)
			r := workload.NewWorkloadReconciler(cl, queues, cache, &utiltesting.EventRecorder{},
				workload.WithPreemptionExpectations(preemptexpectations.New()), workload.WithWorkloadUpdateWatchers(lqReconciler, cqReconciler))
			// Watchers send into buffered channels before returning, so a non-blocking drain collects every request.
			requested := func() (cqs []kueue.ClusterQueueReference, lqs []kueue.LocalQueueName) {
				for {
					select {
					case e := <-cqReconciler.nonCQObjectUpdateCh:
						cqs = slices.AppendSeq(cqs, e.Object)
					case e := <-lqReconciler.wlUpdateCh:
						lqs = append(lqs, e.Object.(*kueue.Workload).Spec.QueueName)
					default:
						return cqs, lqs
					}
				}
			}
			pending := utiltestingapi.MakeWorkload("wl", "ns").Queue("lq").Request(corev1.ResourceCPU, "1")
			if !cache.AddOrUpdateWorkload(ctx, log, pending.Clone().SimpleReserveQuota("cq", "rf", time.Now()).Obj()) {
				t.Fatal("adding scheduler assumption")
			}
			deleted := pending.Obj()
			if tc.movedToQueue != "" {
				deleted = pending.Clone().Queue(tc.movedToQueue).Obj()
				r.UpdateForTest(ctx, event.TypedUpdateEvent[*kueue.Workload]{ObjectOld: pending.Obj(), ObjectNew: deleted})
				requested()
			}

			r.DeleteForTest(ctx, event.TypedDeleteEvent[*kueue.Workload]{Object: deleted})

			if !cache.ClusterQueueEmpty("cq") {
				t.Fatal("deleted workload is still in the cache")
			}
			gotCQs, gotLQs := requested()
			if diff := cmp.Diff(tc.wantCQs, gotCQs); diff != "" {
				t.Errorf("requested ClusterQueues (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantLQs, gotLQs); diff != "" {
				t.Errorf("requested LocalQueues (-want,+got):\n%s", diff)
			}
		})
	}
}

// setupAssumedClusterQueue matches setupClusterQueue in the workload package with shouldDelete false.
func setupAssumedClusterQueue(ctx context.Context, t *testing.T, cl client.Client, qManager *qcache.Manager, cqCache *schdcache.Cache, cq *kueue.ClusterQueue) {
	t.Helper()
	testCq := cq.DeepCopy()
	if err := cl.Create(ctx, testCq); err != nil {
		t.Fatalf("couldn't create the cluster queue: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, testCq); err != nil {
		t.Fatalf("couldn't add the cluster queue to the cache: %v", err)
	}
	if apimeta.IsStatusConditionTrue(testCq.Status.Conditions, kueue.ClusterQueueActive) {
		if err := cqCache.AddClusterQueue(ctx, testCq); err != nil {
			t.Fatalf("couldn't add the cluster queue to the scheduler cache: %v", err)
		}
	}
}

// setupAssumedLocalQueue matches setupLocalQueue in the workload package with shouldDelete false.
func setupAssumedLocalQueue(ctx context.Context, t *testing.T, cl client.Client, qManager *qcache.Manager, lq *kueue.LocalQueue) {
	t.Helper()
	testLq := lq.DeepCopy()
	if err := cl.Create(ctx, testLq); err != nil {
		t.Fatalf("couldn't create the local queue: %v", err)
	}
	if err := qManager.AddLocalQueue(ctx, testLq); err != nil {
		t.Fatalf("couldn't add the local queue to the cache: %v", err)
	}
}
