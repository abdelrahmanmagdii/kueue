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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/core/leaderaware"
)

// WithLeadingManager returns a decorating reconcile.Reconciler that discards reconciliation requests
// for the controllers that are started with the controller.Options.NeedLeaderElection
// option set to false in non-leading replicas.
//
// Starting controllers in non-leading replicas is needed for these that update the data
// served by the visibility extension API server.
//
// This enables to:
//   - Keep the scheduling decisions under the responsibility of the leading replica alone,
//     to prevent any concurrency issues.
//   - Consume requests from the watch event queues, to prevent them from growing indefinitely
//     in the non-leading replicas.
//   - Transition to actually reconciling requests in the replica that may acquire
//     the leader election lease, in case the previously leading replica failed to renew it.
func WithLeadingManager(mgr ctrl.Manager, reconciler reconcile.Reconciler, obj client.Object, cfg *config.Configuration) reconcile.Reconciler {
	return leaderaware.WithLeadingManager(mgr, reconciler, obj, cfg)
}
