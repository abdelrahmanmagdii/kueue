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

package workload

import (
	"context"
	"fmt"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/util/waitforpodsready"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workload/concurrentadmission"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
	workloadpatching "sigs.k8s.io/kueue/pkg/workload/patching"
)

type waitForPodsReadyConfig struct {
	timeout                     time.Duration
	recoveryTimeout             *time.Duration
	unscheduledTimeout          *time.Duration
	requeuingBackoffLimitCount  *int32
	requeuingBackoffBaseSeconds int32
	requeuingBackoffMaxDuration time.Duration
	requeuingBackoffJitter      float64
}

// WithWaitForPodsReady indicates the configuration for the WaitForPodsReady feature.
func WithWaitForPodsReady(value *waitForPodsReadyConfig) Option {
	return func(r *WorkloadReconciler) {
		r.waitForPodsReady = value
	}
}

func (r *WorkloadReconciler) podsScheduledTrackingEnabled() bool {
	return features.Enabled(features.WaitForPodsReadyUnscheduledTimeout) &&
		r.waitForPodsReady != nil && r.waitForPodsReady.unscheduledTimeout != nil && *r.waitForPodsReady.unscheduledTimeout > 0
}

func (r *WorkloadReconciler) reconcileNotReadyTimeout(ctx context.Context, req ctrl.Request, wl *kueue.Workload) (time.Duration, error) {
	if features.Enabled(features.ConcurrentAdmission) && concurrentadmission.IsVariant(wl) {
		// Variant Workloads are not supposed to have PodsReady condition, it's Parent Workload responsibility.
		return 0, nil
	}
	log := ctrl.LoggerFrom(ctx)

	if !workload.IsActive(wl) || workloadevict.IsEvicted(wl) {
		// the workload has already been evicted by the PodsReadyTimeout or been deactivated.
		return 0, nil
	}

	underlyingCause, recheckAfter := r.admittedNotReadyWorkload(ctx, wl)
	if underlyingCause == "" {
		return 0, nil
	}
	if recheckAfter > 0 {
		log.V(4).Info("Workload not yet ready and did not exceed its timeout", "recheckAfter", recheckAfter)
		return recheckAfter, nil
	}

	deactivated, err := r.triggerDeactivation(ctx, wl)
	if err != nil || deactivated {
		return 0, err
	}

	// Increments a re-queueing count and update a time to be re-queued.
	message := fmt.Sprintf("Exceeded the PodsReady timeout %s", req.String())
	err = workloadevict.Evict(
		ctx,
		r.client,
		r.recorder,
		wl,
		kueue.WorkloadEvictedByPodsReadyTimeout,
		message,
		underlyingCause,
		r.clock,
		r.cache.RecordLocalQueueMetricsForWorkload,
		r.roleTracker,
		r.customLabels,
		workloadevict.WithCustomPrepare(func(wl *kueue.Workload) {
			workload.UpdateRequeueState(wl, r.waitForPodsReady.requeuingBackoffBaseSeconds, int32(r.waitForPodsReady.requeuingBackoffMaxDuration.Seconds()), r.clock)
		}),
	)

	return 0, err
}

// triggerDeactivation trigger deactivation of workload
// if a re-queued number has already exceeded the limit of re-queuing backoff.
// It returns true as a first value if a workload triggered deactivation.
func (r *WorkloadReconciler) triggerDeactivation(ctx context.Context, wl *kueue.Workload) (bool, error) {
	requeueState := ptr.Deref(wl.Status.RequeueState, kueue.RequeueState{})
	// If requeuingBackoffLimitCount equals to null, the workloads is repeatedly and endless re-queued.
	if r.waitForPodsReady.requeuingBackoffLimitCount != nil && ptr.Deref(requeueState.Count, 0)+1 > *r.waitForPodsReady.requeuingBackoffLimitCount {
		if err := workloadpatching.PatchAdmissionStatus(ctx, r.client, wl, r.clock, func(wl *kueue.Workload) (bool, error) {
			return workload.SetDeactivationTarget(wl, kueue.WorkloadRequeuingLimitExceeded, "exceeding the maximum number of re-queuing retries"), nil
		}); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// parseWorkloadTimeouts defines the workload timeouts either from workload
// spec or the annotation.
func parseWorkloadTimeouts(ctx context.Context, wl *kueue.Workload) (*int64, *int64) {
	if !waitforpodsready.WorkloadLevelWaitForPodsReadyEnabled() {
		return nil, nil
	}
	cfg := wl.Spec.WaitForPodsReady
	if cfg == nil {
		// Not yet migrated to the spec field: fall back to the legacy annotation.
		var err error
		cfg, err = waitforpodsready.ParseWaitForPodsReady(wl)
		if err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "Failed to parse WaitForPodsReady annotation; falling back to cluster-level configuration")
			return nil, nil
		}
	}
	if cfg == nil {
		return nil, nil
	}
	return cfg.TimeoutSeconds, cfg.RecoveryTimeoutSeconds
}

// determineTimeouts returns the timeout and recovery timeout for the workload.
// It relies first on the per-workload spec, then on the legacy per-workload annotation,
// and finally falls back to the cluster-level configuration.
// It returns ok=false when no workload timeout or cluster configuration is available.
func (r *WorkloadReconciler) determineTimeouts(ctx context.Context, wl *kueue.Workload) (timeout time.Duration, recoveryTimeout *time.Duration, ok bool) {
	workloadTimeoutSeconds, workloadRecoveryTimeoutSeconds := parseWorkloadTimeouts(ctx, wl)
	hasClusterConfig := r.waitForPodsReady != nil

	// Resolve the timeout, preferring the per-workload value over the cluster-level one.
	switch {
	case workloadTimeoutSeconds != nil && *workloadTimeoutSeconds > 0:
		timeout = time.Duration(*workloadTimeoutSeconds) * time.Second
	case hasClusterConfig:
		timeout = r.waitForPodsReady.timeout
	default:
		// No start timeout is configured at either level.
		return 0, nil, false
	}

	// Resolve the recovery timeout with the same per-workload-over-cluster precedence.
	switch {
	case workloadRecoveryTimeoutSeconds != nil && *workloadRecoveryTimeoutSeconds > 0:
		recoveryTimeout = new(time.Duration(*workloadRecoveryTimeoutSeconds) * time.Second)
	case hasClusterConfig:
		recoveryTimeout = r.waitForPodsReady.recoveryTimeout
	}

	return timeout, recoveryTimeout, true
}

// admittedNotReadyWorkload returns the underlying cause and remaining time for
// a workload that is admitted but not yet in PodsReady condition.
//
// If the workload is not admitted, PodsReady is true, or no timeout is configured,
// it returns an empty underlyingCause and zero duration.
func (r *WorkloadReconciler) admittedNotReadyWorkload(ctx context.Context, wl *kueue.Workload) (kueue.EvictionUnderlyingCause, time.Duration) {
	timeout, recoveryTimeout, ok := r.determineTimeouts(ctx, wl)
	if !ok {
		return "", 0
	}
	if !workload.IsAdmitted(wl) {
		// the workload is not admitted so there is no need to time it out
		return "", 0
	}

	podsReadyCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadPodsReady)
	if podsReadyCond != nil && podsReadyCond.Status == metav1.ConditionTrue {
		return "", 0
	}

	admittedAt := apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadAdmitted).LastTransitionTime.Time
	if r.podsScheduledTrackingEnabled() && podsReadyCond != nil && podsReadyCond.Reason == kueue.WorkloadWaitForScheduling {
		return kueue.WorkloadWaitForScheduling, r.remainingUntil(r.schedulingDeadline(wl, admittedAt, timeout))
	}

	switch {
	case podsReadyCond == nil, podsReadyCond.Reason == kueue.WorkloadWaitForStart, podsReadyCond.Reason == kueue.WorkloadPodsReady,
		podsReadyCond.Reason == kueue.WorkloadWaitForScheduling:
		return kueue.WorkloadWaitForStart, r.remainingUntil(admittedAt.Add(timeout))
	case podsReadyCond.Reason == kueue.WorkloadWaitForRecovery && recoveryTimeout != nil:
		// A pod has failed and the workload is waiting for recovery
		elapsedTime := r.clock.Since(podsReadyCond.LastTransitionTime.Time)
		return kueue.WorkloadWaitForRecovery, max(*recoveryTimeout-elapsedTime, 0)
	}
	return "", 0
}

func (r *WorkloadReconciler) schedulingDeadline(wl *kueue.Workload, admittedAt time.Time, timeout time.Duration) time.Time {
	deadline := metav1.NewTime(admittedAt.Add(timeout))
	if r.waitForPodsReady.unscheduledTimeout == nil {
		return deadline.Time
	}
	cur := workload.CurrentPodsScheduledCondition(wl, admittedAt)
	if cur == nil || cur.Status != metav1.ConditionFalse {
		return deadline.Time
	}
	schedulingDeadline := metav1.NewTime(admittedAt.Add(*r.waitForPodsReady.unscheduledTimeout))
	if schedulingDeadline.Before(&deadline) {
		return schedulingDeadline.Time
	}
	return deadline.Time
}

func (r *WorkloadReconciler) remainingUntil(deadline time.Time) time.Duration {
	return max(deadline.Sub(r.clock.Now()), 0)
}

// WaitForPodsReady builds the controller config from the cluster WaitForPodsReady settings.
func WaitForPodsReady(cfg *config.WaitForPodsReady) *waitForPodsReadyConfig {
	if !waitforpodsready.Enabled(cfg) {
		return nil
	}
	result := waitForPodsReadyConfig{
		timeout: cfg.Timeout.Duration,
	}
	result.recoveryTimeout = waitforpodsready.RecoveryTimeout(cfg)
	if waitforpodsready.PodsScheduledTrackingEnabled(cfg) {
		result.unscheduledTimeout = &cfg.UnscheduledTimeout.Duration
	}
	if cfg.RequeuingStrategy != nil {
		result.requeuingBackoffBaseSeconds = *cfg.RequeuingStrategy.BackoffBaseSeconds
		result.requeuingBackoffLimitCount = cfg.RequeuingStrategy.BackoffLimitCount
		result.requeuingBackoffMaxDuration = time.Duration(*cfg.RequeuingStrategy.BackoffMaxSeconds) * time.Second
		result.requeuingBackoffJitter = 0.0001
	}
	return &result
}
