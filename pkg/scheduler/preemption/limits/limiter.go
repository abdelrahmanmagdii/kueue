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

package limits

import (
	"fmt"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/util/expectations"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
)

// selectorError reports an invalid selector on a PreemptionLimit.
type selectorError struct {
	PreemptionLimit string
	Selector        string
	Err             error
}

func (e *selectorError) Error() string {
	return fmt.Sprintf("preemptionLimit %q %s: %v", e.PreemptionLimit, e.Selector, e.Err)
}

func (e *selectorError) Unwrap() error {
	return e.Err
}

// applicableLimit is a PreemptionLimit that applies to one PreemptionConfig,
// with its ClusterQueue selectors compiled once for the scheduling attempt.
type applicableLimit struct {
	limit                         *kueuealpha.PreemptionLimit
	preemptorClusterQueueSelector labels.Selector
	preempteeClusterQueueSelector labels.Selector
}

// newApplicableLimit returns limit when it applies to config, or nil when its
// configSelector does not. It returns an error if a selector it needs is invalid.
// ClusterQueue selectors are parsed only after the config matches, so an invalid
// ClusterQueue selector on a limit that does not apply to config is ignored.
func newApplicableLimit(limit *kueuealpha.PreemptionLimit, config *kueuealpha.PreemptionConfig) (*applicableLimit, error) {
	configSelector, err := metav1.LabelSelectorAsSelector(&limit.Spec.ConfigSelector)
	if err != nil {
		return nil, &selectorError{PreemptionLimit: limit.Name, Selector: "configSelector", Err: err}
	}
	if !configSelector.Matches(labels.Set(config.Labels)) {
		return nil, nil
	}
	preemptorClusterQueueSelector, err := selectorOrEverything(limit.Spec.PreemptorClusterQueueSelector)
	if err != nil {
		return nil, &selectorError{PreemptionLimit: limit.Name, Selector: "preemptorClusterQueueSelector", Err: err}
	}
	preempteeClusterQueueSelector, err := selectorOrEverything(limit.Spec.PreempteeClusterQueueSelector)
	if err != nil {
		return nil, &selectorError{PreemptionLimit: limit.Name, Selector: "preempteeClusterQueueSelector", Err: err}
	}
	return &applicableLimit{
		limit:                         limit,
		preemptorClusterQueueSelector: preemptorClusterQueueSelector,
		preempteeClusterQueueSelector: preempteeClusterQueueSelector,
	}, nil
}

// selectorOrEverything is metav1.LabelSelectorAsSelector, except that nil selects everything.
func selectorOrEverything(ls *metav1.LabelSelector) (labels.Selector, error) {
	if ls == nil {
		return labels.Everything(), nil
	}
	return metav1.LabelSelectorAsSelector(ls)
}

// matchesClusterQueues reports whether preemptorCQ and targetCQ match the
// compiled ClusterQueue selectors of a limit.
func matchesClusterQueues(preemptorSelector, preempteeSelector labels.Selector, preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot) bool {
	return preemptorSelector.Matches(labels.Set(preemptorCQ.Labels)) &&
		preempteeSelector.Matches(labels.Set(targetCQ.Labels))
}

// aggregationKey returns the key that identifies this preemption within aggregation.
func aggregationKey(aggregation kueuealpha.PreemptionLimitAggregation, preemptor, target *workload.Info) policy.PreemptionLimitAggregationKey {
	switch aggregation {
	case kueuealpha.ByPreemptorCQLimitAggregation:
		return policy.PreemptionLimitAggregationKey(preemptor.ClusterQueue)
	case kueuealpha.ByPreempteeCQLimitAggregation:
		return policy.PreemptionLimitAggregationKey(target.ClusterQueue)
	case kueuealpha.ByPreempteeWorkloadLimitAggregation:
		return policy.PreemptionLimitAggregationKey(workload.Key(target.Obj))
	default:
		// GlobalLimitAggregation, as the API admits no other aggregation.
		return policy.GlobalAggregationKey
	}
}

// PreemptionLimiter enforces the PreemptionLimits of a snapshot that apply to
// a PreemptionConfig, during a single scheduling attempt.
//
// A nil PreemptionLimiter imposes no limits.
type PreemptionLimiter struct {
	log          logr.Logger
	applicable   map[policy.PreemptionLimitReference]*applicableLimit
	expectations *expectations.Store
	snapshot     *PreemptionLimitsSnapshot
}

// NewPreemptionLimiter returns a PreemptionLimiter for the PreemptionLimits of
// snapshot that apply to config. snapshot, config, and preemptionExpectations must not be nil.
func NewPreemptionLimiter(
	log logr.Logger,
	snapshot *PreemptionLimitsSnapshot,
	config *kueuealpha.PreemptionConfig,
	preemptionExpectations *expectations.Store,
) (*PreemptionLimiter, error) {
	applicable := make(map[policy.PreemptionLimitReference]*applicableLimit)
	for ref, state := range snapshot.limits {
		if state.limit == nil {
			continue
		}
		limit, err := newApplicableLimit(state.limit, config)
		if err != nil {
			return nil, err
		}
		if limit != nil {
			applicable[ref] = limit
		}
	}
	return &PreemptionLimiter{
		log:          log,
		applicable:   applicable,
		expectations: preemptionExpectations,
		snapshot:     snapshot,
	}, nil
}

// consumedLimits returns the limits and aggregation keys that preempting target for
// preemptor consumes, or no limits if target's preemption is already in progress.
func (l *PreemptionLimiter) consumedLimits(
	preemptor, target *workload.Info,
	preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot,
) policy.ConsumedLimits {
	if l == nil || l.preemptionInProgress(target) {
		return nil
	}
	consumedLimits := make(policy.ConsumedLimits)
	for ref, limit := range l.applicable {
		if matchesClusterQueues(limit.preemptorClusterQueueSelector, limit.preempteeClusterQueueSelector, preemptorCQ, targetCQ) {
			consumedLimits[ref] = policy.NewConsumedLimit(limit.limit, aggregationKey(limit.limit.Spec.Aggregation, preemptor, target))
		}
	}
	return consumedLimits
}

// preemptionInProgress reports whether target's eviction is already in progress.
// A waiting preemptor re-selects such a target every cycle until the target's
// quota is released, and IssuePreemptions does not evict or record it again.
func (l *PreemptionLimiter) preemptionInProgress(target *workload.Info) bool {
	return workloadevict.IsEvicted(target.Obj) ||
		!l.expectations.Satisfied(l.log, client.ObjectKeyFromObject(target.Obj))
}
