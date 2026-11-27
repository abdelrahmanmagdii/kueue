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
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/dra"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/util/api"
	qutil "sigs.k8s.io/kueue/pkg/util/queue"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadpatching "sigs.k8s.io/kueue/pkg/workload/patching"
)

// handleDRAConsumableCapacity processes capacity-based resources for consumable
// capacity devices and merges them into draResources.
// On success it returns the updated map and done=false.
// On failure it marks the workload inadmissible and returns done=true: the
// caller must return (result, err) immediately. Internal (retryable) errors
// return a non-nil err so the reconciler retries with backoff; deterministic
// errors return nil err (no retry).
func (r *WorkloadReconciler) handleDRAConsumableCapacity(
	ctx context.Context,
	wl *kueue.Workload,
	sliceCache *dra.ResourceSliceCache,
	draResources map[kueue.PodSetReference]corev1.ResourceList,
) (updated map[kueue.PodSetReference]corev1.ResourceList, done bool, result ctrl.Result, err error) {
	capacityResources, ccFieldErrs := dra.DetermineCapacityResourcesForWorkload(ctx, r.client, sliceCache, r.draMapper, wl)
	if len(ccFieldErrs) > 0 {
		return nil, true, ctrl.Result{}, r.markDRAInadmissible(ctx, wl, ccFieldErrs, "Failed to process DRA capacity resources for workload")
	}

	return dra.MergeDRAResources(draResources, capacityResources), false, ctrl.Result{}, nil
}

func isDRAInadmissibleReason(reason string) bool {
	// Keep WorkloadInadmissible for backwards compatibility while
	// UnadmittedWorkloadsObservability is disabled.
	return reason == kueue.WorkloadInadmissible || reason == kueue.WorkloadDRAResourcesUnresolved
}

func draConditionReasonsWithLegacyFallback() (quotaReservedReason, requeuedReason string) {
	return workload.UnadmittedWorkloadReasonWithFallback(
			kueue.WorkloadQuotaReservedReasonDRAResourcesUnresolved,
			kueue.WorkloadInadmissible,
		),
		workload.UnadmittedWorkloadReasonWithFallback(
			kueue.WorkloadDRAResourcesUnresolved,
			kueue.WorkloadInadmissible,
		)
}

// handleDRA preprocesses DRA-backed resources for a pending workload. It does not
// queue the workload; Reconcile does that once with the returned queueOptions.
// Returns done=true when reconciliation should stop (error or terminal DRA outcome).
// When done=false, queueOptions holds the InfoOptions Reconcile must pass to
// AddOrUpdateWorkload.
func (r *WorkloadReconciler) handleDRA(ctx context.Context, wl *kueue.Workload) (done bool, result ctrl.Result, queueOptions []workload.InfoOption, err error) {
	log := ctrl.LoggerFrom(ctx)

	wi := workload.NewInfoFromClient(ctx, r.client, wl, workload.WithSkipSchedulingHash())
	if workload.HasResourceClaim(wl) {
		reason := workload.UnadmittedWorkloadReasonWithFallback(kueue.WorkloadQuotaReservedReasonMisconfigured, kueue.WorkloadInadmissible)
		message := "KueueDRAIntegration feature does not support use of resource claims"
		var changed bool
		err := workloadpatching.PatchAdmissionStatus(ctx, r.client, wl, r.clock, func(wl *kueue.Workload) (bool, error) {
			updated := workload.UnsetQuotaReservationWithCondition(wl, reason, message, r.clock.Now())
			requeuedChanged := workload.SetRequeuedCondition(wl, kueue.WorkloadInadmissible, "DRA resource claims not supported", false)
			changed = updated || requeuedChanged
			return changed, nil
		})
		if err != nil {
			return true, ctrl.Result{}, nil, fmt.Errorf("failed to update workload status for DRA resource claims error: %w", err)
		}
		if changed {
			log.V(3).Info("Workload status updated", "reason", reason, "message", message)
		}
		return true, ctrl.Result{}, nil, nil
	}

	sliceCache := dra.NewResourceSliceCache(r.client)
	skipDeviceCheck := r.hasMultiKueueAdmissionCheck(wl)

	// Process ResourceClaimTemplates (existing DRA path)
	draResources, fieldErrs := dra.GetResourceRequestsForResourceClaimTemplates(ctx, r.client, sliceCache, r.draMapper, wl, skipDeviceCheck)
	if len(fieldErrs) > 0 {
		return true, ctrl.Result{}, nil, r.markDRAInadmissible(ctx, wl, fieldErrs, "Failed to process DRA resources for workload")
	}

	// Process Extended Resources backed by DRA
	var replacedExtendedResources map[kueue.PodSetReference]sets.Set[corev1.ResourceName]
	if features.Enabled(features.KueueDRAIntegrationExtendedResource) {
		extendedResources, replaced, extFieldErrs := dra.ResolveExtendedResourceQuota(ctx, r.client, r.draMapper, wi)
		if len(extFieldErrs) > 0 {
			return true, ctrl.Result{}, nil, r.markDRAInadmissible(ctx, wl, extFieldErrs, "Failed to process DRA extended resources for workload")
		}
		// Merge extended resources into draResources. When a DeviceClass appears
		// in both paths, the extended resources path uses the deviceClassMappings
		// logical name as the quota key, unifying quota accounting.
		draResources = dra.MergeDRAResources(draResources, extendedResources)
		replacedExtendedResources = replaced
	}

	// Process counter-based resources for partitionable devices
	if features.Enabled(features.KueueDRAIntegrationPartitionableDevices) && r.resourceSliceAPIAvailable {
		counterResources, counterFieldErrs := dra.GetCounterResourcesForWorkload(ctx, r.client, sliceCache, r.draMapper, wl)
		if len(counterFieldErrs) > 0 {
			return true, ctrl.Result{}, nil, r.markDRAInadmissible(ctx, wl, counterFieldErrs, "Failed to process DRA counter resources for workload")
		}
		draResources = dra.MergeDRAResources(draResources, counterResources)
	}

	// Process capacity-based resources for consumable capacity devices
	if features.Enabled(features.KueueDRAIntegrationConsumableCapacity) && r.resourceSliceAPIAvailable {
		ccResources, ccDone, ccResult, ccErr := r.handleDRAConsumableCapacity(ctx, wl, sliceCache, draResources)
		if ccDone {
			return true, ccResult, nil, ccErr
		}
		draResources = ccResources
	}

	// If the workload was held back due to inadmissible DRA resources (Requeued=False/Inadmissible),
	// persist Requeued=True now that DRA processing succeeded so the scheduler can pick it up.
	// Non-DRA reasons (e.g. PodsReadyTimeout) are left untouched for the backoff path to handle.
	var conditionsChanged bool
	requeuedCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadRequeued)
	if requeuedCond != nil && requeuedCond.Status == metav1.ConditionFalse && isDRAInadmissibleReason(requeuedCond.Reason) {
		if err := workloadpatching.PatchAdmissionStatus(ctx, r.client, wl, r.clock, func(wl *kueue.Workload) (bool, error) {
			conditionsChanged = workload.SetRequeuedCondition(wl, kueue.WorkloadDRAResourcesResolved, "DRA resources were resolved after a previous inadmissible marking", true)
			return conditionsChanged, nil
		}); err != nil {
			return true, ctrl.Result{}, nil, fmt.Errorf("failed to persist DRA resources resolved condition: %w", err)
		}
	}

	if conditionsChanged {
		log.V(3).Info("Updated previous inadmissible conditions after successful DRA processing")
	}

	queueOptions = []workload.InfoOption{workload.WithEffectivePodSpecs(wi.EffectivePodSpecs)}
	if len(draResources) > 0 || len(replacedExtendedResources) > 0 {
		queueOptions = append(queueOptions, workload.WithPreprocessedDRAResources(draResources, replacedExtendedResources))
	}
	log.V(3).Info("Successfully pre-processed DRA workload")
	return false, ctrl.Result{}, queueOptions, nil
}

func (r *WorkloadReconciler) hasMultiKueueAdmissionCheck(wl *kueue.Workload) bool {
	if wl.Spec.QueueName == "" {
		return false
	}
	cqName, ok := r.queues.ClusterQueueFromLocalQueue(qutil.KeyFromWorkload(wl))
	if !ok {
		return false
	}
	for _, ac := range r.cache.AdmissionChecksForClusterQueue(cqName) {
		if ac.Controller == kueue.MultiKueueControllerName {
			return true
		}
	}
	return false
}

func (r *WorkloadReconciler) markDRAInadmissible(ctx context.Context, wl *kueue.Workload, fieldErrs field.ErrorList, logMsg string) error {
	processingErr := fieldErrs.ToAggregate()
	quotaReservedReason, requeuedReason := draConditionReasonsWithLegacyFallback()
	existingCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadQuotaReserved)
	requeuedCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadRequeued)
	shouldEmitWarning := existingCond == nil ||
		existingCond.Status != metav1.ConditionFalse ||
		existingCond.Reason != quotaReservedReason ||
		existingCond.Message != api.TruncateConditionMessage(processingErr.Error()) ||
		(requeuedCond != nil && requeuedCond.Status == metav1.ConditionTrue)
	var changed bool
	updateErr := workloadpatching.PatchAdmissionStatus(ctx, r.client, wl, r.clock, func(wl *kueue.Workload) (bool, error) {
		updated := workload.UnsetQuotaReservationWithCondition(wl, quotaReservedReason, processingErr.Error(), r.clock.Now())
		requeuedChanged := workload.SetRequeuedCondition(wl, requeuedReason, processingErr.Error(), false)
		changed = updated || requeuedChanged
		return changed, nil
	})
	// handleUpdate does not update the queue for DRA workloads, so remove the entry
	// queued from an earlier successful DRA processing.
	r.queues.DeleteWorkload(ctrl.LoggerFrom(ctx), workload.Key(wl))
	if updateErr != nil {
		return errors.Join(
			fmt.Errorf("%s: %w", logMsg, processingErr),
			fmt.Errorf("failed to persist DRA inadmissibility: %w", updateErr),
		)
	}
	if shouldEmitWarning {
		r.recorder.Eventf(
			wl,
			nil,
			corev1.EventTypeWarning,
			requeuedReason,
			requeuedReason,
			"%s",
			api.TruncateEventMessage(processingErr.Error()),
		)
	}
	if workload.HasInternalError(fieldErrs) {
		return fmt.Errorf("%s: %w", logMsg, processingErr)
	}
	if changed {
		ctrl.LoggerFrom(ctx).V(3).Info("Workload status updated", "reason", quotaReservedReason, "message", processingErr.Error())
	}
	return nil
}

func WithDRAMapper(value *dra.ResourceMapper) Option {
	return func(r *WorkloadReconciler) {
		r.draMapper = value
	}
}

func WithDRABackedResources(value *dra.ExtendedResourceCache) Option {
	return func(r *WorkloadReconciler) {
		r.draBackedResources = value
	}
}

// WithResourceSliceAPIAvailable indicates whether the ResourceSlice API
// (resource.k8s.io/v1) is available on the cluster. When false, the
// KueueDRAIntegrationPartitionableDevices and KueueDRAIntegrationConsumableCapacity
// code paths are skipped to avoid reconciliation errors on clusters without ResourceSlice API.
func WithResourceSliceAPIAvailable(value bool) Option {
	return func(r *WorkloadReconciler) {
		r.resourceSliceAPIAvailable = value
	}
}

// rejectDRAWhenDisabled marks a pending workload that uses DRA resources as
// inadmissible because the KueueDRAIntegration feature gate is not enabled.
func (r *WorkloadReconciler) rejectDRAWhenDisabled(ctx context.Context, wl *kueue.Workload) (ctrl.Result, error) {
	quotaReservedReason := workload.UnadmittedWorkloadReasonWithFallback(
		kueue.WorkloadQuotaReservedReasonMisconfigured,
		kueue.WorkloadInadmissible,
	)
	message := "Workload uses DRA resources but the KueueDRAIntegration feature gate is not enabled"
	var changed bool
	err := workloadpatching.PatchAdmissionStatus(ctx, r.client, wl, r.clock, func(wl *kueue.Workload) (bool, error) {
		updated := workload.UnsetQuotaReservationWithCondition(wl, quotaReservedReason, message, r.clock.Now())
		requeuedChanged := workload.SetRequeuedCondition(wl, kueue.WorkloadInadmissible, message, false)
		changed = updated || requeuedChanged
		return changed, nil
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update workload status for DRA rejection: %w", err)
	}
	if changed {
		ctrl.LoggerFrom(ctx).V(3).Info("Workload status updated", "reason", quotaReservedReason, "message", message)
	}
	return ctrl.Result{}, nil
}

type draEventHandler struct{}

var _ handler.TypedEventHandler[*kueue.Workload, reconcile.Request] = (*draEventHandler)(nil)

func (h *draEventHandler) Create(ctx context.Context, e event.TypedCreateEvent[*kueue.Workload], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *draEventHandler) Update(ctx context.Context, e event.TypedUpdateEvent[*kueue.Workload], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *draEventHandler) Delete(ctx context.Context, e event.TypedDeleteEvent[*kueue.Workload], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *draEventHandler) Generic(ctx context.Context, e event.TypedGenericEvent[*kueue.Workload], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	reconcileReq := reconcile.Request{
		Name:      e.Object.Name,
		Namespace: e.Object.Namespace,
	}
	q.Add(reconcileReq)
	log := ctrl.LoggerFrom(ctx).WithValues("workload", klog.KObj(e.Object))
	log.V(5).Info("Queued reconcile for DRA workload from event channel")
}

// GetDRAReconcileChannel returns the DRA reconcile channel for connecting to the queue manager.
func (r *WorkloadReconciler) GetDRAReconcileChannel() chan<- event.TypedGenericEvent[*kueue.Workload] {
	return r.draReconcileChannel
}

type deviceClassHandler struct {
	r *WorkloadReconciler
}

func (h *deviceClassHandler) Create(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.requeuePendingWorkloadsWithClaimTemplates(ctx, q)
	if !features.Enabled(features.KueueDRAIntegrationExtendedResource) {
		return
	}
	dc := e.Object.(*resourcev1.DeviceClass)
	implicitERN := implicitExtendedResourceName(dc)
	h.r.draBackedResources.Add(corev1.ResourceName(implicitERN), dc.Name)
	ern := extendedResourceName(dc)
	if ern != "" {
		h.r.draBackedResources.Add(corev1.ResourceName(ern), dc.Name)
	}
	h.reconcileWorkloads(ctx, q, ern, implicitERN)
}

// Update leaves the implicit extended resource name alone, since it derives from
// the DeviceClass name, which cannot change.
func (h *deviceClassHandler) Update(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	if !features.Enabled(features.KueueDRAIntegrationExtendedResource) {
		return
	}
	oldDC := e.ObjectOld.(*resourcev1.DeviceClass)
	newDC := e.ObjectNew.(*resourcev1.DeviceClass)
	if oldERN := extendedResourceName(oldDC); oldERN != "" {
		h.r.draBackedResources.Remove(corev1.ResourceName(oldERN), oldDC.Name)
	}
	if newERN := extendedResourceName(newDC); newERN != "" {
		h.r.draBackedResources.Add(corev1.ResourceName(newERN), newDC.Name)
	}
	h.reconcileWorkloads(ctx, q, extendedResourceName(oldDC), extendedResourceName(newDC))
}

func (h *deviceClassHandler) Delete(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.requeuePendingWorkloadsWithClaimTemplates(ctx, q)
	if !features.Enabled(features.KueueDRAIntegrationExtendedResource) {
		return
	}
	dc := e.Object.(*resourcev1.DeviceClass)
	implicitERN := implicitExtendedResourceName(dc)
	h.r.draBackedResources.Remove(corev1.ResourceName(implicitERN), dc.Name)
	ern := extendedResourceName(dc)
	if ern != "" {
		h.r.draBackedResources.Remove(corev1.ResourceName(ern), dc.Name)
	}
	h.reconcileWorkloads(ctx, q, ern, implicitERN)
}

func (h *deviceClassHandler) Generic(_ context.Context, _ event.GenericEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

// requeuePendingWorkloadsWithClaimTemplates reconciles the pending Workloads that use ResourceClaimTemplates,
// so that handleDRA checks again whether every DeviceClass their templates name exists.
func (h *deviceClassHandler) requeuePendingWorkloadsWithClaimTemplates(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	log := h.r.logger()
	lst := kueue.WorkloadList{}
	if err := h.r.client.List(ctx, &lst, client.MatchingFields{indexer.WorkloadQuotaReservedKey: string(metav1.ConditionFalse)}); err != nil {
		log.Error(err, "Could not list pending workloads with ResourceClaimTemplates")
		return
	}
	for i := range lst.Items {
		w := &lst.Items[i]
		if !workload.HasResourceClaimTemplates(w) {
			continue
		}
		log.V(3).Info("Requeuing workload with ResourceClaimTemplates due to DeviceClass change", "workload", klog.KObj(w))
		q.AddAfter(reconcile.Request{Name: w.Name, Namespace: w.Namespace}, time.Second)
	}
}

func extendedResourceName(dc *resourcev1.DeviceClass) string {
	if dc.Spec.ExtendedResourceName != nil {
		return *dc.Spec.ExtendedResourceName
	}
	return ""
}

// implicitExtendedResourceName returns the name every DeviceClass can be requested
// by, whether or not it sets spec.extendedResourceName.
func implicitExtendedResourceName(dc *resourcev1.DeviceClass) string {
	return resourcev1.ResourceDeviceClassPrefix + dc.Name
}

func (h *deviceClassHandler) reconcileWorkloads(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request], resourceNames ...string) {
	log := h.r.logger()

	// Requeue only workloads that request the affected extended resources and have
	// not reserved quota yet. A reserved Workload keeps the quota key resolved at
	// reservation time: re-admitting it would need the DeviceClass the scheduler
	// actually allocated from, which we don't watch today (#14563).
	for _, name := range resourceNames {
		if name == "" {
			continue
		}
		lst := kueue.WorkloadList{}
		err := h.r.client.List(ctx, &lst,
			client.MatchingFields{
				indexer.WorkloadExtendedResourceKey: name,
				indexer.WorkloadQuotaReservedKey:    string(metav1.ConditionFalse),
			},
		)
		if err != nil {
			log.Error(err, "Could not list workloads for extended resource", "resource", name)
			continue
		}
		for i := range lst.Items {
			w := &lst.Items[i]
			wlLog := log.WithValues("workload", klog.KObj(w), "resource", name)
			wlLog.V(3).Info("Requeuing workload due to DeviceClass change")
			if !h.r.needsDRAReconcile(ctx, w) && workload.IsAdmissible(w) {
				if err := h.r.queues.AddOrUpdateWorkload(ctx, wlLog, w); err != nil {
					// An admissible Workload fails only while its LocalQueue is missing or stopped, or
					// its ClusterQueue is missing. The queue manager queues it once that queue is added,
					// and the Reconcile requested below records the reason in the Workload status.
					wlLog.V(3).Info("Workload was not added to the scheduling queue after DeviceClass change", "error", err)
				}
			}
			q.AddAfter(reconcile.Request{
				Name:      w.Name,
				Namespace: w.Namespace,
			}, time.Second)
		}
	}

	// LimitRange defaults are absent from the raw Workload resource index.
	// Refresh pending workloads in namespaces whose defaults use an affected resource.
	namespaces, err := h.namespacesWithDefaultedResources(ctx, resourceNames...)
	if err != nil {
		log.Error(err, "Could not list LimitRanges for extended resources", "resources", resourceNames)
		return
	}
	resourceHandler := &resourceUpdatesHandler{r: h.r}
	for namespace := range namespaces {
		resourceHandler.queueReconcileForPending(ctx, q, nil, client.InNamespace(namespace))
	}
}

func (h *deviceClassHandler) namespacesWithDefaultedResources(ctx context.Context, resourceNames ...string) (sets.Set[string], error) {
	limitRanges := corev1.LimitRangeList{}
	if err := h.r.client.List(ctx, &limitRanges,
		client.MatchingFields{indexer.LimitRangeHasContainerOrPodType: "true"}); err != nil {
		return nil, err
	}
	namespaces := sets.New[string]()
	for _, lr := range limitRanges.Items {
		for _, limit := range lr.Spec.Limits {
			if limit.Type != corev1.LimitTypeContainer {
				continue
			}
			for _, name := range resourceNames {
				defaultRequest := limit.DefaultRequest[corev1.ResourceName(name)]
				defaultLimit := limit.Default[corev1.ResourceName(name)]
				if !defaultRequest.IsZero() || !defaultLimit.IsZero() {
					namespaces.Insert(lr.Namespace)
				}
			}
		}
	}
	return namespaces, nil
}

func (r *WorkloadReconciler) needsDRAReconcile(ctx context.Context, wl *kueue.Workload) bool {
	wi := &workload.Info{Obj: wl}
	// The effective PodSpecs only matter when a DeviceClass backs an extended
	// resource. This runs on every Workload event, so skip building the Info otherwise.
	if features.Enabled(features.KueueDRAIntegration) && features.Enabled(features.KueueDRAIntegrationExtendedResource) &&
		r.draBackedResources != nil && !r.draBackedResources.IsEmpty() {
		wi = workload.NewInfoFromClient(ctx, r.client, wl, workload.WithSkipSchedulingHash())
	}
	return dra.NeedsDRAReconcile(wi, r.draBackedResources)
}
