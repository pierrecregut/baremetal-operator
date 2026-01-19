/*
Copyright 2025 The Metal3 Authors.

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

package controllers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	hostclaimManager "github.com/metal3-io/baremetal-operator/pkg/hostclaim"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

// HostClaimReconciler reconciles a HostClaim object.
type HostClaimReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Log       logr.Logger
	APIReader client.Reader
}

//+kubebuilder:rbac:groups=metal3.io,resources=hostclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=metal3.io,resources=hostclaims/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=metal3.io,resources=hostclaims/finalizers,verbs=update
//+kubebuilder:rbac:groups=metal3.io,resources=hostdeploypolicies,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

func (r *HostClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, rerr error) {
	log := r.Log.WithValues("hostclaim", req.NamespacedName)
	hostClaim := &metal3api.HostClaim{}
	if err := r.Client.Get(ctx, req.NamespacedName, hostClaim); err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Always patch hostClaim exiting this function so we can persist any hostClaim changes.
	patchHelper, err := patch.NewHelper(hostClaim, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to init patch helper: %w", err)
	}

	defer func() {
		if err = patchHostClaim(ctx, patchHelper, hostClaim); err != nil {
			log.Error(err, "failed to Patch HostClaim")
			rerr = err
		}
	}()

	// Create a helper for managing the baremetal container hosting the machine.
	hostMgr, err := hostclaimManager.NewHostManager(r.Client, log, hostClaim, r.APIReader)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create helper for managing the hostMgr: %w", err)
	}

	if hostMgr.HasAnnotation(metal3api.PausedAnnotation) {
		// set pause annotation on associated bmh (if any)
		err = hostMgr.SetPauseAnnotation(ctx)
		if err != nil {
			log.Info("failed to set pause annotation on associated bmh")
			hostMgr.SetConditionHostToFalse(
				metal3api.AssociatedCondition, metal3api.PauseAnnotationSetFailedReason, err.Error())

			return ctrl.Result{}, nil
		}
		hostMgr.SetConditionHostToFalse(
			metal3api.AssociatedCondition, metal3api.HostPausedReason, "Host is paused")
		// No reason to requeue. A change to pause annotation will trigger a reconcile.
		return ctrl.Result{}, nil
	}
	err = hostMgr.RemovePauseAnnotation(ctx)
	if err != nil {
		log.Info("failed to check pause annotation on associated bmh")
		hostMgr.SetConditionHostToFalse(
			metal3api.AssociatedCondition, metal3api.PauseAnnotationRemoveFailedReason,
			"Removal of pause annotation failed: "+err.Error())
		return ctrl.Result{}, nil
	}

	// Handle deleted machines
	if !hostClaim.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, hostMgr)
	}

	// Handle non-deleted machines
	return r.reconcileNormal(ctx, hostMgr)
}

func (r *HostClaimReconciler) reconcileNormal(ctx context.Context,
	hostMgr hostclaimManager.HostManagerInterface,
) (ctrl.Result, error) {
	// If the Host doesn't have finalizer, add it.
	hostMgr.SetFinalizer()

	// if the machine is already provisioned, update and return
	if hostMgr.IsProvisioned() {
		return checkHostClaimError(hostMgr.Update(ctx), "Failed to update the Host")
	}

	// Check if the hostclaim was associated with a baremetalhost
	if !hostMgr.IsAssociated() {
		// Associate the baremetalhost hosting the machine
		// We do not update: there is an event on the BMH consumer ref update.
		// we do not want more reconcile than necessary.
		err := hostMgr.Associate(ctx)
		if err != nil {
			return checkHostClaimError(err, "failed to associate the Host to a BaremetalHost")
		}
	} else {
		// Update Condition to reflect that we have an associated BMH
		hostMgr.SetConditionHostToTrue(metal3api.AssociatedCondition, metal3api.BareMetalHostAssociatedReason)
		err := hostMgr.Update(ctx)
		if err != nil {
			return checkHostClaimError(err, "failed to update BaremetalHost")
		}
	}
	return ctrl.Result{}, nil
}

func (r *HostClaimReconciler) reconcileDelete(ctx context.Context,
	hostMgr hostclaimManager.HostManagerInterface,
) (ctrl.Result, error) {
	// set machine condition to Deleting
	hostMgr.SetConditionHostToFalse(metal3api.AssociatedCondition,
		metal3api.HostClaimDeletingReason,
		"")

	// delete the machine
	if err := hostMgr.Delete(ctx); err != nil {
		hostMgr.SetConditionHostToFalse(metal3api.AssociatedCondition,
			metal3api.HostClaimDeletionFailedReason,
			err.Error())
		return checkHostClaimError(err, "failed to delete Host")
	}

	// hostclaim is marked for deletion and ready to be deleted,
	// so remove the finalizer.
	hostMgr.UnsetFinalizer()

	return ctrl.Result{}, nil
}

// patchHostClaim patch the HostClaim and ensures that the conditions are initialized.
// Can be used several times without creating a conflict.
func patchHostClaim(ctx context.Context, patchHelper *patch.Helper, claim *metal3api.HostClaim, options ...patch.Option) error {
	// Always update the readyCondition by summarizing the state of other conditions.
	sumOption := conditions.ForConditionTypes{
		metal3api.AssociatedCondition, metal3api.SynchronizedCondition, metal3api.ProvisionedCondition}
	if err := conditions.SetSummaryCondition(claim, claim, clusterv1.ReadyCondition, sumOption); err != nil {
		return err
	}

	// Patch the object, ignoring conflicts on the conditions owned by this controller.
	options = append(
		options,
		patch.WithOwnedConditions{Conditions: []string{
			clusterv1.ReadyCondition,
			metal3api.AssociatedCondition,
			metal3api.SynchronizedCondition,
			metal3api.ProvisionedCondition,
			metal3api.AvailableForProvisioningCondition,
		}},
		patch.WithStatusObservedGeneration{},
	)
	return patchHelper.Patch(ctx, claim, options...)
}

// SetupWithManager sets up the controller with the Manager.
func (r *HostClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// This is still applied on client side not server side.
	return ctrl.NewControllerManagedBy(mgr).
		For(&metal3api.HostClaim{}).
		Watches(
			&metal3api.BareMetalHost{},
			handler.EnqueueRequestsFromMapFunc(BareMetalHostToHostClaims),
		).
		Complete(r)
}

// BareMetalHostToHostClaims will return a reconcile request for a HostClaim if the event is for a
// BareMetalHost and that BareMetalHost references a HostClaim.
func BareMetalHostToHostClaims(_ context.Context, obj client.Object) []ctrl.Request {
	if host, ok := obj.(*metal3api.BareMetalHost); ok {
		if host.Spec.ConsumerRef != nil &&
			host.Spec.ConsumerRef.Kind == hostclaimManager.HostClaimKind &&
			host.Spec.ConsumerRef.APIVersion == metal3api.GroupVersion.Identifier() {
			return []ctrl.Request{
				{
					NamespacedName: types.NamespacedName{
						Name:      host.Spec.ConsumerRef.Name,
						Namespace: host.Spec.ConsumerRef.Namespace,
					},
				},
			}
		}
	}
	return []ctrl.Request{}
}

func checkHostClaimError(err error, errMessage string) (ctrl.Result, error) {
	if err == nil {
		return ctrl.Result{}, nil
	}
	if ok, delay := hostclaimManager.IsRequeueAfterError(err); ok {
		return ctrl.Result{Requeue: true, RequeueAfter: delay}, nil
	}
	return ctrl.Result{}, fmt.Errorf("%s: %w", errMessage, err)
}
