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

package hostclaim

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"math/big"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	"github.com/metal3-io/baremetal-operator/pkg/secretutils"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/cache"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type HostManagerInterface interface {
	SetFinalizer()
	UnsetFinalizer()
	IsProvisioned() bool
	SetPauseAnnotation(context.Context) error
	RemovePauseAnnotation(context.Context) error
	HasAnnotation(string) bool
	SetConditionHostToFalse(string, string, string)
	SetConditionHostToTrue(string, string)
	Associate(context.Context) error
	Delete(context.Context) error
	Update(context.Context) error
	PatchHost(context.Context, ...patch.Option) error
}

type HostManager struct {
	client      client.Client
	HostClaim   *metal3api.HostClaim
	Log         logr.Logger
	PatchHelper *patch.Helper
	APIReader   client.Reader
}

const (
	// PausedAnnotationKey is an annotation to be used for pausing a hostclaim.
	PausedAnnotationKey = "metal3.io/hostmgr"
	// BareMetalHostAnnotation is the key for an annotation that should go on a Host to
	// reference what BareMetalHost it corresponds to.
	BareMetalHostAnnotation = "metal3.io/BareMetalHost"
	// UnhealthyAnnotation is the annotation used by the Metal3Health
	// that sets unhealthy status of BMH.
	UnhealthyAnnotation = "capi.metal3.io/unhealthy"
	// nodeReuseLabelName is the label set on BMH when node reuse feature is enabled.
	// and the label set on HostClaim as target for reuse.
	nodeReuseLabelName = "infrastructure.cluster.x-k8s.io/node-reuse"
	// rebootDomain is the domain of metal3 reboot annotations.
	rebootDomain = "reboot.metal3.io"
	// Requeueing after 0 is in fact not requeuing.
	TerminalReueueDelay time.Duration = 0
	// Standard delay when waiting for other to settle.
	HostClaimRequeueDelay = time.Second * 30
	// Small delay on conflict error.
	ConflictRequeueDelay = time.Millisecond * 100
	// FailureDomainLabelName is a label name for FailureDomains.
	FailureDomainLabelName = "infrastructure.cluster.x-k8s.io/failure-domain"
	// HostClaimKind is the name of the kind
	HostClaimKind = "HostClaim"
)

var (
	// Capm3FastTrack is the variable fetched from the CAPM3_FAST_TRACK environment variable.
	Capm3FastTrack = os.Getenv("CAPM3_FAST_TRACK")
	// An error raised when BareMetalHost does not exists (only because go vet does not like returning nil, nil).
	ErrNotFound       = errors.New("NotFound")
	associateBMHMutex sync.Mutex
)

func NewHostManager(client client.Client, log logr.Logger, host *metal3api.HostClaim, apireader client.Reader) (*HostManager, error) {
	patchHelper, err := patch.NewHelper(host, client)
	if err != nil {
		return nil, err
	}
	return &HostManager{
		client:      client,
		HostClaim:   host,
		Log:         log,
		PatchHelper: patchHelper,
		APIReader:   apireader,
	}, nil
}

// SetFinalizer sets finalizer on the host.
func (m *HostManager) SetFinalizer() {
	if !controllerutil.ContainsFinalizer(m.HostClaim, metal3api.HostClaimFinalizer) {
		controllerutil.AddFinalizer(m.HostClaim, metal3api.HostClaimFinalizer)
	}
}

// UnsetFinalizer unsets finalizer on the host.
func (m *HostManager) UnsetFinalizer() {
	controllerutil.RemoveFinalizer(m.HostClaim, metal3api.HostClaimFinalizer)
}

// IsProvisioned checks if the baremetalhost associated to the hostclaim is provisioned.
// This is visible is the Ready field of the status.
func (m *HostManager) IsProvisioned() bool {
	return conditions.IsTrue(m.HostClaim, metal3api.ProvisionedCondition)
}

// SetPauseAnnotation propagates the pause annotation from the hostclaim to the BareMetalHost.
func (m *HostManager) SetPauseAnnotation(ctx context.Context) error {
	// look for associated BMH
	host, helper, err := m.getBmh(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if host == nil {
		return nil
	}
	annotations := host.GetAnnotations()

	if annotations != nil {
		if _, ok := annotations[metal3api.PausedAnnotation]; ok {
			m.Log.Info("BaremetalHost is already paused")
			return nil
		}
	} else {
		host.Annotations = make(map[string]string)
	}
	m.Log.Info("Adding PausedAnnotation in BareMetalHost")
	host.Annotations[metal3api.PausedAnnotation] = PausedAnnotationKey

	// Setting annotation with BMH status.
	newAnnotation, err := json.Marshal(&host.Status)
	if err != nil {
		return errors.Wrap(err, "failed to marshall status annotation")
	}
	obj := map[string]interface{}{}
	if err = json.Unmarshal(newAnnotation, &obj); err != nil {
		return errors.Wrap(err, "failed to unmarshall status annotation")
	}
	delete(obj, "hardware")
	newAnnotation, err = json.Marshal(obj)
	if err != nil {
		return err
	}
	host.Annotations[metal3api.StatusAnnotation] = string(newAnnotation)
	return helper.Patch(ctx, host)
}

// RemovePauseAnnotation removes the pause annotation on the BareMetalHost.
func (m *HostManager) RemovePauseAnnotation(ctx context.Context) error {
	// look for associated BMH
	bmh, helper, err := m.getBmh(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return errors.Wrap(err, "Cannot access BareMetalHost")
	}

	if bmh == nil {
		return nil
	}

	annotations := bmh.GetAnnotations()

	if annotations != nil {
		if _, ok := annotations[metal3api.PausedAnnotation]; ok {
			if annotations[metal3api.PausedAnnotation] == PausedAnnotationKey {
				// Removing BMH Paused Annotation Since Owner Cluster is not paused.
				delete(bmh.Annotations, metal3api.PausedAnnotation)
			} else {
				m.Log.Info("BMH is paused by user. Not removing Pause Annotation")
				return nil
			}
		}
	}
	return helper.Patch(ctx, bmh)
}

// HasAnnotation makes sure the host has an annotation that references a host.
func (m *HostManager) HasAnnotation(annotation string) bool {
	annotations := m.HostClaim.ObjectMeta.GetAnnotations()
	if annotations == nil {
		return false
	}
	_, ok := annotations[annotation]
	return ok
}

// SetConditionHostToFalse sets Host condition status to False.
func (m *HostManager) SetConditionHostToFalse(
	t string,
	reason string,
	message string,
) {
	conditions.Set(m.HostClaim, metav1.Condition{Type: t, Status: metav1.ConditionFalse, Reason: reason, Message: message})
}

// SetConditionHostToFalse sets Host condition status to False.
func (m *HostManager) SetConditionHostToTrue(
	t string,
	reason string,
) {
	conditions.Set(m.HostClaim, metav1.Condition{Type: t, Status: metav1.ConditionTrue, Reason: reason, Message: ""})
}

func (m *HostManager) Associate(ctx context.Context) error {
	// Parallel attempts to associate is problematic since the same BMH
	// could be selected for multiple M3Ms. Therefore we use a mutex lock here.
	associateBMHMutex.Lock()
	defer associateBMHMutex.Unlock()

	m.Log.Info("Associating host")

	// load and validate the config
	if m.HostClaim == nil {
		// Should have been picked earlier. Do not requeue
		m.Log.Info("No hostclaim in Associate")
		return nil
	}

	bmh, helper, err := m.chooseBMH(ctx)
	if err != nil {
		if ok, _ := IsRequeueAfterError(err); !ok {
			m.SetConditionHostToFalse(
				metal3api.AssociatedCondition, metal3api.NoBareMetalHostReason,
				"Failed to pick a BaremetalHost for the Host")
		}
		return err
	}
	if bmh == nil {
		m.Log.Info("No available host found. Requeuing.")
		m.SetConditionHostToFalse(
			metal3api.AssociatedCondition, metal3api.NoBareMetalHostReason,
			"No available host found: requeuing.")
		return &RequeueAfterError{RequeueAfter: HostClaimRequeueDelay}
	}
	m.Log.Info("Associating machine with host", "BareMetalHost", bmh.Name)

	// First we record the association in the BMH. If we fail, we must redo the
	// whole selection process and remove the annotation.
	m.setBmhConsumerRef(bmh)

	if err = helper.Patch(ctx, bmh); err != nil {
		m.Log.Error(err, "Error while patching the consumerRef on BMH")
		delete(m.HostClaim.Annotations, BareMetalHostAnnotation)
		m.SetConditionHostToFalse(
			metal3api.AssociatedCondition, metal3api.BareMetalHostNotSynchronizedReason,
			"Failed to set consumer Reference on BareMetalHost")
		return hideConflictError(err)
	}

	// Then we record the commitment to this given BMH.
	err = m.ensureAnnotation(ctx, bmh)
	if err != nil {
		m.SetConditionHostToFalse(
			metal3api.AssociatedCondition, metal3api.HostClaimAnnotationNotSetReason,
			"Failed to annotate the hostclaim")
		return err
	}

	// From here the hostClaim is definitely associated
	m.SetConditionHostToTrue(metal3api.AssociatedCondition, metal3api.BareMetalHostAssociatedReason)

	return nil
}

// Update updates a machine and is invoked by the Machine Controller.
func (m *HostManager) Update(ctx context.Context) error {
	m.Log.Info("Updating machine")

	// clear any error message that was previously set. This method doesn't set
	// error messages yet, so we know that it's incorrect to have one here.

	bmh, helper, err := m.getBmh(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		m.SetConditionHostToFalse(
			metal3api.AssociatedCondition, metal3api.MissingBareMetalHostReason,
			"Failed to get a BareMetalHost for the Host")
		return err
	}
	if bmh == nil {
		m.SetConditionHostToFalse(
			metal3api.AssociatedCondition, metal3api.MissingBareMetalHostReason,
			"BareMetalHost associated to the claim not found")
		return errors.Errorf("BareMetalHost not found")
	}

	// ensure that the BMH specs are correctly set.
	err = m.setBmhSpec(ctx, bmh)
	if err != nil {
		return err
	}

	if bmh.Annotations == nil {
		bmh.Annotations = map[string]string{}
	}

	syncReboot(m.HostClaim.Annotations, bmh.Annotations)

	err = helper.Patch(ctx, bmh)
	if err != nil {
		m.SetConditionHostToFalse(
			metal3api.SynchronizedCondition, metal3api.BareMetalHostNotSynchronizedReason,
			"Failed to update BareMetalHost")
		m.Log.Error(err, "Error while patching the BareMetalHost")
		return hideConflictError(err)
	}

	// transient rebootAnnotation was successfully transmitted. We can delete it on HostClaim
	delete(m.HostClaim.Annotations, rebootDomain)

	m.updateHostClaimStatus(bmh)

	m.Log.Info("Finished updating machine")
	return nil
}

// Delete deletes a Hostclaim and is invoked by the Machine Controller.
func (m *HostManager) Delete(ctx context.Context) error {
	m.Log.Info("Deleting host machine", "host", m.HostClaim.Name)

	// clear an error if one was previously set.

	if Capm3FastTrack == "" {
		Capm3FastTrack = "false"
		m.Log.Info("Capm3FastTrack is not set, setting it to default value false")
	}

	bmh, helper, err := m.getBmh(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if bmh == nil {
		m.Log.Info("baremetalhost not found for host", "host", m.HostClaim.Name)
		return nil
	}

	if bmh.Spec.ConsumerRef != nil {
		// ConsumerRef must match because of getBmh
		bmhUpdated := false

		if bmh.Spec.Image != nil {
			bmh.Spec.Image = nil
			bmhUpdated = true
		}
		if m.HostClaim.Spec.UserData != nil && bmh.Spec.UserData != nil {
			if err := m.clearSecret(ctx, bmh.Spec.UserData.Name, bmh.Namespace); err != nil {
				return err
			}
			bmh.Spec.UserData = nil
			bmhUpdated = true
		}
		if m.HostClaim.Spec.MetaData != nil && bmh.Spec.MetaData != nil {
			if err := m.clearSecret(ctx, bmh.Spec.MetaData.Name, bmh.Namespace); err != nil {
				return err
			}
			bmh.Spec.MetaData = nil
			bmhUpdated = true
		}
		if m.HostClaim.Spec.NetworkData != nil && bmh.Spec.NetworkData != nil {
			if err := m.clearSecret(ctx, bmh.Spec.NetworkData.Name, bmh.Namespace); err != nil {
				return err
			}
			bmh.Spec.NetworkData = nil
			bmhUpdated = true
		}

		onlineStatus := bmh.Spec.Online

		// We force Cleaning when the hostClaim is deleted.
		if bmh.Spec.AutomatedCleaningMode != metal3api.CleaningModeMetadata {
			bmhUpdated = true
			bmh.Spec.AutomatedCleaningMode = metal3api.CleaningModeMetadata
		}

		switch Capm3FastTrack {
		case "true":
			bmh.Spec.Online = true
		case "false":
			bmh.Spec.Online = false
		}

		if onlineStatus != bmh.Spec.Online {
			bmhUpdated = true
		}

		if bmhUpdated {
			// Update the BMH object, if the errors are NotFound, do not return the
			// errors.
			if err := patchIfFound(ctx, helper, bmh); err != nil {
				return err
			}

			m.Log.Info("Deprovisioning BaremetalHost, requeuing")
			return &RequeueAfterError{RequeueAfter: ConflictRequeueDelay}
		}

		waiting := true
		switch bmh.Status.Provisioning.State {
		case metal3api.StateRegistering,
			metal3api.StateMatchProfile, metal3api.StateInspecting,
			metal3api.StateReady, metal3api.StateAvailable, metal3api.StateNone,
			metal3api.StateUnmanaged:
			// Host is not provisioned.
			waiting = false
		case metal3api.StateExternallyProvisioned:
			// We have no control over provisioning, so just wait until the
			// host is powered off.
			waiting = bmh.Status.PoweredOn
		default:
		}
		if waiting {
			m.Log.Info("Deprovisioning BaremetalHost, requeuing until available")
			return &RequeueAfterError{RequeueAfter: HostClaimRequeueDelay}
		}

		m.Log.Info("Removing Paused Annotation (if any)")
		if bmh.Annotations != nil && bmh.Annotations[metal3api.PausedAnnotation] == PausedAnnotationKey {
			delete(bmh.Annotations, metal3api.PausedAnnotation)
		}

		// ConsumerRef removed last. Made atomic with next step.
		bmh.Spec.ConsumerRef = nil

		// Update the BMH object, if the errors are NotFound, do not return the
		// errors.
		if err := patchIfFound(ctx, helper, bmh); err != nil {
			return err
		}
	}

	m.Log.Info("finished deleting HostClaim")
	return nil
}

// PatchHost patch the HostClaim and ensures that the conditions are initialized.
// Can be used several times without creating a conflict.
func (m *HostManager) PatchHost(ctx context.Context, options ...patch.Option) error {
	if m.PatchHelper == nil {
		m.Log.Info("Patch helper was removed")
		return nil
	}
	// Always update the readyCondition by summarizing the state of other conditions.
	sumOption := conditions.ForConditionTypes{
		metal3api.AssociatedCondition, metal3api.SynchronizedCondition, metal3api.ProvisionedCondition}
	if err := conditions.SetSummaryCondition(m.HostClaim, m.HostClaim, clusterv1.ReadyCondition, sumOption); err != nil {
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
			metal3api.AvailableCondition,
		}},
		patch.WithStatusObservedGeneration{},
	)
	err := m.PatchHelper.Patch(ctx, m.HostClaim, options...)
	if err != nil {
		// Deactivate pathHelper so that it cannot be reused
		m.PatchHelper = nil
	}
	return err
}

// getBmh gets the associated BareMetalHost by looking for an annotation on the machine
// that contains a reference to the host. Returns nil if not found. Assumes the
// host is in the same namespace as the machine.
func (m *HostManager) getBmh(ctx context.Context) (*metal3api.BareMetalHost, *patch.Helper, error) {
	host, err := getBmh(ctx, m.HostClaim, m.client, m.Log)
	if err != nil || host == nil {
		return host, nil, err
	}
	helper, err := patch.NewHelper(host, m.client)
	return host, helper, err
}

func getBmh(ctx context.Context, hostClaim *metal3api.HostClaim, cl client.Client,
	mLog logr.Logger,
) (*metal3api.BareMetalHost, error) {
	annotations := hostClaim.ObjectMeta.GetAnnotations()
	if annotations == nil {
		return nil, ErrNotFound
	}
	bmhKey, ok := annotations[BareMetalHostAnnotation]
	if !ok {
		return nil, ErrNotFound
	}
	bmhNamespace, bmhName, err := cache.SplitMetaNamespaceKey(bmhKey)
	if err != nil {
		mLog.Error(err, "Error parsing annotation value", "annotation key", bmhKey)
		return nil, err
	}

	bmh := metal3api.BareMetalHost{}
	key := types.NamespacedName{
		Name:      bmhName,
		Namespace: bmhNamespace,
	}
	err = cl.Get(ctx, key, &bmh)
	if k8serrors.IsNotFound(err) {
		mLog.Info("Annotated host not found", "host", bmhKey)
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	// This is an important security check. We do not trust the annotation. Only the ConsumerRef
	if !consumerRefMatches(bmh.Spec.ConsumerRef, hostClaim) {
		mLog.Info("The consumer ref does not point to the hostClaim", "consumerRef", bmh.Spec.ConsumerRef)
		delete(annotations, BareMetalHostAnnotation)
		return nil, ErrNotFound
	}
	return &bmh, nil
}

func (m *HostManager) setBmhConsumerRef(bmh *metal3api.BareMetalHost) {
	bmh.Spec.ConsumerRef = &corev1.ObjectReference{
		Kind:       HostClaimKind,
		Name:       m.HostClaim.Name,
		Namespace:  m.HostClaim.Namespace,
		APIVersion: metal3api.GroupVersion.Identifier(),
	}
}

func hideConflictError(err error) error {
	var aggr kerrors.Aggregate
	if ok := errors.As(err, &aggr); ok {
		for _, kerr := range aggr.Errors() {
			if k8serrors.IsConflict(kerr) {
				return &RequeueAfterError{RequeueAfter: ConflictRequeueDelay}
			}
		}
	}
	return err
}

// ensureAnnotation makes sure the machine has an annotation that references the
// host and uses the API to update the machine if necessary.
func (m *HostManager) ensureAnnotation(ctx context.Context, bmh *metal3api.BareMetalHost) error {
	annotations := m.HostClaim.Annotations
	if annotations == nil {
		annotations = map[string]string{}
		m.HostClaim.Annotations = annotations
	}
	bmhKey := cache.MetaObjectToName(bmh).String()
	annotations[BareMetalHostAnnotation] = bmhKey
	return m.PatchHost(ctx)
}

// setBmhSpec will ensure the host's Spec is set according to the machine's
// details. It will then update the host via the kube API. If UserData does not
// include a Namespace, it will default to the Host's namespace.
func (m *HostManager) setBmhSpec(ctx context.Context, bmh *metal3api.BareMetalHost) error {
	secretManager := secretutils.NewSecretManager(ctx, m.Log, m.client, m.APIReader)
	ref, err := m.synchronizeDataSecret(ctx, secretManager, bmh, "userdata", m.HostClaim.Spec.UserData, m.HostClaim.Namespace, m.HostClaim.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		m.SetConditionHostToFalse(
			metal3api.SynchronizedCondition, metal3api.BadUserDataSecretReason, err.Error(),
		)
		return err
	}
	bmh.Spec.UserData = ref
	ref, err = m.synchronizeDataSecret(ctx, secretManager, bmh, "metadata", m.HostClaim.Spec.MetaData, m.HostClaim.Namespace, m.HostClaim.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		m.SetConditionHostToFalse(
			metal3api.SynchronizedCondition, metal3api.BadMetaDataSecretReason, err.Error(),
		)
		return err
	}
	bmh.Spec.MetaData = ref
	ref, err = m.synchronizeDataSecret(ctx, secretManager, bmh, "networkdata", m.HostClaim.Spec.NetworkData, m.HostClaim.Namespace, m.HostClaim.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		m.SetConditionHostToFalse(
			metal3api.SynchronizedCondition, metal3api.BadNetworkDataSecretReason, err.Error(),
		)
		return err
	}
	bmh.Spec.NetworkData = ref
	// A host with an existing image is already provisioned and
	// upgrades are not supported at this time. To re-provision a
	// host, we must fully deprovision it and then provision it again.
	if bmh.Spec.Image == nil && m.HostClaim.Spec.Image != nil {
		bmh.Spec.Image = m.HostClaim.Spec.Image.DeepCopy()
	} else if m.HostClaim.Spec.Image == nil {
		bmh.Spec.Image = nil
	}

	// Propagate custom deploy.
	if m.HostClaim.Spec.CustomDeploy == nil {
		bmh.Spec.CustomDeploy = nil
	} else {
		bmh.Spec.CustomDeploy = &metal3api.CustomDeploy{Method: m.HostClaim.Spec.CustomDeploy.Method}
	}

	// Set automatedCleaningMode to disabled as long as the hostclaim exists
	bmh.Spec.AutomatedCleaningMode = metal3api.CleaningModeDisabled

	bmh.Spec.Online = m.HostClaim.Spec.PoweredOn
	m.SetConditionHostToTrue(metal3api.SynchronizedCondition, metal3api.ConfigurationSyncedReason)
	return nil
}

func (m *HostManager) synchronizeDataSecret(
	ctx context.Context,
	secretManager secretutils.SecretManager,
	bmh *metal3api.BareMetalHost,
	typ string,
	sourceRef *corev1.SecretReference,
	namespace string,
	hostName string,
) (*corev1.SecretReference, error) {
	log := m.Log.WithValues("hostName", hostName, "hostNamespace", namespace, "secret-type", typ)
	log.V(1).Info("Handling secret copy between claim and bmh")
	secretName := bmh.Name + "-" + typ
	if sourceRef == nil {
		key := client.ObjectKey{Name: secretName, Namespace: bmh.Namespace}
		targetSecret := corev1.Secret{}
		err := m.client.Get(ctx, key, &targetSecret)
		if err != nil {
			if !k8serrors.IsNotFound(err) {
				return nil, err
			}
			return nil, ErrNotFound
		}
		log.V(1).Info("no configuration secret in hostclaim: destroying in bmh")
		err = m.client.Delete(ctx, &targetSecret)
		return nil, err
	}
	// For the same reason as bmh, we ignore namespace value in the ref.
	sourceKey := client.ObjectKey{Name: sourceRef.Name, Namespace: namespace}
	sourceSecret, err := secretManager.AcquireSecret(sourceKey, m.HostClaim, false)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Error(err, "Missing source Secret for synchronization from claim to BMH", "source", sourceKey)
			return nil, &RequeueAfterError{RequeueAfter: HostClaimRequeueDelay}
		}
		log.Error(err, "Cannot get source Secret for synchronization from claim to BMH", "source", sourceKey)
		return nil, err
	}
	log.V(1).Info("Updating bmh secret with hostclaim secret content")
	targetSecret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: bmh.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, m.client, &targetSecret,
		func() error {
			if targetSecret.Labels == nil {
				targetSecret.Labels = map[string]string{}
			}
			targetSecret.Labels[secretutils.LabelEnvironmentName] = secretutils.LabelEnvironmentValue
			if err = controllerutil.SetOwnerReference(bmh, &targetSecret, m.client.Scheme()); err != nil {
				return err
			}
			targetSecret.Data = sourceSecret.DeepCopy().Data
			return nil
		},
	)
	if err != nil {
		log.Error(err, "cannot copy/update secret", "source", sourceKey, "target", secretName)
		return nil, err
	}
	return &corev1.SecretReference{Name: secretName, Namespace: bmh.Namespace}, nil
}

// consumerRefMatches returns a boolean based on whether the consumer
// reference and bare metal machine metadata match.
func consumerRefMatches(consumer *corev1.ObjectReference, host *metal3api.HostClaim) bool {
	if consumer == nil || host == nil {
		return false
	}
	if consumer.Name != host.Name {
		return false
	}
	if consumer.Namespace != host.Namespace {
		return false
	}
	if consumer.Kind != HostClaimKind {
		return false
	}
	if consumer.GroupVersionKind().Group != metal3api.GroupVersion.Group {
		return false
	}
	return true
}

func (m *HostManager) selectBMH(
	availableHosts []*metal3api.BareMetalHost,
) (*metal3api.BareMetalHost, error) {
	// choose a host.
	var err error
	var chosenHost *metal3api.BareMetalHost

	// If there are hosts with nodeReuseLabelName:
	chosenHost, err = m.pickHost(availableHosts)
	if err != nil {
		m.Log.Error(err, "Failed to choose host, not choosing host")
		return nil, err
	}

	return chosenHost, nil
}

// Picks host from list of available hosts, if failureDomain is set, tries to choose from hosts in failureDomain.
// When none available in failureDomain it chooses from all available hosts.
func (m *HostManager) pickHost(availableHosts []*metal3api.BareMetalHost) (*metal3api.BareMetalHost, error) {
	var chosenHost *metal3api.BareMetalHost
	var availableHostsInFailureDomain []*metal3api.BareMetalHost

	// When failureDomain is set, create a list from available hosts in failureDomain
	if m.HostClaim.Spec.FailureDomain != "" {
		labelSelector := labels.NewSelector()
		var reqs labels.Requirements
		var r *labels.Requirement
		r, err := labels.NewRequirement(FailureDomainLabelName, selection.Equals, []string{m.HostClaim.Spec.FailureDomain})

		if err != nil {
			m.Log.Error(err, "Failed to create FailureDomain MatchLabel requirement, not choosing host")
			return nil, err
		}
		reqs = append(reqs, *r)
		labelSelector = labelSelector.Add(reqs...)

		for _, host := range availableHosts {
			if labelSelector.Matches(labels.Set(host.ObjectMeta.Labels)) {
				availableHostsInFailureDomain = append(availableHostsInFailureDomain, host)
			}
		}
		if len(availableHostsInFailureDomain) == 0 {
			m.Log.Info("No available hosts in FailureDomain", m.HostClaim.Spec.FailureDomain, "choosing from other available hosts")
		}
	}

	if len(availableHostsInFailureDomain) > 0 {
		rHost, _ := rand.Int(rand.Reader, big.NewInt(int64(len(availableHostsInFailureDomain))))
		randomHost := rHost.Int64()
		chosenHost = availableHostsInFailureDomain[randomHost]
	} else {
		rHost, _ := rand.Int(rand.Reader, big.NewInt(int64(len(availableHosts))))
		randomHost := rHost.Int64()
		chosenHost = availableHosts[randomHost]
	}

	return chosenHost, nil
}

// For a given HostClaim in a given namespace, computes all the namespaces that can contain BMH
// that can be bound to the HostClaim.
//
// The function lists the HostDeployPolicy and keeps the namespaces of the ones that
// match the namespace of the claim. overridesCleaningPolicy specifies whether the hostclaim
// defines the automatedCleaningPolicy field or not.
func (m *HostManager) acceptableNamespaces(ctx context.Context, namespace string) (Set[string], error) {
	m.Log.Info("Searching for suitable namespaces")
	hostdeploypolicies := metal3api.HostDeployPolicyList{}
	options := []client.ListOption{}
	if namespace != "" {
		options = append(options, client.InNamespace(namespace))
	}
	err := m.client.List(ctx, &hostdeploypolicies, options...)
	if err != nil {
		m.Log.Error(err, "cannot list the HostDeployPolicies")
		return nil, err
	}
	hostNs := m.HostClaim.Namespace
	hostNsResource := &corev1.Namespace{}
	err = m.client.Get(ctx, client.ObjectKey{Name: hostNs}, hostNsResource)
	if err != nil {
		m.Log.Error(err, "cannot access the namespace of the claim")
		return nil, err
	}
	nsLabels := hostNsResource.Labels
	if nsLabels == nil {
		nsLabels = map[string]string{}
	}
	m.Log.Info("labels of the NS of hostclaim", "labels", nsLabels)
	namespaces := NewSet[string]()
LOOP_POLICY:
	for _, hostDeployPolicy := range hostdeploypolicies.Items {
		log := m.Log.WithValues("policyNamespace", hostDeployPolicy.Namespace, "policyName", hostDeployPolicy.Name)
		if SetContains(namespaces, hostDeployPolicy.Namespace) {
			// namespace already added, no reason to check this hdp.
			continue
		}
		constraints := hostDeployPolicy.Spec.HostClaimNamespaces
		if constraints == nil {
			log.Info("Rejecting hostdeploypolicy without constraint")
			continue
		}
		if constraints.Names != nil && !slices.Contains(constraints.Names, hostNs) {
			log.Info("Rejecting hostdeploypolicy because claim namespace not in names", "names", constraints.Names)
			continue
		}
		if constraints.NameMatches != "" {
			b, err := regexp.MatchString(constraints.NameMatches, hostNs)
			if err != nil {
				log.Error(
					err, "Error during regexp matching on hostclaim namespace (bad regexp)",
					"regexp", constraints.NameMatches)
				return nil, err
			}
			if !b {
				log.Info("Rejecting hostdeploypolicy because claim namespace does not match regex")
				continue
			}
		}
		if constraints.HasLabels != nil {
			// Behaves as a 'forall' on labels
			for _, pair := range constraints.HasLabels {
				m.Log.Info("checking label", "name", pair.Name, "value", pair.Value)
				if v, ok := nsLabels[pair.Name]; ok {
					if pair.Value != "" && v != pair.Value {
						log.Info("Rejecting hostdeploypolicy because claim namespace labels does not have correct value", "label", pair.Name)
						continue LOOP_POLICY
					}
				} else {
					log.Info("Rejecting hostdeploypolicy because claim namespace labels does not have a required label", "label", pair.Name)
					continue LOOP_POLICY
				}
			}
		}
		log.Info("Accepting namespace because of HostDeployPolicy")
		AddSet(namespaces, hostDeployPolicy.Namespace)
	}
	m.Log.Info("Acceptable namespaces", "namespaces", namespaces)
	return namespaces, nil
}

func hostLabelSelectorForHostClaim(hostClaim *metal3api.HostClaim, log logr.Logger) (labels.Selector, error) {
	labelSelector := labels.NewSelector()

	for labelKey, labelVal := range hostClaim.Spec.HostSelector.MatchLabels {
		log.Info("Adding requirement to match label",
			"label key", labelKey,
			"label value", labelVal)
		r, err := labels.NewRequirement(labelKey, selection.Equals, []string{labelVal})
		if err != nil {
			log.Error(err, "Failed to create MatchLabel requirement, not choosing host")
			return nil, err
		}
		labelSelector = labelSelector.Add(*r)
	}

	for _, req := range hostClaim.Spec.HostSelector.MatchExpressions {
		log.Info("Adding requirement to match label",
			"label key", req.Key,
			"label operator", req.Operator,
			"label value", req.Values)
		lowercaseOperator := selection.Operator(strings.ToLower(string(req.Operator)))
		r, err := labels.NewRequirement(req.Key, lowercaseOperator, req.Values)
		if err != nil {
			log.Error(err, "Failed to create MatchExpression requirement, not choosing host")
			return nil, err
		}
		labelSelector = labelSelector.Add(*r)
	}
	return labelSelector, nil
}

// chooseBMH iterates through known bare-metal hosts and returns one that can be
// associated with the HostClaim. It searches all hosts in case one already has an
// association with this HostClaim.
func (m *HostManager) chooseBMH(ctx context.Context) (*metal3api.BareMetalHost, *patch.Helper, error) {
	namespaces, err := m.acceptableNamespaces(ctx, m.HostClaim.Spec.HostSelector.InNamespace)
	if err != nil || len(namespaces) == 0 {
		return nil, nil, err
	}
	// get list of BMH.
	labelSelector, err := hostLabelSelectorForHostClaim(m.HostClaim, m.Log)
	if err != nil {
		return nil, nil, err
	}

	// Different from M3M: We do not restrict to a namespace

	availableHosts := []*metal3api.BareMetalHost{}

	for namespace := range namespaces {
		bmhs := metal3api.BareMetalHostList{}
		m.Log.Info("Testing in namespace", "namespace", namespace)
		err = m.client.List(ctx, &bmhs, client.MatchingLabelsSelector{Selector: labelSelector}, client.InNamespace(namespace))
		if err != nil {
			return nil, nil, err
		}
		for i, bmh := range bmhs.Items {
			m.Log.Info("Testing host", "name", bmh.Name, "namespace", bmh.Namespace)
			if bmh.Spec.ConsumerRef != nil && consumerRefMatches(bmh.Spec.ConsumerRef, m.HostClaim) {
				m.Log.Info("Found host with existing ConsumerRef", "host", bmh.Name)
				helper, err2 := patch.NewHelper(&bmhs.Items[i], m.client)
				return &bmhs.Items[i], helper, err2
			}

			// nodeReuse is not handled at the level of HostClaims but we still
			// honor nodeReuse labels of others.
			if bmh.Spec.ConsumerRef != nil || nodeReuseLabelExists(&bmh) {
				continue
			}
			if bmh.GetDeletionTimestamp() != nil {
				continue
			}
			if bmh.Status.ErrorMessage != "" {
				continue
			}

			// continue if BaremetalHost is paused or marked with UnhealthyAnnotation.
			annotations := bmh.GetAnnotations()
			if annotations != nil {
				if _, ok := annotations[metal3api.PausedAnnotation]; ok {
					continue
				}
				if _, ok := annotations[UnhealthyAnnotation]; ok {
					continue
				}
			}

			switch bmh.Status.Provisioning.State {
			case metal3api.StateReady, metal3api.StateAvailable:
			default:
				continue
			}
			m.Log.Info("Host matched hostSelector for Host, adding it to availableHosts list",
				"host", bmh.Name)
			availableHosts = append(availableHosts, &bmhs.Items[i])
		}
	}

	m.Log.Info("Host count available while choosing host for HostClaim", "hostcount", len(availableHosts))
	if len(availableHosts) == 0 {
		return nil, nil, nil
	}

	chosenHost, err := m.selectBMH(availableHosts)
	if err != nil {
		m.Log.Error(err, "Failed to select a Host")
		return nil, nil, err
	}

	helper, err := patch.NewHelper(chosenHost, m.client)
	return chosenHost, helper, err
}

// updateHostClaimStatus updates the status of the HostClaim with information from BareMetalHost
func (m *HostManager) updateHostClaimStatus(bmh *metal3api.BareMetalHost) {
	hostOld := m.HostClaim.Status.DeepCopy()

	// synchronize power status
	m.HostClaim.Status.PoweredOn = bmh.Status.PoweredOn
	m.HostClaim.Status.HardwareData = &metal3api.HardwareReference{
		Namespace: bmh.Namespace,
		Name:      bmh.Name,
	}
	switch bmh.Status.Provisioning.State {
	case metal3api.StateAvailable:
		m.SetConditionHostToTrue(metal3api.AvailableCondition, metal3api.AvailableReason)
		m.SetConditionHostToFalse(metal3api.ProvisionedCondition, metal3api.NotProvisionedReason, "")
	case metal3api.StateInspecting:
		m.SetConditionHostToFalse(metal3api.AvailableCondition, metal3api.InspectingReason, "")
		m.SetConditionHostToFalse(metal3api.ProvisionedCondition, metal3api.NotProvisionedReason, "")
	case metal3api.StateProvisioned:
		m.SetConditionHostToFalse(metal3api.AvailableCondition, metal3api.NotAvailableReason, "")
		m.SetConditionHostToTrue(metal3api.ProvisionedCondition, metal3api.ProvisionedReason)
	case metal3api.StateProvisioning:
		m.SetConditionHostToFalse(metal3api.AvailableCondition, metal3api.NotAvailableReason, "")
		m.SetConditionHostToFalse(metal3api.ProvisionedCondition, metal3api.ProvisioningReason, "")
	default:
		m.SetConditionHostToFalse(metal3api.AvailableCondition, metal3api.NotAvailableReason, "")
		m.SetConditionHostToFalse(metal3api.ProvisionedCondition, metal3api.NotProvisionedReason, "")
	}

	m.SetConditionHostToTrue(metal3api.AssociatedCondition, metal3api.BareMetalHostAssociatedReason)

	if !equality.Semantic.DeepEqual(m.HostClaim.Status, hostOld) {
		now := metav1.Now()
		m.HostClaim.Status.LastUpdated = &now
	}
}

// Transfer elements from argument bmhMap to hostMap and deletes the elements from
// hostMap whose key is in synced and do not appear in bmhMap. The result of the
// function is the comma separated names of keys from bmhMap.
func syncReboot(hostMap, bmhMap map[string]string) {
	for key := range bmhMap {
		elts := strings.Split(key, "/")
		// Propagates down unless it is just reboot and already propagated.
		if elts[0] == rebootDomain && len(elts) == 2 {
			if _, ok := hostMap[key]; !ok {
				delete(bmhMap, key)
			}
		}
	}

	// We propagate reboot annotations to the bmh when it appears.
	for key, v := range hostMap {
		elts := strings.Split(key, "/")
		// Propagates down unless it is just reboot and already propagated.
		if elts[0] == rebootDomain {
			bmhMap[key] = v
			if len(elts) == 1 {
				delete(hostMap, key)
			}
		}
	}
}

// nodeReuseLabelExists returns true if host contains nodeReuseLabelName label.
func nodeReuseLabelExists(bmh *metal3api.BareMetalHost) bool {
	if bmh == nil {
		return false
	}
	if bmh.Labels == nil {
		return false
	}
	_, ok := bmh.Labels[nodeReuseLabelName]
	return ok
}

func (m *HostManager) clearSecret(ctx context.Context, name, namespace string) error {
	secret := &corev1.Secret{}
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	if err := m.client.Get(ctx, key, secret); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return m.client.Delete(ctx, secret)
}

func patchIfFound(ctx context.Context, helper *patch.Helper, host client.Object) error {
	err := helper.Patch(ctx, host)
	if err != nil {
		notFound := true
		var aggr kerrors.Aggregate
		if ok := errors.As(err, &aggr); ok {
			for _, kerr := range aggr.Errors() {
				if !k8serrors.IsNotFound(kerr) {
					notFound = false
				}
				if k8serrors.IsConflict(kerr) {
					return &RequeueAfterError{RequeueAfter: ConflictRequeueDelay}
				}
			}
		} else {
			notFound = false
		}
		if notFound {
			return nil
		}
	}
	return err
}

type Set[T comparable] = map[T]struct{}

func NewSet[T comparable]() Set[T] {
	return Set[T]{}
}

func AddSet[T comparable](m Set[T], s T) {
	m[s] = struct{}{}
}

func SetContains[T comparable](m Set[T], s T) bool {
	_, ok := m[s]
	return ok
}
