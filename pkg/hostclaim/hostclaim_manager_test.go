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
	"encoding/json"
	"errors"
	"maps"
	"testing"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/metal3-io/baremetal-operator/internal/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// setupSchemes configures schemes.
func setupScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}

	if err := metal3api.AddToScheme(scheme); err != nil {
		panic(err)
	}

	return scheme
}

var _ = Describe("HostClaim manager", func() {

	var (
		defaultConsumerRef = corev1.ObjectReference{
			Name:       HostclaimName,
			Namespace:  HostclaimNamespace,
			Kind:       HostClaimKind,
			APIVersion: metal3api.GroupVersion.String(),
		}
		defaultImage = metal3api.Image{URL: "url"}
	)

	type testCaseChooseBMH struct {
		HostClaim          *metal3api.HostClaim
		HostDeployPolicies []*metal3api.HostDeployPolicy
		BareMetalHosts     []*metal3api.BareMetalHost
		Namespaces         []*corev1.Namespace
		ExpectedBmhName    string
		ExpectRequeue      bool
		ExpectFail         bool
	}

	var (
		hcNs                    = NewNamespace(HostclaimNamespace).Build()
		ns1                     = NewNamespace("ns1").Build()
		ns2                     = NewNamespace("ns2").Build()
		hcNsLabelled            = NewNamespace(HostclaimNamespace).SetLabels(map[string]string{"l": "v"}).Build()
		defaultBmhLabels        = map[string]string{"default-selector": "default-value"}
		defaultFailureBmhLabels = map[string]string{"default-selector": "default-value", "infrastructure.cluster.x-k8s.io/failure-domain": "zone"}
		bmhns1                  = NewBaremetalhost("bmh1", "ns1", metal3api.StateAvailable).SetLabels(defaultBmhLabels).Build()
		bmhns2                  = NewBaremetalhost("bmh2", "ns2", metal3api.StateAvailable).SetLabels(defaultBmhLabels).Build()
		bmh2ns1FailureDomain    = NewBaremetalhost("bmh2", "ns1", metal3api.StateAvailable).SetLabels(defaultFailureBmhLabels).Build()
		bmhns1BadLabel          = NewBaremetalhost("nolabel-bmh1", "ns1", metal3api.StateAvailable).Build()
		bmhns1NotAvail          = NewBaremetalhost("notavail-bmh1", "ns1", metal3api.StateRegistering).SetLabels(defaultBmhLabels).Build()
		bmhns1Paused            = NewBaremetalhost("paused-bmh1", "ns1", metal3api.StateRegistering).SetLabels(defaultBmhLabels).
					SetAnnotations(map[string]string{metal3api.PausedAnnotation: PausedAnnotationValue}).Build()
		bmhns1Consumed = NewBaremetalhost(
			"bmh-consumed", "ns1", metal3api.StateAvailable).SetLabels(defaultBmhLabels).
			SetConsumerRef(corev1.ObjectReference{Kind: HostClaimKind, Namespace: HostclaimNamespace,
				APIVersion: metal3api.GroupVersion.String(), Name: HostclaimName}).Build()
		bmhns1ConsOther = NewBaremetalhost(
			"bmh-cons-other", "ns1", metal3api.StateAvailable).SetLabels(defaultBmhLabels).
			SetConsumerRef(corev1.ObjectReference{Kind: HostClaimKind, Namespace: HostclaimNamespace,
				APIVersion: metal3api.GroupVersion.String(), Name: "other"}).Build()
	)

	DescribeTable("Test Finalizers",
		func(hc *metal3api.HostClaim) {
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
			hostMgr := NewManager(fakeClient, GinkgoLogr, hc, fakeClient)
			hostMgr.SetFinalizer()
			Expect(hc.ObjectMeta.Finalizers).To(ContainElement(
				metal3api.HostClaimFinalizer,
			))

			hostMgr.UnsetFinalizer()

			Expect(hc.ObjectMeta.Finalizers).NotTo(ContainElement(
				metal3api.HostClaimFinalizer,
			))
		},
		Entry("No finalizers", NewHostclaim(HostclaimName).Build()),
		Entry("Additional Finalizers",
			NewHostclaim(HostclaimName).SetFinalizer([]string{"finz"}).Build(),
		),
	)

	It("test IsProvisionned", func() {
		hc1 := NewHostclaim(HostclaimName).Build()
		hc2 := NewHostclaim(HostclaimName).SetCondition(metal3api.ProvisionedCondition, true, "reason").Build()
		fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
		hostMgr := NewManager(fakeClient, GinkgoLogr, hc1, fakeClient)
		Expect(hostMgr.IsProvisioned()).To(BeFalse())
		hostMgr = NewManager(fakeClient, GinkgoLogr, hc2, fakeClient)
		Expect(hostMgr.IsProvisioned()).To(BeTrue())
	})

	type testCaseSetPauseAnnotation struct {
		HostClaim             *metal3api.HostClaim
		BareMetalHost         *metal3api.BareMetalHost
		ExpectPausePresent    bool
		ExpectStatusPresent   bool
		ExpectBlockMoveAbsent bool
		ExpectError           bool
	}

	DescribeTable("Test Set BMH Pause Annotation",
		func(tc testCaseSetPauseAnnotation) {
			objs := []client.Object{tc.HostClaim}
			if tc.BareMetalHost != nil {
				objs = append(objs, tc.BareMetalHost)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objs...).Build()

			hostMgr := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)

			err := hostMgr.SetPauseAnnotation(context.TODO())
			if tc.ExpectError {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			if tc.BareMetalHost == nil {
				return
			}
			savedHost := metal3api.BareMetalHost{}
			err = fakeClient.Get(context.TODO(),
				client.ObjectKey{
					Name:      tc.BareMetalHost.Name,
					Namespace: tc.BareMetalHost.Namespace,
				},
				&savedHost,
			)
			Expect(err).NotTo(HaveOccurred())
			_, pausePresent := savedHost.Annotations[metal3api.PausedAnnotation]
			if tc.ExpectPausePresent {
				Expect(pausePresent).To(BeTrue())
			} else {
				Expect(pausePresent).To(BeFalse())
			}
			status, statusPresent := savedHost.Annotations[metal3api.StatusAnnotation]
			if tc.ExpectStatusPresent {
				Expect(statusPresent).To(BeTrue())
				annotation, err := json.Marshal(&tc.BareMetalHost.Status)
				Expect(err).ToNot(HaveOccurred())
				// (Note) manager code marshals the inspection data stored in annotation,
				// which causes alphabetically reordering of keys. Since we are marshaling
				// only the annotation, the status value here doesn't match the marshaled
				// annotation data, because it wasn't re-ordered by the JSON marshaller.
				// That's why we are marshaling status data as well so that its fields are
				// also alphabetically reordered to match the annotation keys style..
				obj := map[string]any{}
				err = json.Unmarshal(annotation, &obj)
				Expect(err).ToNot(HaveOccurred())
				annotation, _ = json.Marshal(obj)
				Expect(status).To(Equal(string(annotation)))
			} else {
				Expect(statusPresent).To(BeFalse())
			}
			if tc.ExpectBlockMoveAbsent {
				_, blockMovePresent := savedHost.Annotations[BlockMoveAnnotation]
				Expect(blockMovePresent).To(BeFalse(), "block-move annotation should be removed after SetPauseAnnotation")
			}

		},
		Entry("Set BMH Pause Annotation, with valid Paused annotations, already paused", testCaseSetPauseAnnotation{
			BareMetalHost: NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).
				SetAnnotations(map[string]string{metal3api.PausedAnnotation: PausedAnnotationValue, BlockMoveAnnotation: ""}).Build(),
			HostClaim:             NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectPausePresent:    true,
			ExpectBlockMoveAbsent: true,
			ExpectError:           false,
		}),
		Entry("Set BMH Pause Annotation, with valid Paused annotations, Empty Key, already paused", testCaseSetPauseAnnotation{
			BareMetalHost: NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).
				SetAnnotations(map[string]string{metal3api.PausedAnnotation: "", BlockMoveAnnotation: ""}).Build(),
			HostClaim:             NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectPausePresent:    true,
			ExpectBlockMoveAbsent: true,
			ExpectError:           false,
		}),
		Entry("Set BMH Pause Annotation, with no Paused annotations", testCaseSetPauseAnnotation{
			BareMetalHost:       NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).Build(),
			HostClaim:           NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectPausePresent:  true,
			ExpectStatusPresent: true,
			ExpectError:         false,
		}),
		Entry("Set BMH Pause Annotation, no bmh", testCaseSetPauseAnnotation{
			BareMetalHost: nil,
			HostClaim:     NewHostclaim(HostclaimName).Build(),
		}),
	)

	type testCaseRemovePauseAnnotation struct {
		HostClaim     *metal3api.HostClaim
		BareMetalHost *metal3api.BareMetalHost
		ExpectPresent bool
		ExpectError   bool
	}

	DescribeTable("Test Remove BMH Pause Annotation",
		func(tc testCaseRemovePauseAnnotation) {
			objs := []client.Object{tc.HostClaim}
			if tc.BareMetalHost != nil {
				objs = append(objs, tc.BareMetalHost)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objs...).Build()

			hostMgr := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)

			err := hostMgr.RemovePauseAnnotation(context.TODO())
			if tc.ExpectError {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			if tc.BareMetalHost == nil {
				return
			}
			savedHost := metal3api.BareMetalHost{}
			err = fakeClient.Get(context.TODO(),
				client.ObjectKey{
					Name:      tc.BareMetalHost.Name,
					Namespace: tc.BareMetalHost.Namespace,
				},
				&savedHost,
			)
			Expect(err).NotTo(HaveOccurred())
			if tc.ExpectPresent {
				Expect(savedHost.Annotations[metal3api.PausedAnnotation]).NotTo(BeNil())
			} else {
				Expect(savedHost.Annotations).To(BeNil())
			}
		},
		Entry("Remove BMH Pause Annotation, with valid Paused annotations", testCaseRemovePauseAnnotation{
			BareMetalHost: NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).
				SetAnnotations(map[string]string{metal3api.PausedAnnotation: PausedAnnotationValue}).Build(),
			HostClaim:     NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectPresent: false,
			ExpectError:   false,
		}),
		Entry("Do not Remove Annotation, with valid Paused annotations, Empty Key", testCaseRemovePauseAnnotation{
			BareMetalHost: NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).
				SetAnnotations(map[string]string{metal3api.PausedAnnotation: ""}).Build(),
			HostClaim:     NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectPresent: true,
			ExpectError:   false,
		}),
		Entry("No Annotation, Should Not Error", testCaseRemovePauseAnnotation{
			BareMetalHost: NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).Build(),
			HostClaim:     NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectPresent: false,
			ExpectError:   false,
		}),
		Entry("No Bmh, Should Not Error", testCaseRemovePauseAnnotation{
			BareMetalHost: nil,
			HostClaim:     NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
		}),
	)

	type testCaseEnsureBlockMoveAnnotation struct {
		HostClaim          *metal3api.HostClaim
		BareMetalHost      *metal3api.BareMetalHost
		ExpectBlockMoveSet bool
		ExpectError        bool
	}

	DescribeTable("Test EnsureBlockMoveAnnotation",
		func(tc testCaseEnsureBlockMoveAnnotation) {
			objs := []client.Object{tc.HostClaim}
			if tc.BareMetalHost != nil {
				objs = append(objs, tc.BareMetalHost)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objs...).Build()

			hostMgr := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)

			err := hostMgr.EnsureBlockMoveAnnotation(context.TODO())
			if tc.ExpectError {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			if tc.BareMetalHost == nil {
				return
			}
			savedHost := metal3api.BareMetalHost{}
			err = fakeClient.Get(context.TODO(),
				client.ObjectKey{
					Name:      tc.BareMetalHost.Name,
					Namespace: tc.BareMetalHost.Namespace,
				},
				&savedHost,
			)
			Expect(err).NotTo(HaveOccurred())
			if tc.ExpectBlockMoveSet {
				Expect(savedHost.Annotations).To(HaveKey(BlockMoveAnnotation))
			} else {
				Expect(savedHost.Annotations).NotTo(HaveKey(BlockMoveAnnotation))
			}
		},
		Entry("BMH with matching consumer ref, no annotations: block-move is set", testCaseEnsureBlockMoveAnnotation{
			BareMetalHost:      NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).Build(),
			HostClaim:          NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectBlockMoveSet: true,
		}),
		Entry("BMH with matching consumer ref, block-move already present: no change", testCaseEnsureBlockMoveAnnotation{
			BareMetalHost: NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).
				SetAnnotations(map[string]string{BlockMoveAnnotation: ""}).Build(),
			HostClaim:          NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectBlockMoveSet: true,
		}),
		Entry("BMH already paused: block-move is NOT (re-)set", testCaseEnsureBlockMoveAnnotation{
			BareMetalHost: NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).
				SetAnnotations(map[string]string{metal3api.PausedAnnotation: PausedAnnotationValue}).Build(),
			HostClaim:          NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectBlockMoveSet: false,
		}),
		Entry("BMH with no consumer ref, block-move not set", testCaseEnsureBlockMoveAnnotation{
			BareMetalHost:      NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).Build(),
			HostClaim:          NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectBlockMoveSet: false,
		}),
		Entry("BMH consumer ref points to another claim, block-move not set", testCaseEnsureBlockMoveAnnotation{
			BareMetalHost: NewBaremetalhost("bmh1", "ns1", metal3api.StateProvisioned).SetConsumerRef(
				corev1.ObjectReference{
					Name:       "other",
					Namespace:  HostclaimNamespace,
					Kind:       HostClaimKind,
					APIVersion: metal3api.GroupVersion.String(),
				}).Build(),
			HostClaim:          NewHostclaim(HostclaimName).SetAssociatedBMH("ns1", "bmh1").Build(),
			ExpectBlockMoveSet: false,
		}),
		Entry("HostClaim has no associated BMH ref", testCaseEnsureBlockMoveAnnotation{
			BareMetalHost: nil,
			HostClaim:     NewHostclaim(HostclaimName).Build(),
		}),
	)

	type testHasAnnotation struct {
		HostClaim *metal3api.HostClaim
		Result    bool
	}

	DescribeTable("Test HasAnnotation",
		func(tc testHasAnnotation) {
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
			hostMgr := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)
			result := hostMgr.HasAnnotation("annot")
			Expect(result).To(Equal(tc.Result))
		},
		Entry("no annotation", testHasAnnotation{HostClaim: NewHostclaim(HostclaimName).Build(), Result: false}),
		Entry("other annotation", testHasAnnotation{HostClaim: NewHostclaim(HostclaimName).SetAnnotations(map[string]string{"other": "v"}).Build(), Result: false}),
		Entry("other annotation", testHasAnnotation{HostClaim: NewHostclaim(HostclaimName).SetAnnotations(map[string]string{"annot": "ns/bmh"}).Build(), Result: true}),
	)

	DescribeTable("Test chooseBMH",
		func(tc testCaseChooseBMH) {
			objects := []client.Object{}
			objects = append(objects, tc.HostClaim)
			if tc.Namespaces != nil {
				for _, obj := range tc.Namespaces {
					objects = append(objects, obj)
				}
			}
			if tc.HostDeployPolicies != nil {
				for _, obj := range tc.HostDeployPolicies {
					objects = append(objects, obj)
				}
			}
			if tc.BareMetalHosts != nil {
				for _, obj := range tc.BareMetalHosts {
					objects = append(objects, obj)
					objects = append(objects, NewHardwareData(obj).Build())
				}
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			hostMgr, ok := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient).(*Manager)
			Expect(ok).To(BeTrue())
			bmh, err := hostMgr.chooseBMH(context.TODO())
			if tc.ExpectedBmhName == "" {
				Expect(bmh).To(BeNil())
				if tc.ExpectFail {
					Expect(err).To(HaveOccurred())
					var requeueAfterError RequeueAfterError
					Expect(errors.As(err, &requeueAfterError)).To(BeFalse())
				} else if tc.ExpectRequeue {
					Expect(err).To(HaveOccurred())
					var requeueAfterError RequeueAfterError
					Expect(errors.As(err, &requeueAfterError)).To(BeTrue())
				} else {
					Expect(errors.Is(err, ErrNoAvailableBMH)).To(BeTrue())
				}
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(bmh).NotTo(BeNil())
				Expect(bmh.Name).To(Equal(tc.ExpectedBmhName))
			}
		},
		Entry("no policies", testCaseChooseBMH{
			HostClaim:      NewHostclaim(HostclaimName).Build(),
			Namespaces:     []*corev1.Namespace{hcNs, ns1, ns2},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1, bmhns2},
		}),
		Entry("bad namespace name", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1, ns2},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{"otherNs"}).Build()},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1, bmhns2},
		}),
		Entry("with policy in ns1", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1, ns2},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "bmh1",
		}),
		Entry("HostClaim targets ns1 (positive)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).SetTargetNamespace("ns1").Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1, ns2},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build(),
				NewHostdeploypolicy("hdp", "ns2").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "bmh1",
		}),
		Entry("HostClaim targets ns1 (negative)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).SetTargetNamespace("ns1").Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1, ns2},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build(),
				NewHostdeploypolicy("hdp", "ns2").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns2},
		}),
		Entry("with criteriums (positive)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).SetLabelSelector(map[string]string{"default-selector": "default-value"}).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail, bmhns1Paused},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with criteriums (negative)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).SetLabelSelector(map[string]string{"default-selector": "default-value"}).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail, bmhns1Paused},
		}),
		Entry("with bad label value", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).SetLabelSelector(map[string]string{"default-selector": "other-value"}).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail, bmhns1Paused},
			ExpectedBmhName: "",
		}),
		Entry("with eroneous labels", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).SetLabelSelector(map[string]string{"*/123": "default-value"}).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail, bmhns1Paused},
			ExpectFail:     true,
		}),
		Entry("with consumerRef", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1Consumed, bmhns1, bmhns1ConsOther},
			ExpectedBmhName: "bmh-consumed",
		}),
		Entry("with expr (positive)", testCaseChooseBMH{
			HostClaim: NewHostclaim(HostclaimName).SetMatchExpressions(
				[]metal3api.HostSelectorRequirement{{
					Key: "default-selector", Values: []string{"other", "default-value"},
					Operator: selection.In}}).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with labeled hostclaim namespace", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).Build(),
			Namespaces: []*corev1.Namespace{hcNsLabelled, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptLabels([]metal3api.NameValuePair{{Name: "l", Value: "v"}}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with labeled hostclaim namespace (bad label)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).Build(),
			Namespaces: []*corev1.Namespace{hcNsLabelled, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptLabels([]metal3api.NameValuePair{{Name: "l", Value: "w"}}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "",
		}),
		Entry("with labeled hostclaim namespace (negative)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptLabels([]metal3api.NameValuePair{{Name: "l", Value: "v"}}).Build()},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1, bmhns2},
		}),
		Entry("with regexp on hostclaim namespace", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).Build(),
			Namespaces: []*corev1.Namespace{hcNsLabelled, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("hc.*").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with bad regexp on hostclaim namespace", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).Build(),
			Namespaces: []*corev1.Namespace{hcNsLabelled, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("hc[").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "",
			ExpectFail:      true,
		}),
		Entry("with regexp matching exact namespace", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).InNamespace("prod").Build(),
			Namespaces: []*corev1.Namespace{NewNamespace("prod").Build(), ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("prod").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with regexp should not match namespace with extra prefix", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).InNamespace("not-prod").Build(),
			Namespaces: []*corev1.Namespace{NewNamespace("not-prod").Build(), ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("prod").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with regexp should not match namespace with extra suffix", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).InNamespace("production").Build(),
			Namespaces: []*corev1.Namespace{NewNamespace("production").Build(), ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("prod").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with regexp should not match unrelated namespace containing substring", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).InNamespace("reproduce").Build(),
			Namespaces: []*corev1.Namespace{NewNamespace("reproduce").Build(), ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("prod").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with regexp anchored full match", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).InNamespace("prod").Build(),
			Namespaces: []*corev1.Namespace{NewNamespace("prod").Build(), ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("^prod$").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with regexp anchored partial match (positive)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).InNamespace("production").Build(),
			Namespaces: []*corev1.Namespace{NewNamespace("production").Build(), ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("^prod.*$").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with regexp anchored partial match should not match different prefix", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName).InNamespace("reproduce").Build(),
			Namespaces: []*corev1.Namespace{NewNamespace("reproduce").Build(), ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptRegexp("^prod.*$").Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1},
			ExpectedBmhName: "",
		}),
		Entry("with Failure Domain (available bmh)", testCaseChooseBMH{
			HostClaim: NewHostclaim(HostclaimName).
				SetLabelSelector(map[string]string{"default-selector": "default-value"}).
				SetFailureDomain("zone").Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmh2ns1FailureDomain},
			ExpectedBmhName: "bmh2",
		}),
		Entry("with Failure Domain (no available bmh in zone)", testCaseChooseBMH{
			HostClaim: NewHostclaim(HostclaimName).
				SetLabelSelector(map[string]string{"default-selector": "default-value"}).
				SetFailureDomain("zone").Build(),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1").AcceptNames([]string{HostclaimNamespace}).Build()},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1},
			ExpectedBmhName: "bmh1",
		}),
	)

	type testCaseAssociate struct {
		HostClaim     *metal3api.HostClaim
		ExpectFails   bool
		ExpectRequeue bool
	}

	It("test hide conflict error",
		func() {
			ctx := context.TODO()
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).Build()
			oldBmh := bmh.DeepCopy()
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(bmh).Build()
			bmh.Spec.Description = "v0"
			err := fakeClient.Update(ctx, bmh)
			Expect(err).NotTo(HaveOccurred())
			helper, err := patch.NewHelper(bmh, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			bmh.Spec.Description = "v1"
			err = hideConflictError(helper.Patch(ctx, bmh))
			Expect(err).NotTo(HaveOccurred(), "Patch succeeds")
			helper, err = patch.NewHelper(oldBmh, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			oldBmh.ResourceVersion = "234"
			oldBmh.Spec.Description = "v2"
			err = hideConflictError(helper.Patch(ctx, oldBmh))
			Expect(err).To(HaveOccurred(), "Conflict error becomes requeue")
			var requeueAfterError RequeueAfterError
			Expect(errors.As(err, &requeueAfterError)).To(BeTrue())
		})

	DescribeTable("test Associate",
		func(tc testCaseAssociate) {
			sec := NewSecret("sec-user-data", HostclaimNamespace).SetData(map[string][]byte{"user-data": []byte("v")}).Build()
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).Build()
			objects := []client.Object{
				tc.HostClaim, bmh, sec,
				NewHostdeploypolicy("hdp", "ns").AcceptNames([]string{HostclaimNamespace}).Build(),
				NewNamespace("hcNs").Build(), NewNamespace("ns").Build(),
			}
			// We patch the status during associate to set the annotation.
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).WithStatusSubresource(tc.HostClaim).Build()
			hostMgr := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)
			err := hostMgr.Associate(context.TODO())
			if tc.ExpectFails {
				Expect(err).To(HaveOccurred())
				var requeueAfterError RequeueAfterError
				Expect(errors.As(err, &requeueAfterError)).To(Equal(tc.ExpectRequeue))
				return
			}
			Expect(err).NotTo(HaveOccurred())
			updatedBmh := &metal3api.BareMetalHost{}
			getErr := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(bmh), updatedBmh)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(updatedBmh.Spec.ConsumerRef).ToNot(BeNil())
			Expect(updatedBmh.Spec.ConsumerRef.Name).To(Equal(tc.HostClaim.Name))
			Expect(updatedBmh.Spec.ConsumerRef.Namespace).To(Equal(tc.HostClaim.Namespace))
			Expect(tc.HostClaim.Status.BareMetalHost).ToNot(BeNil())
			Expect(tc.HostClaim.Status.BareMetalHost.Name).To(Equal(bmh.Name))
			Expect(tc.HostClaim.Status.BareMetalHost.Namespace).To(Equal(bmh.Namespace))
		},
		Entry("Regular case", testCaseAssociate{
			HostClaim: NewHostclaim(HostclaimName).SetImage(defaultImage).SetUserData("sec-user-data").Build(),
		}),
		Entry("Bad Selector, True failure", testCaseAssociate{
			HostClaim: NewHostclaim(HostclaimName).
				SetMatchExpressions([]metal3api.HostSelectorRequirement{{
					Key: "k", Operator: selection.Exists, Values: []string{"a", "b"}}}).Build(),
			ExpectFails: true,
		}),
		Entry("Incompatible selector", testCaseAssociate{
			HostClaim: NewHostclaim(HostclaimName).
				SetMatchExpressions([]metal3api.HostSelectorRequirement{{
					Key: "k", Operator: selection.Exists, Values: []string{}}}).Build(),
			ExpectFails:   true,
			ExpectRequeue: true,
		}),
	)

	type testCaseSetBMHSpec struct {
		SetImage         bool
		SetCustomDeploy  bool
		SetPoweredOn     bool
		BmhImage         *metal3api.Image
		Updated          bool
		ExpectImageIssue bool
		ExpectedBmhImage *metal3api.Image
	}

	DescribeTable("Test setBMHspec",
		func(tc testCaseSetBMHSpec) {
			hcBuilder := NewHostclaim(HostclaimName)
			if tc.SetImage {
				hcBuilder = hcBuilder.SetImage(defaultImage)
			}
			if tc.SetCustomDeploy {
				hcBuilder = hcBuilder.SetCustomDeploy("custom")
			}
			if tc.SetPoweredOn {
				hcBuilder = hcBuilder.SetPowerOn()
			}
			hostClaim := hcBuilder.Build()
			bmhBuilder := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable)
			if tc.BmhImage != nil {
				bmhBuilder = bmhBuilder.SetImage(*tc.BmhImage)
			}
			bmh := bmhBuilder.Build()
			hostMgr, ok := NewManager(nil, GinkgoLogr, hostClaim, nil).(*Manager)
			Expect(ok).To(BeTrue())
			updated, imageMismatch := hostMgr.setBmhSpec(bmh)
			if tc.ExpectedBmhImage != nil {
				Expect(bmh.Spec.Image).NotTo(BeNil())
				Expect(*bmh.Spec.Image).To(Equal(*tc.ExpectedBmhImage))
			} else if tc.SetImage && tc.BmhImage == nil {
				Expect(bmh.Spec.Image).NotTo(BeNil())
				Expect(*bmh.Spec.Image).To(Equal(defaultImage))
			}
			if tc.SetCustomDeploy {
				Expect(bmh.Spec.CustomDeploy).NotTo(BeNil())
				Expect(bmh.Spec.CustomDeploy.Method).To(Equal("custom"))
			}
			Expect(bmh.Spec.Online).To(Equal(tc.SetPoweredOn))
			Expect(updated).To(Equal(tc.Updated))
			Expect(imageMismatch).To(Equal(tc.ExpectImageIssue))
			updated, imageMismatch = hostMgr.setBmhSpec(bmh)
			Expect(updated).To(BeFalse())
			Expect(imageMismatch).To(Equal(tc.ExpectImageIssue))
		},
		Entry("set image", testCaseSetBMHSpec{
			SetImage: true,
			Updated:  true,
		}),
		Entry("set custom deploy", testCaseSetBMHSpec{
			SetCustomDeploy: true,
			Updated:         true,
		}),
		Entry("set power on", testCaseSetBMHSpec{
			SetPoweredOn: true,
			Updated:      true,
		}),
		Entry("image mismatch keeps the bmh image", testCaseSetBMHSpec{
			SetImage:         true,
			BmhImage:         &metal3api.Image{URL: "other"},
			ExpectedBmhImage: &metal3api.Image{URL: "other"},
			ExpectImageIssue: true,
		}),
	)

	It("Test updateHostClaimStatus (cross-namespace: points at claim-namespace copy)",
		func() {
			hostClaim := NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build()
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).
				SetCondition(metal3api.ProvisionedCondition, false, "reason").
				SetCondition(metal3api.AvailableForProvisioningCondition, true, "reason").
				Build()
			bmh.Status.PoweredOn = true
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
			hostMgr, ok := NewManager(fakeClient, GinkgoLogr, hostClaim, fakeClient).(*Manager)
			Expect(ok).To(BeTrue())
			hostMgr.updateHostClaimStatus(bmh)
			Expect(hostClaim.Status.PoweredOn).To(BeTrue())
			Expect(hostClaim.Status.HardwareData).NotTo(BeNil())
			Expect(hostClaim.Status.HardwareData.Name).To(Equal(hostClaim.Name))
			Expect(hostClaim.Status.HardwareData.Namespace).To(Equal(hostClaim.Namespace))
			Expect(conditions.IsFalse(hostClaim, metal3api.ProvisionedCondition)).To(BeTrue())
			Expect(conditions.IsTrue(hostClaim, metal3api.AvailableForProvisioningCondition)).To(BeTrue())
		})

	It("Test updateHostClaimStatus (same-namespace: points at source BMH-named HD)",
		func() {
			hostClaim := NewHostclaim(HostclaimName).SetAssociatedBMH(HostclaimNamespace, "bmh").Build()
			bmh := NewBaremetalhost("bmh", HostclaimNamespace, metal3api.StateAvailable).Build()
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
			hostMgr, ok := NewManager(fakeClient, GinkgoLogr, hostClaim, fakeClient).(*Manager)
			Expect(ok).To(BeTrue())
			hostMgr.updateHostClaimStatus(bmh)
			Expect(hostClaim.Status.HardwareData).NotTo(BeNil())
			Expect(hostClaim.Status.HardwareData.Name).To(Equal(bmh.Name))
			Expect(hostClaim.Status.HardwareData.Namespace).To(Equal(bmh.Namespace))
		})

	It("test syncReboot",
		func() {
			otherReboot := metal3api.RebootAnnotationPrefix + "/manual"
			claimAnnot := metal3api.RebootAnnotationPrefix + "/test"
			bmhAnnot := metal3api.RebootAnnotationPrefix + "/" + RebootHostClaimOwnedPrefix + "test"
			annots := map[string]string{"a": "v1", otherReboot: "v2"}
			saved := maps.Clone(annots)
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).SetAnnotations(annots).Build()
			// Phased reboot
			hostClaim := NewHostclaim(HostclaimName).SetAnnotations(map[string]string{claimAnnot: "v0"}).SetAssociatedBMH("ns", "bmh").Build()
			upd := syncReboot(hostClaim.Annotations, bmh.Annotations)
			Expect(bmh.Annotations).To(HaveLen(3))
			Expect(bmh.Annotations[bmhAnnot]).To(Equal("v0"))
			Expect(bmh.Annotations[otherReboot]).To(Equal("v2"))
			Expect(upd).To(BeTrue())
			// idempotence
			upd = syncReboot(hostClaim.Annotations, bmh.Annotations)
			Expect(upd).To(BeFalse())
			// Remove reboot Annotation on the HostClaim.
			delete(hostClaim.Annotations, claimAnnot)
			upd = syncReboot(hostClaim.Annotations, bmh.Annotations)
			Expect(maps.Equal(bmh.Annotations, saved)).To(BeTrue())
			Expect(upd).To(BeTrue())
			// idempotence
			upd = syncReboot(hostClaim.Annotations, bmh.Annotations)
			Expect(upd).To(BeFalse())
			// simple reboot
			hostClaim.Annotations[metal3api.RebootAnnotationPrefix] = "v3"
			upd = syncReboot(hostClaim.Annotations, bmh.Annotations)
			Expect(upd).To(BeTrue(), "transient reboot propagated")
			saved[metal3api.RebootAnnotationPrefix] = "v3"
			Expect(maps.Equal(bmh.Annotations, saved)).To(BeTrue())
		},
	)

	type testCaseSyncDetach struct {
		AcceptHostClaimNs string
		InHostClaimAnnot  string
		InBmhAnnot        string
		OutHostClaimAnnot string
		OutBmhAnnot       string
		Updated           bool
	}

	DescribeTable("test syncDetach",
		func(tc testCaseSyncDetach) {
			ctx := context.TODO()
			ns := NewNamespace(HostclaimNamespace).Build()

			annotHc := map[string]string{}
			annotBmh := map[string]string{}
			if tc.InHostClaimAnnot != "" {
				annotHc = map[string]string{metal3api.DetachedAnnotation: tc.InHostClaimAnnot}
			}
			if tc.InBmhAnnot != "" {
				annotBmh = map[string]string{metal3api.DetachedAnnotation: tc.InBmhAnnot}
			}

			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).SetAnnotations(annotBmh).SetConsumerRef(defaultConsumerRef).Build()
			hostClaim := NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").SetAnnotations(annotHc).Build()
			hdp := NewHostdeploypolicy("hdp", "ns").AcceptNames([]string{tc.AcceptHostClaimNs}).AllowsDetaching().Build()
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(bmh, hostClaim, ns, hdp).Build()
			hostMgr, ok := NewManager(fakeClient, GinkgoLogr, hostClaim, fakeClient).(*Manager)
			Expect(ok).To(BeTrue())
			updated, err := hostMgr.syncDetached(ctx, "ns", hostClaim.Annotations, bmh.Annotations)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated).To(Equal(tc.Updated))
			outBmhAnnot := ""
			if bmh.Annotations != nil {
				outBmhAnnot = bmh.Annotations[metal3api.DetachedAnnotation]
			}
			Expect(outBmhAnnot).To(Equal(tc.OutBmhAnnot))
		},
		Entry("not allowed", testCaseSyncDetach{AcceptHostClaimNs: "other", InHostClaimAnnot: "{}", OutBmhAnnot: ""}),
		Entry("allowed",
			testCaseSyncDetach{
				AcceptHostClaimNs: HostclaimNamespace, InHostClaimAnnot: "{}", OutBmhAnnot: "{\"setByHostClaim\":true}", Updated: true}),
		Entry("sanitize",
			testCaseSyncDetach{
				AcceptHostClaimNs: HostclaimNamespace,
				InHostClaimAnnot:  "{\"deleteAction\": \"delay\"}",
				OutBmhAnnot:       "{\"deleteAction\":\"delay\",\"setByHostClaim\":true}",
				Updated:           true}),
		Entry("no override",
			testCaseSyncDetach{AcceptHostClaimNs: HostclaimNamespace, InHostClaimAnnot: "{}",
				InBmhAnnot: "zzz", OutBmhAnnot: "zzz"}),
		Entry("delete",
			testCaseSyncDetach{
				AcceptHostClaimNs: HostclaimNamespace, InHostClaimAnnot: "",
				InBmhAnnot: "{\"setByHostClaim\": true}", OutBmhAnnot: "", Updated: true}),
		Entry("no delete",
			testCaseSyncDetach{
				AcceptHostClaimNs: HostclaimNamespace, InHostClaimAnnot: "",
				InBmhAnnot: "{}", OutBmhAnnot: "{}"}),
	)

	type testCaseUpdate struct {
		HostClaim              *metal3api.HostClaim
		BareMetalHost          *metal3api.BareMetalHost
		ExpectFail             bool
		ExpectClear            bool
		ExpectSyncReason       string
		ExpectBmhImageURL      string
		ExpectBmhOnline        bool
		ExpectHostClaimPowerOn bool
		ExpectHardwareData     bool
	}
	DescribeTable("test Update",
		func(tc testCaseUpdate) {
			ctx := context.TODO()
			hc := tc.HostClaim
			var bmh *metal3api.BareMetalHost
			if tc.BareMetalHost != nil {
				bmh = tc.BareMetalHost
			} else {
				bmh = NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).SetConsumerRef(defaultConsumerRef).Build()
			}
			objects := []client.Object{
				hc, bmh,
				NewHostdeploypolicy("hdp", "ns").AcceptNames([]string{HostclaimNamespace}).Build(),
				NewNamespace("hcNs").Build(), NewNamespace("ns").Build(),
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			hostMgr := NewManager(fakeClient, GinkgoLogr, hc, fakeClient)
			err := hostMgr.Update(ctx)
			if tc.ExpectFail {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			if tc.ExpectSyncReason != "" {
				sync := conditions.Get(hc, metal3api.SynchronizedCondition)
				Expect(sync).NotTo(BeNil())
				Expect(sync.Reason).To(Equal(tc.ExpectSyncReason))
			}
			if tc.ExpectClear {
				Expect(hc.Status.BareMetalHost).To(BeNil())
				Expect(hc.Status.HardwareData).To(BeNil())
				Expect(hc.Status.PoweredOn).To(BeFalse())
				// associated should be reset
				assoc := conditions.Get(hc, metal3api.AssociatedCondition)
				Expect(assoc).NotTo(BeNil())
				Expect(assoc.Status).To(Equal(metav1.ConditionFalse))
				Expect(assoc.Reason).To(Equal(metal3api.MissingBareMetalHostReason))
				for _, condType := range []string{metal3api.ProvisionedCondition, metal3api.AvailableForProvisioningCondition} {
					c := conditions.Get(hc, condType)
					Expect(c).NotTo(BeNil(), condType)
					Expect(c.Status).To(Equal(metav1.ConditionUnknown), condType)
				}
				return
			}
			bmhOut := &metal3api.BareMetalHost{}
			Expect(fakeClient.Get(ctx, client.ObjectKeyFromObject(bmh), bmhOut)).To(Succeed())
			if tc.ExpectBmhImageURL != "" {
				Expect(bmhOut.Spec.Image).NotTo(BeNil())
				Expect(bmhOut.Spec.Image.URL).To(Equal(tc.ExpectBmhImageURL))
			}
			Expect(bmhOut.Spec.Online).To(Equal(tc.ExpectBmhOnline))
			Expect(hc.Status.PoweredOn).To(Equal(tc.ExpectHostClaimPowerOn))
			if tc.ExpectHardwareData {
				Expect(hc.Status.HardwareData).NotTo(BeNil())
				Expect(hc.Status.HardwareData.Name).To(Equal(hc.Name))
				Expect(hc.Status.HardwareData.Namespace).To(Equal(hc.Namespace))
				Expect(hc.Status.LastUpdated).NotTo(BeNil())
			}
			assoc := conditions.Get(hc, metal3api.AssociatedCondition)
			Expect(assoc).NotTo(BeNil())
			Expect(assoc.Status).To(Equal(metav1.ConditionTrue))
		},
		Entry("Regular case (no spec change)", testCaseUpdate{
			HostClaim:          NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build(),
			ExpectSyncReason:   metal3api.ConfigurationSyncedReason,
			ExpectHardwareData: true,
		}),
		Entry("Propagates image and power on to BMH", testCaseUpdate{
			HostClaim: NewHostclaim(HostclaimName).SetImage(defaultImage).SetPowerOn().
				SetAssociatedBMH("ns", "bmh").Build(),
			ExpectSyncReason:   metal3api.ConfigurationSyncedReason,
			ExpectBmhImageURL:  defaultImage.URL,
			ExpectBmhOnline:    true,
			ExpectHardwareData: true,
		}),
		Entry("Image mismatch keeps BMH image and sets Synchronized=False", testCaseUpdate{
			HostClaim: NewHostclaim(HostclaimName).SetImage(metal3api.Image{URL: "new"}).
				SetAssociatedBMH("ns", "bmh").Build(),
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateProvisioned).
				SetConsumerRef(defaultConsumerRef).
				SetImage(metal3api.Image{URL: "old"}).Build(),
			ExpectSyncReason:   metal3api.OtherImageProvisionedReason,
			ExpectBmhImageURL:  "old",
			ExpectHardwareData: true,
		}),
		Entry("Mirrors BMH power state into hostClaim status", testCaseUpdate{
			HostClaim: NewHostclaim(HostclaimName).SetPowerOn().
				SetAssociatedBMH("ns", "bmh").Build(),
			BareMetalHost: func() *metal3api.BareMetalHost {
				b := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).
					SetConsumerRef(defaultConsumerRef).SetPowerOn().Build()
				b.Status.PoweredOn = true
				return b
			}(),
			ExpectSyncReason:       metal3api.ConfigurationSyncedReason,
			ExpectBmhOnline:        true,
			ExpectHostClaimPowerOn: true,
			ExpectHardwareData:     true,
		}),
		Entry("badly associated case", testCaseUpdate{
			HostClaim:        NewHostclaim("other").SetAssociatedBMH("ns", "bmh").Build(),
			ExpectSyncReason: metal3api.BareMetalHostNotSynchronizedReason,
			ExpectClear:      true,
		}),
		Entry("no bmh", testCaseUpdate{
			HostClaim:        NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "other").Build(),
			ExpectSyncReason: metal3api.BareMetalHostNotSynchronizedReason,
			ExpectClear:      true,
		}),
		Entry("no status reference", testCaseUpdate{
			HostClaim:        NewHostclaim(HostclaimName).Build(),
			ExpectSyncReason: metal3api.BareMetalHostNotSynchronizedReason,
			ExpectClear:      true,
		}),
	)

	It("test Update clears reboot annotation on hostClaim after success",
		func() {
			ctx := context.TODO()
			hc := NewHostclaim(HostclaimName).
				SetAnnotations(map[string]string{metal3api.RebootAnnotationPrefix: ""}).
				SetAssociatedBMH("ns", "bmh").Build()
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).SetConsumerRef(defaultConsumerRef).Build()
			objects := []client.Object{
				hc, bmh,
				NewHostdeploypolicy("hdp", "ns").AcceptNames([]string{HostclaimNamespace}).Build(),
				NewNamespace("hcNs").Build(), NewNamespace("ns").Build(),
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			hostMgr := NewManager(fakeClient, GinkgoLogr, hc, fakeClient)
			Expect(hostMgr.Update(ctx)).To(Succeed())
			_, present := hc.Annotations[metal3api.RebootAnnotationPrefix]
			Expect(present).To(BeFalse(), "reboot annotation was consumed from the hostClaim")
			bmhOut := &metal3api.BareMetalHost{}
			Expect(fakeClient.Get(ctx, client.ObjectKeyFromObject(bmh), bmhOut)).To(Succeed())
			_, present = bmhOut.Annotations[metal3api.RebootAnnotationPrefix]
			Expect(present).To(BeTrue(), "reboot annotation was mirrored on the BMH")
		})

	type testCaseDelete struct {
		HostClaim           *metal3api.HostClaim
		BareMetalHost       *metal3api.BareMetalHost
		ExpectRequeue       bool
		ExpectBmhGone       bool
		ExpectImageCleared  bool
		ExpectCustomCleared bool
	}
	DescribeTable("Test Delete",
		func(tc testCaseDelete) {
			ctx := context.TODO()
			hc := tc.HostClaim
			if hc == nil {
				hc = NewHostclaim(HostclaimName).
					SetUserData("sec1").SetMetaData("sec2").SetNetworkData("sec3").
					SetAssociatedBMH("ns", "bmh").Build()
			}
			objects := []client.Object{
				hc,
				NewHostdeploypolicy("hdp", "ns").AcceptNames([]string{HostclaimNamespace}).Build(),
				NewNamespace("hcNs").Build(), NewNamespace("ns").Build(),
			}
			if tc.BareMetalHost != nil {
				objects = append(objects, tc.BareMetalHost)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			hostMgr := NewManager(fakeClient, GinkgoLogr, hc, fakeClient)
			err := hostMgr.Delete(ctx)
			if tc.ExpectBmhGone || tc.BareMetalHost == nil {
				Expect(err).NotTo(HaveOccurred(), "Delete returns nil when there is no reachable BMH")
				return
			}
			bmhOut := &metal3api.BareMetalHost{}
			getErr := fakeClient.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "bmh"}, bmhOut)
			Expect(getErr).NotTo(HaveOccurred())
			if tc.ExpectRequeue {
				Expect(err).To(HaveOccurred())
				isRequeue, _ := IsRequeueAfterError(err)
				Expect(isRequeue).To(BeTrue())
				Expect(bmhOut.Spec.ConsumerRef).NotTo(BeNil(), "Access to bmh is kept while waiting")
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(bmhOut.Spec.ConsumerRef).To(BeNil(), "Access to bmh is revoked")
			}
			if tc.ExpectImageCleared {
				Expect(bmhOut.Spec.Image).To(BeNil(), "Image was cleared")
			}
			if tc.ExpectCustomCleared {
				Expect(bmhOut.Spec.CustomDeploy).To(BeNil(), "CustomDeploy was cleared")
			}
			Expect(bmhOut.Spec.Online).To(BeFalse(), "host is offline")
		},
		Entry("Wait for deprovisioning: image is cleared, requeue", testCaseDelete{
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateProvisioned).
				SetConsumerRef(defaultConsumerRef).SetImage(defaultImage).SetPowerOn().Build(),
			ExpectRequeue:      true,
			ExpectImageCleared: true,
		}),
		Entry("Wait for deprovisioning with customDeploy", testCaseDelete{
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateProvisioned).
				SetConsumerRef(defaultConsumerRef).SetCustomDeploy("custom").SetPowerOn().Build(),
			ExpectRequeue:       true,
			ExpectCustomCleared: true,
		}),
		Entry("Wait for provisioning state to transition after online was cleared", testCaseDelete{
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateDeprovisioning).
				SetConsumerRef(defaultConsumerRef).Build(),
			ExpectRequeue: true,
		}),
		Entry("ExternallyProvisioned: wait until host actually powers off", testCaseDelete{
			BareMetalHost: func() *metal3api.BareMetalHost {
				b := NewBaremetalhost("bmh", "ns", metal3api.StateExternallyProvisioned).
					SetConsumerRef(defaultConsumerRef).Build()
				b.Status.PoweredOn = true
				return b
			}(),
			ExpectRequeue: true,
		}),
		Entry("ExternallyProvisioned, already powered off: cleanup", testCaseDelete{
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateExternallyProvisioned).
				SetConsumerRef(defaultConsumerRef).Build(),
		}),
		Entry("Last step, cleanup", testCaseDelete{
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).
				SetConsumerRef(defaultConsumerRef).Build(),
		}),
		Entry("No BMH", testCaseDelete{
			BareMetalHost: nil,
			ExpectBmhGone: true,
		}),
		Entry("HostClaim status has no BMH reference", testCaseDelete{
			HostClaim: NewHostclaim(HostclaimName).Build(),
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).
				SetConsumerRef(defaultConsumerRef).Build(),
			ExpectBmhGone: true,
		}),
		Entry("BMH ConsumerRef points elsewhere: nothing to clean", testCaseDelete{
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable).
				SetConsumerRef(corev1.ObjectReference{
					Name:       "other",
					Namespace:  HostclaimNamespace,
					Kind:       HostClaimKind,
					APIVersion: metal3api.GroupVersion.String(),
				}).Build(),
			ExpectBmhGone: true,
		}),
	)

	DescribeTable("Test filterMetadata",
		func(in, want map[string]string) {
			Expect(filterMetadata(in)).To(Equal(want))
		},
		Entry("nil input returns nil", nil, map[string]string(nil)),
		Entry("empty input returns empty",
			map[string]string{},
			map[string]string{},
		),
		Entry("filters metal3.io",
			map[string]string{"metal3.io/foo": "v", "keep": "v"},
			map[string]string{"keep": "v"},
		),
		Entry("filters kubernetes.io and *.kubernetes.io",
			map[string]string{
				"kubernetes.io/arch":      "amd64",
				"node.kubernetes.io/zone": "a",
				"example.com/foo":         "bar",
			},
			map[string]string{"example.com/foo": "bar"},
		),
		Entry("keeps bare keys and multi-slash keys",
			map[string]string{
				"single":        "v",
				"a/b/c":         "v",
				"metal3.io/foo": "drop",
			},
			map[string]string{"single": "v", "a/b/c": "v"},
		),
	)

	type testCaseSyncHardwareData struct {
		HostClaim        *metal3api.HostClaim
		SourceHardware   *metal3api.HardwareData // in BMH namespace; nil = not present
		ExistingCopy     *metal3api.HardwareData // in HostClaim namespace; nil = not present
		BMHLabels        map[string]string
		BMHAnnotations   map[string]string
		ExpectCopyExists bool
		ExpectDetails    *metal3api.HardwareDetails
		ExpectLabels     map[string]string
		ExpectAnnots     map[string]string
		ExpectError      bool
	}

	DescribeTable("Test syncHardwareData",
		func(tc testCaseSyncHardwareData) {
			ctx := context.TODO()
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateProvisioned).SetConsumerRef(defaultConsumerRef).Build()
			if tc.BMHLabels != nil {
				bmh.Labels = tc.BMHLabels
			}
			if tc.BMHAnnotations != nil {
				bmh.Annotations = tc.BMHAnnotations
			}
			objs := []client.Object{tc.HostClaim, bmh}
			if tc.SourceHardware != nil {
				objs = append(objs, tc.SourceHardware)
			}
			if tc.ExistingCopy != nil {
				objs = append(objs, tc.ExistingCopy)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objs...).Build()
			hostMgr, ok := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient).(*Manager)
			Expect(ok).To(BeTrue())
			err := hostMgr.syncHardwareData(ctx, bmh)
			if tc.ExpectError {
				Expect(err).To(HaveOccurred())
				return
			}
			Expect(err).NotTo(HaveOccurred())
			hdCopy := &metal3api.HardwareData{}
			getErr := fakeClient.Get(ctx, client.ObjectKey{
				Name: tc.HostClaim.Name, Namespace: tc.HostClaim.Namespace,
			}, hdCopy)
			if !tc.ExpectCopyExists {
				Expect(k8serrors.IsNotFound(getErr)).To(BeTrue(), "copy must not exist")
				return
			}
			Expect(getErr).NotTo(HaveOccurred())
			Expect(hdCopy.Spec.HardwareDetails).To(Equal(tc.ExpectDetails))
			if tc.ExpectLabels == nil {
				Expect(hdCopy.Labels).To(BeEmpty())
			} else {
				Expect(hdCopy.Labels).To(Equal(tc.ExpectLabels))
			}
			if tc.ExpectAnnots == nil {
				Expect(hdCopy.Annotations).To(BeEmpty())
			} else {
				Expect(hdCopy.Annotations).To(Equal(tc.ExpectAnnots))
			}
			if tc.ExistingCopy == nil && tc.SourceHardware != nil {
				// Fresh copy created by sync: owner ref must be set.
				Expect(hdCopy.OwnerReferences).To(HaveLen(1))
				Expect(hdCopy.OwnerReferences[0].Name).To(Equal(tc.HostClaim.Name))
				Expect(hdCopy.OwnerReferences[0].Kind).To(Equal("HostClaim"))
			}
			if tc.SourceHardware != nil {
				// Source HD must carry the bound annotation so HardwareData
				// events can be routed back to this claim without a BMH lookup.
				src := &metal3api.HardwareData{}
				Expect(fakeClient.Get(ctx, client.ObjectKey{
					Name: tc.SourceHardware.Name, Namespace: tc.SourceHardware.Namespace,
				}, src)).To(Succeed())
				Expect(src.Annotations).To(HaveKeyWithValue(
					HostClaimBoundAnnotation,
					tc.HostClaim.Namespace+"/"+tc.HostClaim.Name,
				))
				// The copy must NOT carry the bound annotation (filterMetadata drops it).
				Expect(hdCopy.Annotations).NotTo(HaveKey(HostClaimBoundAnnotation))
			}
		},
		Entry("Source missing, no copy: no-op", testCaseSyncHardwareData{
			HostClaim:        NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build(),
			ExpectCopyExists: false,
		}),
		Entry("Source missing, copy exists: copy preserved (inspection gap)", testCaseSyncHardwareData{
			HostClaim: NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build(),
			ExistingCopy: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{Name: HostclaimName, Namespace: HostclaimNamespace},
				Spec:       metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "stale"}},
			},
			ExpectCopyExists: true,
			ExpectDetails:    &metal3api.HardwareDetails{Hostname: "stale"},
		}),
		Entry("Create copy when source exists", testCaseSyncHardwareData{
			HostClaim: NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build(),
			SourceHardware: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{Name: "bmh", Namespace: "ns"},
				Spec:       metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "host1"}},
			},
			BMHLabels:        map[string]string{"metal3.io/skip": "x", "keep": "v"},
			BMHAnnotations:   map[string]string{"kubernetes.io/arch": "amd64", "note": "hi"},
			ExpectCopyExists: true,
			ExpectDetails:    &metal3api.HardwareDetails{Hostname: "host1"},
			ExpectLabels:     map[string]string{"keep": "v"},
			ExpectAnnots:     map[string]string{"note": "hi"},
		}),
		Entry("Update copy when source details change", testCaseSyncHardwareData{
			HostClaim: NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build(),
			SourceHardware: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{Name: "bmh", Namespace: "ns"},
				Spec:       metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "new"}},
			},
			ExistingCopy: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{Name: HostclaimName, Namespace: HostclaimNamespace},
				Spec:       metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "old"}},
			},
			ExpectCopyExists: true,
			ExpectDetails:    &metal3api.HardwareDetails{Hostname: "new"},
		}),
		Entry("No-op when copy already matches", testCaseSyncHardwareData{
			HostClaim: NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build(),
			SourceHardware: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{Name: "bmh", Namespace: "ns"},
				Spec:       metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "same"}},
			},
			BMHLabels: map[string]string{"keep": "v"},
			ExistingCopy: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name:      HostclaimName,
					Namespace: HostclaimNamespace,
					Labels:    map[string]string{"keep": "v"},
				},
				Spec: metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "same"}},
			},
			ExpectCopyExists: true,
			ExpectDetails:    &metal3api.HardwareDetails{Hostname: "same"},
			ExpectLabels:     map[string]string{"keep": "v"},
		}),
	)

	type testCaseSyncInPlace struct {
		HostClaim         *metal3api.HostClaim
		SourceHardware    *metal3api.HardwareData
		BMHLabels         map[string]string
		BMHAnnotations    map[string]string
		OriginalDetails   *metal3api.HardwareDetails
		OriginalOwnerRefs []metav1.OwnerReference
		ExpectLabels      map[string]string
		ExpectAnnots      map[string]string
	}

	DescribeTable("Test syncHardwareData (same-namespace / in-place)",
		func(tc testCaseSyncInPlace) {
			ctx := context.TODO()
			// Same-namespace: BMH lives in HostclaimNamespace.
			bmh := NewBaremetalhost("bmh", HostclaimNamespace, metal3api.StateProvisioned).
				SetConsumerRef(defaultConsumerRef).Build()
			if tc.BMHLabels != nil {
				bmh.Labels = tc.BMHLabels
			}
			if tc.BMHAnnotations != nil {
				bmh.Annotations = tc.BMHAnnotations
			}
			objs := []client.Object{tc.HostClaim, bmh}
			if tc.SourceHardware != nil {
				objs = append(objs, tc.SourceHardware)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objs...).Build()
			hostMgr, ok := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient).(*Manager)
			Expect(ok).To(BeTrue())
			Expect(hostMgr.syncHardwareData(ctx, bmh)).To(Succeed())

			// No "copy" at (claimNs, claim.Name) must be created in the same-ns path.
			dup := &metal3api.HardwareData{}
			dupErr := fakeClient.Get(ctx, client.ObjectKey{
				Name: tc.HostClaim.Name, Namespace: tc.HostClaim.Namespace,
			}, dup)
			Expect(k8serrors.IsNotFound(dupErr)).To(BeTrue(), "no copy must be created in same-namespace case")

			if tc.SourceHardware == nil {
				return
			}
			src := &metal3api.HardwareData{}
			Expect(fakeClient.Get(ctx, client.ObjectKey{
				Name: tc.SourceHardware.Name, Namespace: tc.SourceHardware.Namespace,
			}, src)).To(Succeed())
			if tc.ExpectLabels == nil {
				Expect(src.Labels).To(BeEmpty())
			} else {
				Expect(src.Labels).To(Equal(tc.ExpectLabels))
			}
			if tc.ExpectAnnots == nil {
				Expect(src.Annotations).To(BeEmpty())
			} else {
				Expect(src.Annotations).To(Equal(tc.ExpectAnnots))
			}
			// Spec and OwnerReferences must not be touched by the in-place path.
			Expect(src.Spec.HardwareDetails).To(Equal(tc.OriginalDetails))
			Expect(src.OwnerReferences).To(Equal(tc.OriginalOwnerRefs))
		},
		Entry("Source missing: no-op", testCaseSyncInPlace{
			HostClaim: NewHostclaim(HostclaimName).SetAssociatedBMH(HostclaimNamespace, "bmh").Build(),
		}),
		Entry("Write filtered labels/annotations + bound annotation onto source HD", testCaseSyncInPlace{
			HostClaim: NewHostclaim(HostclaimName).SetAssociatedBMH(HostclaimNamespace, "bmh").Build(),
			SourceHardware: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bmh",
					Namespace: HostclaimNamespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "metal3.io/v1alpha1", Kind: "BareMetalHost",
						Name: "bmh", UID: "bmh-uid",
					}},
				},
				Spec: metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "untouched"}},
			},
			BMHLabels:       map[string]string{"metal3.io/drop": "x", "keep": "v"},
			BMHAnnotations:  map[string]string{"kubernetes.io/arch": "amd64", "note": "hi"},
			OriginalDetails: &metal3api.HardwareDetails{Hostname: "untouched"},
			OriginalOwnerRefs: []metav1.OwnerReference{{
				APIVersion: "metal3.io/v1alpha1", Kind: "BareMetalHost",
				Name: "bmh", UID: "bmh-uid",
			}},
			ExpectLabels: map[string]string{"keep": "v"},
			ExpectAnnots: map[string]string{
				"note":                   "hi",
				HostClaimBoundAnnotation: HostclaimNamespace + "/" + HostclaimName,
			},
		}),
		Entry("No-op when metadata already matches (bound annotation preserved)", testCaseSyncInPlace{
			HostClaim: NewHostclaim(HostclaimName).SetAssociatedBMH(HostclaimNamespace, "bmh").Build(),
			SourceHardware: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bmh", Namespace: HostclaimNamespace,
					Labels:      map[string]string{"keep": "v"},
					Annotations: map[string]string{HostClaimBoundAnnotation: HostclaimNamespace + "/" + HostclaimName},
				},
				Spec: metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "x"}},
			},
			BMHLabels:       map[string]string{"keep": "v"},
			OriginalDetails: &metal3api.HardwareDetails{Hostname: "x"},
			ExpectLabels:    map[string]string{"keep": "v"},
			ExpectAnnots:    map[string]string{HostClaimBoundAnnotation: HostclaimNamespace + "/" + HostclaimName},
		}),
	)

	type testCaseCleanupOnUnbind struct {
		HostClaim    *metal3api.HostClaim
		ReferencedHD *metal3api.HardwareData
		SourceHD     *metal3api.HardwareData

		ExpectReferencedHDDeleted bool
		ExpectSourceLabels        map[string]string
		ExpectSourceAnnots        map[string]string
	}

	DescribeTable("Test cleanupHardwareDataOnUnbind",
		func(tc testCaseCleanupOnUnbind) {
			ctx := context.TODO()
			objs := []client.Object{tc.HostClaim}
			if tc.ReferencedHD != nil {
				objs = append(objs, tc.ReferencedHD)
			}
			if tc.SourceHD != nil {
				objs = append(objs, tc.SourceHD)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objs...).Build()
			hostMgr, ok := NewManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient).(*Manager)
			Expect(ok).To(BeTrue())
			Expect(hostMgr.cleanupHardwareDataOnUnbind(ctx)).To(Succeed())

			if tc.ReferencedHD != nil {
				refHD := &metal3api.HardwareData{}
				err := fakeClient.Get(ctx, client.ObjectKeyFromObject(tc.ReferencedHD), refHD)
				if tc.ExpectReferencedHDDeleted {
					Expect(k8serrors.IsNotFound(err)).To(BeTrue(), "referenced HD should have been deleted")
				} else {
					Expect(err).NotTo(HaveOccurred())
					if tc.ExpectSourceLabels == nil {
						Expect(refHD.Labels).To(BeEmpty())
					} else {
						Expect(refHD.Labels).To(Equal(tc.ExpectSourceLabels))
					}
					if tc.ExpectSourceAnnots == nil {
						Expect(refHD.Annotations).To(BeEmpty())
					} else {
						Expect(refHD.Annotations).To(Equal(tc.ExpectSourceAnnots))
					}
					// Spec and OwnerReferences must not be touched on the same-ns path.
					Expect(refHD.Spec.HardwareDetails).To(Equal(tc.ReferencedHD.Spec.HardwareDetails))
					Expect(refHD.OwnerReferences).To(Equal(tc.ReferencedHD.OwnerReferences))
				}
			}
			if tc.SourceHD != nil {
				src := &metal3api.HardwareData{}
				Expect(fakeClient.Get(ctx, client.ObjectKeyFromObject(tc.SourceHD), src)).To(Succeed())
				if tc.ExpectSourceAnnots == nil {
					Expect(src.Annotations).To(BeEmpty())
				} else {
					Expect(src.Annotations).To(Equal(tc.ExpectSourceAnnots))
				}
			}
		},
		Entry("Status.HardwareData nil: no-op", testCaseCleanupOnUnbind{
			HostClaim: NewHostclaim(HostclaimName).Build(),
		}),
		Entry("Referenced HD missing: no-op", testCaseCleanupOnUnbind{
			HostClaim: func() *metal3api.HostClaim {
				hc := NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build()
				hc.UID = "claim-uid"
				hc.Status.HardwareData = &metal3api.ObjectReference{Namespace: HostclaimNamespace, Name: HostclaimName}
				return hc
			}(),
		}),
		Entry("Cross-ns: controlled copy is deleted; bound annotation stripped on source HD", testCaseCleanupOnUnbind{
			HostClaim: func() *metal3api.HostClaim {
				hc := NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build()
				hc.UID = "claim-uid"
				hc.Status.HardwareData = &metal3api.ObjectReference{Namespace: HostclaimNamespace, Name: HostclaimName}
				return hc
			}(),
			ReferencedHD: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name: HostclaimName, Namespace: HostclaimNamespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: metal3api.GroupVersion.Identifier(),
						Kind:       "HostClaim",
						Name:       HostclaimName,
						UID:        "claim-uid",
						Controller: ptr.To(true),
					}},
				},
			},
			SourceHD: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bmh", Namespace: "ns",
					Annotations: map[string]string{HostClaimBoundAnnotation: HostclaimNamespace + "/" + HostclaimName},
				},
			},
			ExpectReferencedHDDeleted: true,
			// Source HD annotations are now empty: the bound one was stripped.
		}),
		Entry("Cross-ns: controlled copy deleted; source HD left alone when ownership guard rejects", testCaseCleanupOnUnbind{
			HostClaim: func() *metal3api.HostClaim {
				hc := NewHostclaim(HostclaimName).SetAssociatedBMH("ns", "bmh").Build()
				hc.UID = "claim-uid"
				hc.Status.HardwareData = &metal3api.ObjectReference{Namespace: HostclaimNamespace, Name: HostclaimName}
				return hc
			}(),
			ReferencedHD: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name: HostclaimName, Namespace: HostclaimNamespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: metal3api.GroupVersion.Identifier(),
						Kind:       "HostClaim", Name: HostclaimName, UID: "claim-uid",
						Controller: ptr.To(true),
					}},
				},
			},
			SourceHD: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bmh", Namespace: "ns",
					Annotations: map[string]string{HostClaimBoundAnnotation: HostclaimNamespace + "/other"},
				},
			},
			ExpectReferencedHDDeleted: true,
			ExpectSourceAnnots:        map[string]string{HostClaimBoundAnnotation: HostclaimNamespace + "/other"},
		}),
		Entry("Same-ns: HD not controlled by claim → clear labels/annotations", testCaseCleanupOnUnbind{
			HostClaim: func() *metal3api.HostClaim {
				hc := NewHostclaim(HostclaimName).SetAssociatedBMH(HostclaimNamespace, "bmh").Build()
				hc.UID = "claim-uid"
				hc.Status.HardwareData = &metal3api.ObjectReference{Namespace: HostclaimNamespace, Name: "bmh"}
				return hc
			}(),
			ReferencedHD: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bmh", Namespace: HostclaimNamespace,
					Labels: map[string]string{"keep": "v"},
					Annotations: map[string]string{
						"note":                   "hi",
						HostClaimBoundAnnotation: HostclaimNamespace + "/" + HostclaimName,
					},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: metal3api.GroupVersion.Identifier(),
						Kind:       "BareMetalHost", Name: "bmh", UID: "bmh-uid",
						Controller: ptr.To(true),
					}},
				},
				Spec: metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "preserve"}},
			},
			// ExpectReferencedHDDeleted false; labels/annotations wiped.
		}),
		Entry("Same-ns: ownership guard rejects (annotation points elsewhere) → HD left intact", testCaseCleanupOnUnbind{
			HostClaim: func() *metal3api.HostClaim {
				hc := NewHostclaim(HostclaimName).SetAssociatedBMH(HostclaimNamespace, "bmh").Build()
				hc.UID = "claim-uid"
				hc.Status.HardwareData = &metal3api.ObjectReference{Namespace: HostclaimNamespace, Name: "bmh"}
				return hc
			}(),
			ReferencedHD: &metal3api.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bmh", Namespace: HostclaimNamespace,
					Labels: map[string]string{"keep": "v"},
					Annotations: map[string]string{
						"note":                   "hi",
						HostClaimBoundAnnotation: HostclaimNamespace + "/other",
					},
				},
				Spec: metal3api.HardwareDataSpec{HardwareDetails: &metal3api.HardwareDetails{Hostname: "preserve"}},
			},
			ExpectSourceLabels: map[string]string{"keep": "v"},
			ExpectSourceAnnots: map[string]string{
				"note":                   "hi",
				HostClaimBoundAnnotation: HostclaimNamespace + "/other",
			},
		}),
	)
})

func TestManagers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Manager Suite")
}
