//go:build unit

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
	"maps"
	"reflect"
	"strings"
	"testing"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/metal3-io/baremetal-operator/internal/testing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
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
	DescribeTable("Test Finalizers",
		func(hc *metal3api.HostClaim) {
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, hc, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			hostMgr.SetFinalizer()
			Expect(hc.ObjectMeta.Finalizers).To(ContainElement(
				metal3api.HostClaimFinalizer,
			))

			hostMgr.UnsetFinalizer()

			Expect(hc.ObjectMeta.Finalizers).NotTo(ContainElement(
				metal3api.HostClaimFinalizer,
			))
		},
		Entry("No finalizers", NewHostclaim(HostclaimName)),
		Entry("Additional Finalizers",
			NewHostclaim(HostclaimName, WithFinalizers{"finz"}),
		),
	)

	type testCaseSetPauseAnnotation struct {
		HostClaim           *metal3api.HostClaim
		BareMetalHost       *metal3api.BareMetalHost
		ExpectPausePresent  bool
		ExpectStatusPresent bool
		ExpectError         bool
	}

	var defaultConsumerRef = WithConsumerRef{
		Name:       HostclaimName,
		Namespace:  HostclaimNamespace,
		Kind:       HostClaimKind,
		APIVersion: metal3api.GroupVersion.String(),
	}

	var (
		defaultImage = metal3api.Image{URL: "url"}
	)

	var otherConsumerRef = WithConsumerRef{
		Name:       HostclaimName,
		Namespace:  "otherNs",
		Kind:       HostClaimKind,
		APIVersion: metal3api.GroupVersion.String(),
	}

	DescribeTable("Test Set BMH Pause Annotation",
		func(tc testCaseSetPauseAnnotation) {
			objs := []client.Object{tc.HostClaim}
			if tc.BareMetalHost != nil {
				objs = append(objs, tc.BareMetalHost)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objs...).Build()

			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)
			Expect(err).NotTo(HaveOccurred())

			err = hostMgr.SetPauseAnnotation(context.TODO())
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
				obj := map[string]interface{}{}
				err = json.Unmarshal(annotation, &obj)
				Expect(err).ToNot(HaveOccurred())
				annotation, _ = json.Marshal(obj)
				Expect(status).To(Equal(string(annotation)))
			} else {
				Expect(statusPresent).To(BeFalse())
			}
		},
		Entry("Set BMH Pause Annotation, with valid CAPM3 Paused annotations, already paused", testCaseSetPauseAnnotation{
			BareMetalHost: NewBaremetalhost(
				"bmh1", "ns1", metal3api.StateProvisioned, defaultConsumerRef,
				WithAnnotations{metal3api.PausedAnnotation: PausedAnnotationKey},
			),
			HostClaim:          NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns1/bmh1"}),
			ExpectPausePresent: true,
			ExpectError:        false,
		}),
		Entry("Set BMH Pause Annotation, with valid Paused annotations, Empty Key, already paused", testCaseSetPauseAnnotation{
			BareMetalHost: NewBaremetalhost(
				"bmh1", "ns1", metal3api.StateProvisioned, defaultConsumerRef,
				WithAnnotations{metal3api.PausedAnnotation: ""},
			),
			HostClaim:          NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns1/bmh1"}),
			ExpectPausePresent: true,
			ExpectError:        false,
		}),
		Entry("Set BMH Pause Annotation, with no Paused annotations", testCaseSetPauseAnnotation{
			BareMetalHost: NewBaremetalhost(
				"bmh1", "ns1", metal3api.StateProvisioned, defaultConsumerRef,
			),
			HostClaim:           NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns1/bmh1"}),
			ExpectPausePresent:  true,
			ExpectStatusPresent: true,
			ExpectError:         false,
		}),
		Entry("Set BMH Pause Annotation, no bmh", testCaseSetPauseAnnotation{
			BareMetalHost: nil,
			HostClaim:     NewHostclaim(HostclaimName),
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

			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)
			Expect(err).NotTo(HaveOccurred())

			err = hostMgr.RemovePauseAnnotation(context.TODO())
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
		Entry("Remove BMH Pause Annotation, with valid CAPM3 Paused annotations", testCaseRemovePauseAnnotation{
			BareMetalHost: NewBaremetalhost(
				"bmh1", "ns1", metal3api.StateProvisioned, defaultConsumerRef,
				WithAnnotations{metal3api.PausedAnnotation: PausedAnnotationKey},
			),
			HostClaim:     NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns1/bmh1"}),
			ExpectPresent: false,
			ExpectError:   false,
		}),
		Entry("Do not Remove Annotation, with valid Paused annotations, Empty Key", testCaseRemovePauseAnnotation{
			BareMetalHost: NewBaremetalhost(
				"bmh1", "ns1", metal3api.StateProvisioned, defaultConsumerRef,
				WithAnnotations{metal3api.PausedAnnotation: ""},
			),
			HostClaim:     NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns1/bmh1"}),
			ExpectPresent: true,
			ExpectError:   false,
		}),
		Entry("No Annotation, Should Not Error", testCaseRemovePauseAnnotation{
			BareMetalHost: NewBaremetalhost(
				"bmh1", "ns1", metal3api.StateProvisioned, defaultConsumerRef,
			),
			HostClaim:     NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns1/bmh1"}),
			ExpectPresent: false,
			ExpectError:   false,
		}),
		Entry("No Bmh, Should Not Error", testCaseRemovePauseAnnotation{
			BareMetalHost: nil,
			HostClaim:     NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns1/bmh1"}),
		}),
	)

	type testHasAnnotation struct {
		HostClaim *metal3api.HostClaim
		Result    bool
	}

	DescribeTable("Test HasAnnotation",
		func(tc testHasAnnotation) {
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			result := hostMgr.HasAnnotation(BareMetalHostAnnotation)
			Expect(result).To(Equal(tc.Result))
		},
		Entry("no annotation", testHasAnnotation{HostClaim: NewHostclaim(HostclaimName), Result: false}),
		Entry("other annotation", testHasAnnotation{HostClaim: NewHostclaim(HostclaimName, WithAnnotations{"other": "v"}), Result: false}),
		Entry("other annotation", testHasAnnotation{HostClaim: NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns/bmh"}), Result: true}),
	)

	var provisionedCondition = Condition{Type: metal3api.ProvisionedCondition, Status: true, Reason: "any"}

	It("test IsProvisionned", func() {
		hc1 := NewHostclaim(HostclaimName)
		hc2 := NewHostclaim(HostclaimName, provisionedCondition)
		fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
		hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, hc1, fakeClient)
		Expect(err).NotTo(HaveOccurred())
		Expect(hostMgr.IsProvisioned()).To(BeFalse())
		hostMgr, err = NewHostManager(fakeClient, GinkgoLogr, hc2, fakeClient)
		Expect(err).NotTo(HaveOccurred())
		Expect(hostMgr.IsProvisioned()).To(BeTrue())
	})

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
		hcNs                    = NewNamespace(HostclaimNamespace)
		ns1                     = NewNamespace("ns1")
		ns2                     = NewNamespace("ns2")
		hcNsLabelled            = NewNamespace(HostclaimNamespace, WithLabels{"l": "v"})
		defaultBmhLabels        = WithLabels{"default-selector": "default-value"}
		defaultFailureBmhLabels = WithLabels{"default-selector": "default-value", "infrastructure.cluster.x-k8s.io/failure-domain": "zone"}
		nodeReuseOther          = WithLabels{"default-selector": "default-value", nodeReuseLabelName: "hcNs.mdOther"}
		bmhns1                  = NewBaremetalhost("bmh1", "ns1", metal3api.StateAvailable, defaultBmhLabels)
		bmhns2                  = NewBaremetalhost("bmh2", "ns2", metal3api.StateAvailable, defaultBmhLabels)
		bmh4ns1                 = NewBaremetalhost("bmh4", "ns1", metal3api.StateAvailable, nodeReuseOther)
		bmh2ns1FailureDomain    = NewBaremetalhost("bmh2", "ns1", metal3api.StateAvailable, defaultFailureBmhLabels)
		bmhns1BadLabel          = NewBaremetalhost("nolabel-bmh1", "ns1", metal3api.StateAvailable)
		bmhns1NotAvail          = NewBaremetalhost("notavail-bmh1", "ns1", metal3api.StateRegistering, defaultBmhLabels)
		bmhns1Paused            = NewBaremetalhost("paused-bmh1", "ns1", metal3api.StateRegistering, defaultBmhLabels, WithAnnotations{metal3api.PausedAnnotation: PausedAnnotationKey})
		bmhns1Unhealthy         = NewBaremetalhost("unhealthy-bmh1", "ns1", metal3api.StateRegistering, defaultBmhLabels, WithAnnotations{UnhealthyAnnotation: ""})
		bmhns1Consumed          = NewBaremetalhost(
			"bmh-consumed", "ns1", metal3api.StateAvailable, defaultBmhLabels,
			WithConsumerRef{Kind: HostClaimKind, Namespace: HostclaimNamespace, APIVersion: metal3api.GroupVersion.String(), Name: HostclaimName})
		bmhns1ConsOther = NewBaremetalhost(
			"bmh-cons-other", "ns1", metal3api.StateAvailable, defaultBmhLabels,
			WithConsumerRef{Kind: HostClaimKind, Namespace: HostclaimNamespace, APIVersion: metal3api.GroupVersion.String(), Name: "other"})
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
					objects = append(objects, NewHardwareData(obj))
				}
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			bmh, _, err := hostMgr.chooseBMH(context.TODO())
			if tc.ExpectedBmhName == "" {
				Expect(bmh).To(BeNil())
				if tc.ExpectFail {
					Expect(err).To(HaveOccurred())
					var requeueAfterError HasRequeueAfterError
					Expect(errors.As(err, &requeueAfterError)).To(BeFalse())
				} else if tc.ExpectRequeue {
					Expect(err).To(HaveOccurred())
					var requeueAfterError HasRequeueAfterError
					Expect(errors.As(err, &requeueAfterError)).To(BeTrue())
				} else {
					Expect(err).NotTo(HaveOccurred())
				}
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(bmh).NotTo(BeNil())
				Expect(bmh.Name).To(Equal(tc.ExpectedBmhName))
			}
		},
		Entry("no policies", testCaseChooseBMH{
			HostClaim:      NewHostclaim(HostclaimName),
			Namespaces:     []*corev1.Namespace{hcNs, ns1, ns2},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1, bmhns2},
		}),
		Entry("bad namespace name", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName),
			Namespaces: []*corev1.Namespace{hcNs, ns1, ns2},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{"otherNs"})},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1, bmhns2},
		}),
		Entry("with policy in ns1", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName),
			Namespaces: []*corev1.Namespace{hcNs, ns1, ns2},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "bmh1",
		}),
		Entry("HostClaim targets ns1 (positive)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName, WithTargetNamespace("ns1")),
			Namespaces: []*corev1.Namespace{hcNs, ns1, ns2},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace}),
				NewHostdeploypolicy("hdp", "ns2", AcceptNames{HostclaimNamespace})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "bmh1",
		}),
		Entry("HostClaim targets ns1 (negative)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName, WithTargetNamespace("ns1")),
			Namespaces: []*corev1.Namespace{hcNs, ns1, ns2},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace}),
				NewHostdeploypolicy("hdp", "ns2", AcceptNames{HostclaimNamespace})},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns2},
		}),
		Entry("with criteriums (positive)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName, WithLabelSelector{"default-selector": "default-value"}),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail, bmhns1Paused, bmhns1Unhealthy, bmh4ns1},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with criteriums (negative)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName, WithLabelSelector{"default-selector": "default-value"}),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail, bmhns1Paused, bmhns1Unhealthy, bmh4ns1},
		}),
		Entry("with bad label value", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName, WithLabelSelector{"default-selector": "other-value"}),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail, bmhns1Paused, bmhns1Unhealthy},
			ExpectedBmhName: "",
		}),
		Entry("with eroneous labels", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName, WithLabelSelector{"*/123": "default-value"}),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail, bmhns1Paused, bmhns1Unhealthy},
			ExpectFail:     true,
		}),
		Entry("with consumerRef", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1Consumed, bmhns1, bmhns1ConsOther},
			ExpectedBmhName: "bmh-consumed",
		}),
		Entry("with expr (positive)", testCaseChooseBMH{
			HostClaim: NewHostclaim(
				HostclaimName,
				WithMatchExprSelector{{
					Key: "default-selector", Values: []string{"other", "default-value"},
					Operator: selection.In}}),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns1BadLabel, bmhns1ConsOther, bmhns1NotAvail},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with labeled hostclaim namespace", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName),
			Namespaces: []*corev1.Namespace{hcNsLabelled, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptLabels{{Name: "l", Value: "v"}})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with labeled hostclaim namespace (bad label)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName),
			Namespaces: []*corev1.Namespace{hcNsLabelled, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptLabels{{Name: "l", Value: "w"}})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "",
		}),
		Entry("with labeled hostclaim namespace (negative)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptLabels{{Name: "l", Value: "v"}})},
			BareMetalHosts: []*metal3api.BareMetalHost{bmhns1, bmhns2},
		}),
		Entry("with regexp on hostclaim namespace", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName),
			Namespaces: []*corev1.Namespace{hcNsLabelled, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptRegexp("hc.*"))},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "bmh1",
		}),
		Entry("with bad regexp on hostclaim namespace", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName),
			Namespaces: []*corev1.Namespace{hcNsLabelled, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptRegexp("hc["))},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmhns2},
			ExpectedBmhName: "",
			ExpectFail:      true,
		}),
		Entry("with Failure Domain (available bmh)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName, WithLabelSelector{"default-selector": "default-value"}, WithFailureDomain("zone")),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
			BareMetalHosts:  []*metal3api.BareMetalHost{bmhns1, bmh2ns1FailureDomain},
			ExpectedBmhName: "bmh2",
		}),
		Entry("with Failure Domain (no available bmh in zone)", testCaseChooseBMH{
			HostClaim:  NewHostclaim(HostclaimName, WithLabelSelector{"default-selector": "default-value"}, WithFailureDomain("zone")),
			Namespaces: []*corev1.Namespace{hcNs, ns1},
			HostDeployPolicies: []*metal3api.HostDeployPolicy{
				NewHostdeploypolicy("hdp", "ns1", AcceptNames{HostclaimNamespace})},
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
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable)
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
			oldBmh.ResourceVersion = "234"
			oldBmh.Spec.Description = "v2"
			err = hideConflictError(helper.Patch(ctx, oldBmh))
			Expect(err).To(HaveOccurred(), "Conflict error becomes requeue")
			var requeueAfterError HasRequeueAfterError
			Expect(errors.As(err, &requeueAfterError)).To(BeTrue())
		})

	DescribeTable("test Associate",
		func(tc testCaseAssociate) {
			sec := NewSecret("sec-user-data", HostclaimNamespace, WithData{"user-data": []byte("v")})
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable)
			objects := []client.Object{
				tc.HostClaim, bmh, sec,
				NewHostdeploypolicy("hdp", "ns", AcceptNames{HostclaimNamespace}),
				NewNamespace("hcNs"), NewNamespace("ns"),
			}
			// We patch the status during associate to set the annotation.
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).WithStatusSubresource(tc.HostClaim).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, tc.HostClaim, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			err = hostMgr.Associate(context.TODO())
			if tc.ExpectFails {
				Expect(err).To(HaveOccurred())
				var requeueAfterError HasRequeueAfterError
				Expect(errors.As(err, &requeueAfterError)).To(Equal(tc.ExpectRequeue))
				return
			}
			Expect(err).NotTo(HaveOccurred())
		},
		Entry("Regular case", testCaseAssociate{HostClaim: NewHostclaim(HostclaimName,
			WithImage{Image: defaultImage},
			WithUserData("sec-user-data"),
		)}),
		Entry("Bad Selector, True failure", testCaseAssociate{
			HostClaim: NewHostclaim(HostclaimName,
				WithMatchExprSelector{metal3api.HostSelectorRequirement{
					Key: "k", Operator: selection.Exists, Values: []string{"a", "b"}}}),
			ExpectFails: true,
		}),
		Entry("Incompatible selector", testCaseAssociate{
			HostClaim: NewHostclaim(HostclaimName,
				WithMatchExprSelector{metal3api.HostSelectorRequirement{
					Key: "k", Operator: selection.Exists, Values: []string{}}}),
			ExpectFails:   true,
			ExpectRequeue: true,
		}),
	)

	type testCaseSetBMHSpec struct {
		UserData        *corev1.Secret
		NetworkData     *corev1.Secret
		MetaData        *corev1.Secret
		BMHUserData     *corev1.Secret
		BMHNetworkData  *corev1.Secret
		BMHMetaData     *corev1.Secret
		SetImage        bool
		SetCustomDeploy bool
		SetPoweredOn    bool
	}

	DescribeTable("Test setBMHspec",
		func(tc testCaseSetBMHSpec) {
			hcOptions := []HostclaimOption{}
			ctx := context.TODO()
			objects := []client.Object{}
			numSecrets := 0
			if tc.UserData != nil {
				hcOptions = append(hcOptions, WithUserData(tc.UserData.Name))
				if !strings.HasPrefix(tc.UserData.Name, "removed") {
					objects = append(objects, tc.UserData)
					numSecrets++
				}
			}
			if tc.MetaData != nil {
				hcOptions = append(hcOptions, WithMetaData(tc.MetaData.Name))
				if !strings.HasPrefix(tc.MetaData.Name, "removed") {
					objects = append(objects, tc.MetaData)
					numSecrets++
				}
			}
			if tc.NetworkData != nil {
				hcOptions = append(hcOptions, WithNetworkData(tc.NetworkData.Name))
				if !strings.HasPrefix(tc.MetaData.Name, "removed") {
					objects = append(objects, tc.NetworkData)
					numSecrets++
				}
			}
			if tc.SetImage {
				hcOptions = append(hcOptions, WithImage{Image: defaultImage})
			}
			if tc.SetCustomDeploy {
				hcOptions = append(hcOptions, WithCustomDeploy("custom"))
			}
			hostClaim := NewHostclaim(HostclaimName, hcOptions...)
			bmhOptions := []BaremetalhostOption{}
			if tc.BMHUserData != nil {
				bmhOptions = append(bmhOptions, WithUserData(tc.BMHUserData.Name))
				objects = append(objects, tc.BMHUserData)
			}
			if tc.BMHMetaData != nil {
				bmhOptions = append(bmhOptions, WithMetaData(tc.BMHMetaData.Name))
				objects = append(objects, tc.BMHMetaData)
			}
			if tc.BMHNetworkData != nil {
				bmhOptions = append(bmhOptions, WithNetworkData(tc.BMHNetworkData.Name))
				objects = append(objects, tc.BMHNetworkData)
			}
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable, bmhOptions...)
			objects = append(objects, hostClaim, bmh)
			// Add secrets if they exists
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, hostClaim, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			err = hostMgr.setBmhSpec(ctx, bmh)
			errorExpected := false
			var checkSecret = func(ref *corev1.SecretReference, source *corev1.Secret, message string) {
				if source == nil {
					Expect(ref).To(BeNil(), message)
				} else if strings.HasPrefix(source.Name, "removed") {
					Expect(ref).To(BeNil(), message)
					errorExpected = true
				} else {
					Expect(ref).NotTo(BeNil(), message)
					sec := &corev1.Secret{}
					key := client.ObjectKey{Name: ref.Name, Namespace: "ns"}
					err = fakeClient.Get(ctx, key, sec)
					Expect(err).NotTo(HaveOccurred(), message)
					Expect(reflect.DeepEqual(sec.Data, source.Data)).To(BeTrue(), message)
				}
			}
			checkSecret(bmh.Spec.UserData, tc.UserData, "userdata coherence")
			checkSecret(bmh.Spec.MetaData, tc.MetaData, "metadata coherence")
			checkSecret(bmh.Spec.NetworkData, tc.NetworkData, "networkdata coherence")
			if errorExpected {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			secrets := &corev1.SecretList{}
			err = fakeClient.List(ctx, secrets, client.InNamespace("ns"))
			Expect(err).NotTo(HaveOccurred())
			Expect(secrets.Items).To(HaveLen(numSecrets))
			if tc.SetImage {
				Expect(bmh.Spec.Image).NotTo(BeNil())
				Expect(*bmh.Spec.Image).To(Equal(defaultImage))
			}
			if tc.SetCustomDeploy {
				Expect(bmh.Spec.CustomDeploy).NotTo(BeNil())
				Expect(bmh.Spec.CustomDeploy.Method).To(Equal("custom"))
			}
		},
		Entry("set user-data (initialize)", testCaseSetBMHSpec{
			UserData: NewSecret("s1", HostclaimNamespace, WithData{"f": []byte("udt")}),
		}),
		Entry("set user-data (override)", testCaseSetBMHSpec{
			UserData:    NewSecret("s1", HostclaimNamespace, WithData{"f": []byte("udt")}),
			BMHUserData: NewSecret("bmh-userdata", "ns", WithData{"f": []byte("other")}),
		}),
		Entry("reset user-data (override)", testCaseSetBMHSpec{
			BMHUserData: NewSecret("bmh-userdata", "ns", WithData{"f": []byte("other")}),
		}),
		Entry("set meta-data/network-data (initialize)", testCaseSetBMHSpec{
			MetaData:    NewSecret("s1", HostclaimNamespace, WithData{"f": []byte("mdt")}),
			NetworkData: NewSecret("s2", HostclaimNamespace, WithData{"f": []byte("nwdt")}),
		}),
		Entry("set meta-data/network-data (overide)", testCaseSetBMHSpec{
			MetaData:       NewSecret("s1", HostclaimNamespace, WithData{"f": []byte("mdt")}),
			NetworkData:    NewSecret("s2", HostclaimNamespace, WithData{"f": []byte("nwdt")}),
			BMHMetaData:    NewSecret("bmh-metadata", "ns", WithData{"f": []byte("other")}),
			BMHNetworkData: NewSecret("bmh-networkdata", "ns", WithData{"f": []byte("other")}),
		}),
		Entry("reset meta-data/network-data (overide)", testCaseSetBMHSpec{
			BMHMetaData:    NewSecret("bmh-metadata", "ns", WithData{"f": []byte("other")}),
			BMHNetworkData: NewSecret("bmh-networkdata", "ns", WithData{"f": []byte("other")}),
		}),
		Entry("set meta-data (initialize/not yet available)", testCaseSetBMHSpec{
			MetaData: NewSecret("removed-secret", HostclaimNamespace),
		}),
		Entry("set image", testCaseSetBMHSpec{
			SetImage: true,
		}),
		Entry("set custom deploy", testCaseSetBMHSpec{
			SetCustomDeploy: true,
		}),
	)

	type testCaseGetBMH struct {
		HostClaim     *metal3api.HostClaim
		BareMetalHost *metal3api.BareMetalHost
		ExpectFails   bool
	}

	var (
		hcDefault  = NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns/bmh"})
		hcNoAnnot  = NewHostclaim(HostclaimName, WithAnnotations{})
		hcBadAnnot = NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns/bmh/other"})
	)
	DescribeTable("Test getBMH",
		func(tc testCaseGetBMH) {
			hc := tc.HostClaim.DeepCopy()
			objs := []client.Object{hc}
			if tc.BareMetalHost != nil {
				objs = append(objs, tc.BareMetalHost)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objs...).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, hc, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			bmh, _, err := hostMgr.getBmh(context.TODO())
			if tc.ExpectFails {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(bmh.Name).To(Equal("bmh"))
			}

		},
		Entry("no bmh", testCaseGetBMH{HostClaim: hcDefault, BareMetalHost: nil, ExpectFails: true}),
		Entry("bmh no consumer ref",
			testCaseGetBMH{
				HostClaim:     hcDefault,
				BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable),
				ExpectFails:   true}),
		Entry("bmh bad consumer ref",
			testCaseGetBMH{
				HostClaim:     hcDefault,
				BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable, otherConsumerRef),
				ExpectFails:   true}),
		Entry("bmh with righ consumer ref",
			testCaseGetBMH{
				HostClaim:     hcDefault,
				BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable, defaultConsumerRef)}),
		Entry("hostclaim no annotation",
			testCaseGetBMH{
				HostClaim:     hcNoAnnot,
				BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable, defaultConsumerRef),
				ExpectFails:   true}),
		Entry("hostclaim bad annotation",
			testCaseGetBMH{
				HostClaim:     hcBadAnnot,
				BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable, defaultConsumerRef),
				ExpectFails:   true}),
	)

	DescribeTable("Test updateHostClaimStatus",
		func(state metal3api.ProvisioningState, provisionned, available bool) {
			hostClaim := NewHostclaim(
				HostclaimName,
				WithAnnotations{BareMetalHostAnnotation: "ns/bmh"},
			)
			bmh := NewBaremetalhost("bmh", "ns", state)
			bmh.Status.PoweredOn = true
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, hostClaim, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			hostMgr.updateHostClaimStatus(bmh)
			Expect(hostClaim.Status.PoweredOn).To(BeTrue())
			Expect(hostClaim.Status.HardwareData).NotTo(BeNil())
			Expect(hostClaim.Status.HardwareData.Name).To(Equal("bmh"))
			Expect(hostClaim.Status.HardwareData.Namespace).To(Equal("ns"))
			if provisionned {
				Expect(conditions.IsTrue(hostClaim, metal3api.ProvisionedCondition)).To(BeTrue())
			} else {
				Expect(conditions.IsFalse(hostClaim, metal3api.ProvisionedCondition)).To(BeTrue())
			}
			if available {
				Expect(conditions.IsTrue(hostClaim, metal3api.AvailableCondition)).To(BeTrue())
			} else {
				Expect(conditions.IsFalse(hostClaim, metal3api.AvailableCondition)).To(BeTrue())
			}

		},
		Entry("provisioned", metal3api.StateProvisioned, true, false),
		Entry("available", metal3api.StateAvailable, false, true),
		Entry("provisioning", metal3api.StateProvisioning, false, false),
		Entry("inspecting", metal3api.StateInspecting, false, false),
		Entry("other bmh state", metal3api.StateDeprovisioning, false, false),
	)

	It("test syncReboot",
		func() {
			opt := map[string]string{"a": "w1"}
			saved := maps.Clone(opt)
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable, WithAnnotations(opt))
			annot := rebootDomain + "/test"
			hostClaim := NewHostclaim(
				HostclaimName,
				WithAnnotations{BareMetalHostAnnotation: "ns/bmh"},
			)
			hostClaim.Annotations[annot] = "v0"
			syncReboot(hostClaim.Annotations, bmh.Annotations)
			Expect(bmh.Annotations[annot]).To(Equal("v0"))
			// Remove reboot/stop Annotation
			delete(hostClaim.Annotations, annot)
			syncReboot(hostClaim.Annotations, bmh.Annotations)
			Expect(maps.Equal(bmh.Annotations, saved)).To(BeTrue())
			// Set transient reboot. Propagation erase it.
			hostClaim.Annotations[rebootDomain] = "v1"
			syncReboot(hostClaim.Annotations, bmh.Annotations)
			_, ok := hostClaim.Annotations[rebootDomain]
			Expect(ok).To(BeFalse())
			// Check reboot propagated to save
			if saved == nil {
				saved = map[string]string{}
			}
			saved[rebootDomain] = "v1"
			Expect(maps.Equal(bmh.Annotations, saved)).To(BeTrue())
		},
	)

	It("test Patch if found",
		func() {
			ctx := context.TODO()
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable)
			oldBmh := bmh.DeepCopy()
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(bmh).Build()
			bmh.Spec.Description = "v0"
			err := fakeClient.Update(ctx, bmh)
			Expect(err).NotTo(HaveOccurred())
			helper, err := patch.NewHelper(bmh, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			bmh.Spec.Description = "v1"
			err = patchIfFound(ctx, helper, bmh)
			Expect(err).NotTo(HaveOccurred(), "Patch succeeds")
			helper, err = patch.NewHelper(oldBmh, fakeClient)
			oldBmh.ResourceVersion = "234"
			oldBmh.Spec.Description = "v2"
			err = patchIfFound(ctx, helper, oldBmh)
			Expect(err).To(HaveOccurred(), "Conflict error becomes requeue")
			var requeueAfterError HasRequeueAfterError
			Expect(errors.As(err, &requeueAfterError)).To(BeTrue())
			err = fakeClient.Delete(ctx, bmh)
			Expect(err).NotTo(HaveOccurred())
			helper, err = patch.NewHelper(bmh, fakeClient)
			bmh.Spec.Description = "v2"
			err = patchIfFound(ctx, helper, bmh)
			Expect(err).NotTo(HaveOccurred(), "Not found is ignored")
		})

	type testCaseUpdate struct {
		HostClaim  *metal3api.HostClaim
		ExpectFail bool
	}
	DescribeTable("test Update",
		func(tc testCaseUpdate) {
			ctx := context.TODO()
			hc := tc.HostClaim
			bmh := NewBaremetalhost("bmh", "ns", metal3api.StateAvailable, defaultConsumerRef)
			objects := []client.Object{
				hc, bmh,
				NewHostdeploypolicy("hdp", "ns", AcceptNames{HostclaimNamespace}),
				NewNamespace("hcNs"), NewNamespace("ns"),
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, hc, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			err = hostMgr.Update(ctx)
			if tc.ExpectFail {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
		},
		Entry("Regular case", testCaseUpdate{HostClaim: NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns/bmh"})}),
		Entry("Bad annotation fail BMH", testCaseUpdate{HostClaim: NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "a/b/c"}), ExpectFail: true}),
		Entry("no bmh", testCaseUpdate{HostClaim: NewHostclaim(HostclaimName, WithAnnotations{BareMetalHostAnnotation: "ns/other"}), ExpectFail: true}),
	)

	type testCaseDelete struct {
		BareMetalHost *metal3api.BareMetalHost
		ExpectRequeue bool
		SecretCleared bool
	}
	DescribeTable("Test Delete",
		func(tc testCaseDelete) {
			ctx := context.TODO()
			hcOpts := []HostclaimOption{
				WithUserData("sec1"), WithMetaData("sec2"), WithNetworkData("sec3"),
				WithAnnotations{BareMetalHostAnnotation: "ns/bmh"},
			}
			hc := NewHostclaim(HostclaimName, hcOpts...)
			objects := []client.Object{
				hc,
				NewHostdeploypolicy("hdp", "ns", AcceptNames{HostclaimNamespace}),
				NewNamespace("hcNs"), NewNamespace("ns"),
			}
			if tc.BareMetalHost != nil {
				bmh := tc.BareMetalHost
				objects = append(objects, bmh)
				for _, ref := range []*corev1.SecretReference{bmh.Spec.UserData, bmh.Spec.MetaData, bmh.Spec.NetworkData} {
					if ref != nil && !tc.SecretCleared {
						sec := NewSecret(ref.Name, "ns", WithData{"data": []byte("v")})
						objects = append(objects, sec)
					}
				}
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			hostMgr, err := NewHostManager(fakeClient, GinkgoLogr, hc, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			err = hostMgr.Delete(ctx)
			bmhOut := &metal3api.BareMetalHost{}
			if tc.BareMetalHost == nil {
				return
			}
			e2 := fakeClient.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "bmh"}, bmhOut)
			Expect(e2).NotTo(HaveOccurred())
			if tc.ExpectRequeue {
				Expect(err).To(HaveOccurred())
				var requeueAfterError HasRequeueAfterError
				Expect(errors.As(err, &requeueAfterError)).To(BeTrue())
				Expect(bmhOut.Spec.Image).To(BeNil())
				Expect(bmhOut.Spec.UserData).To(BeNil())
				Expect(bmhOut.Spec.MetaData).To(BeNil())
				Expect(bmhOut.Spec.NetworkData).To(BeNil())
				Expect(bmhOut.Spec.ConsumerRef).NotTo(BeNil(), "Access to bmh is kept")
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(bmhOut.Spec.ConsumerRef).To(BeNil(), "Access to bmh is revoked")
				Expect(bmhOut.Spec.Online).To(BeFalse(), "host is offline")
			}
		},
		Entry("First step remove secrets", testCaseDelete{
			BareMetalHost: NewBaremetalhost(
				"bmh", "ns", metal3api.StateAvailable, defaultConsumerRef,
				WithUserData("bmh-user-data"), WithMetaData("bmh-meta-data"), WithNetworkData("bmh-network-data"),
				PoweredOnTrue{},
				WithImage{Image: defaultImage}),
			ExpectRequeue: true,
		}),
		Entry("First step remove secrets", testCaseDelete{
			BareMetalHost: NewBaremetalhost(
				"bmh", "ns", metal3api.StateAvailable, defaultConsumerRef,
				WithUserData("bmh-user-data"),
				PoweredOnTrue{},
				WithImage{Image: defaultImage}),
			ExpectRequeue: true,
			SecretCleared: true,
		}),
		Entry("Second step, wait for deprovisioning", testCaseDelete{
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateProvisioned, defaultConsumerRef),
			ExpectRequeue: true,
		}),
		Entry("Last step, cleanup", testCaseDelete{
			BareMetalHost: NewBaremetalhost("bmh", "ns", metal3api.StateAvailable, defaultConsumerRef, WithCleaningMode("metadata")),
		}),
		Entry("No BMH", testCaseDelete{
			BareMetalHost: nil,
		}),
	)
})

func TestManagers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Manager Suite")
}
