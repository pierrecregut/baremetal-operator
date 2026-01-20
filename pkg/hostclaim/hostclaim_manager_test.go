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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
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

})

func TestManagers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Manager Suite")
}
