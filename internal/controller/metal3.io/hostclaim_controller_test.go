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

package controllers

import (
	"context"
	"errors"
	"testing"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/metal3-io/baremetal-operator/internal/testing"
	hostclaimPkg "github.com/metal3-io/baremetal-operator/pkg/hostclaim"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

type testCaseHostClaimReconcile struct {
	HostClaim             *metal3api.HostClaim
	BareMetalHost         *metal3api.BareMetalHost
	AssociationReason     string
	SynchronizationReason string
	HasFinalizer          bool
	DeleteRequested       bool
	// Expect an error that is not a Requeue
	ExpectError          bool
	ExpectRequeue        bool
	ExpectDeleteSucceeds bool
	ExpectReady          bool
	WithErrorAfter       int
}

const (
	baremetalNamespace = "ns"
	baremetalName      = "bmh"
)

var defaultConsumerRef = WithConsumerRef{
	Name:       HostclaimName,
	Namespace:  HostclaimNamespace,
	Kind:       "HostClaim",
	APIVersion: metal3api.GroupVersion.String(),
}

var provisionedCondition = Condition{Type: metal3api.ProvisionedCondition, Status: true, Reason: "any"}

func newHostClaimRequest(hostclaim *metal3api.HostClaim) ctrl.Request {
	namespacedName := types.NamespacedName{
		Namespace: hostclaim.Namespace,
		Name:      hostclaim.Name,
	}
	return ctrl.Request{NamespacedName: namespacedName}
}

var _ = Describe("Test HostClaim Controller",
	func() {
		DescribeTable("Test Reconcile",
			func(tc testCaseHostClaimReconcile) {
				ctx := context.TODO()
				hdp := NewHostdeploypolicy("hdp", baremetalNamespace, AcceptNames{HostclaimNamespace})
				ns1 := NewNamespace(HostclaimNamespace)
				ns2 := NewNamespace(baremetalNamespace)
				objs := []client.Object{tc.HostClaim, hdp, ns1, ns2}
				key := types.NamespacedName{Name: HostclaimName, Namespace: HostclaimNamespace}
				if tc.BareMetalHost != nil {
					objs = append(objs, tc.BareMetalHost)
				}
				scheme := setupScheme()
				builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(tc.HostClaim)
				if tc.WithErrorAfter > 0 {
					builder = builder.WithInterceptorFuncs(
						interceptor.Funcs{
							Patch: func(ctx context.Context, client client.WithWatch, obj client.Object,
								patch client.Patch, opts ...client.PatchOption) error {
								if tc.WithErrorAfter > 0 {
									tc.WithErrorAfter--
									if tc.WithErrorAfter == 0 {
										return k8serrors.NewConflict(schema.ParseGroupResource("metal3.io/hostclaims"), obj.GetName(), errors.New("conflict"))
									}
								}
								return client.Patch(ctx, obj, patch, opts...)
							}})
				}
				fakeClient := builder.Build()
				if tc.DeleteRequested {
					hostClaim := &metal3api.HostClaim{}
					err := fakeClient.Get(ctx, key, hostClaim)
					Expect(err).NotTo(HaveOccurred())
					err = fakeClient.Delete(ctx, hostClaim)
					Expect(err).NotTo(HaveOccurred())
				}
				req := newHostClaimRequest(tc.HostClaim)
				hostclaimCtrl := HostClaimReconciler{
					Client: fakeClient, Scheme: scheme, Log: GinkgoLogr, APIReader: fakeClient}
				result, err := hostclaimCtrl.Reconcile(ctx, req)
				if tc.ExpectError {
					Expect(err).To(HaveOccurred())
				} else {
					Expect(err).NotTo(HaveOccurred())
					if tc.ExpectRequeue {
						Expect(result.RequeueAfter).NotTo(BeZero())
					} else {
						Expect(result.RequeueAfter).To(BeZero())
					}
				}
				hostClaim := &metal3api.HostClaim{}
				if tc.DeleteRequested {
					hostClaim := &metal3api.HostClaim{}
					err := fakeClient.Get(ctx, key, hostClaim)
					if tc.ExpectDeleteSucceeds {
						Expect(err).To(HaveOccurred())
					} else {
						Expect(err).NotTo(HaveOccurred())
						Expect(hostClaim.Finalizers).To(HaveLen(1))
					}
					return
				}
				err = fakeClient.Get(ctx, key, hostClaim)
				Expect(err).NotTo(HaveOccurred())
				if tc.HasFinalizer {
					Expect(hostClaim.Finalizers).To(HaveLen(1))
				}
				switch tc.AssociationReason {
				case "":
				case metal3api.BareMetalHostAssociatedReason:
					Expect(conditions.IsTrue(hostClaim, metal3api.AssociatedCondition)).To(BeTrue())
				default:
					Expect(conditions.IsFalse(hostClaim, metal3api.AssociatedCondition)).To(BeTrue())
					Expect(conditions.GetReason(hostClaim, metal3api.AssociatedCondition)).To(Equal(tc.AssociationReason))
				}
				switch tc.SynchronizationReason {
				case "":
				case metal3api.ConfigurationSyncedReason:
					Expect(conditions.IsTrue(hostClaim, metal3api.SynchronizedCondition)).To(BeTrue())
				default:
					Expect(conditions.IsFalse(hostClaim, metal3api.SynchronizedCondition)).To(BeTrue())
					Expect(conditions.GetReason(hostClaim, metal3api.SynchronizedCondition)).To(Equal(tc.SynchronizationReason))
				}
				Expect(conditions.IsTrue(hostClaim, "Ready")).To(Equal(tc.ExpectReady))

			},
			Entry("Starting Reconciliation with a non ready BMH (no association)", testCaseHostClaimReconcile{
				HostClaim:         NewHostclaim(HostclaimName),
				BareMetalHost:     NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateRegistering),
				ExpectRequeue:     true,
				AssociationReason: metal3api.NoBareMetalHostReason,
			}),
			Entry("Starting Reconciliation with a ready BMH", testCaseHostClaimReconcile{
				HostClaim:         NewHostclaim(HostclaimName, WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "ns/bmh"}),
				BareMetalHost:     NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef),
				AssociationReason: metal3api.BareMetalHostAssociatedReason,
				HasFinalizer:      true,
			}),
			Entry("Starting Reconciliation with a ready BMH but not all secrets", testCaseHostClaimReconcile{
				HostClaim:             NewHostclaim(HostclaimName, WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "ns/bmh"}, WithUserData("sec-not-provided")),
				BareMetalHost:         NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef),
				AssociationReason:     metal3api.BareMetalHostAssociatedReason,
				SynchronizationReason: metal3api.BadUserDataSecretReason,
				ExpectRequeue:         true,
				HasFinalizer:          true,
			}),
			Entry("Updating an associated BMH", testCaseHostClaimReconcile{
				HostClaim:             NewHostclaim(HostclaimName, WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "ns/bmh"}),
				BareMetalHost:         NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateProvisioned, defaultConsumerRef),
				AssociationReason:     metal3api.BareMetalHostAssociatedReason,
				SynchronizationReason: metal3api.ConfigurationSyncedReason,
				HasFinalizer:          true,
				ExpectReady:           true,
			}),
			Entry("Updating a ready BMH", testCaseHostClaimReconcile{
				HostClaim:             NewHostclaim(HostclaimName, WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "ns/bmh"}, provisionedCondition),
				BareMetalHost:         NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateProvisioned, defaultConsumerRef),
				AssociationReason:     metal3api.BareMetalHostAssociatedReason,
				SynchronizationReason: metal3api.ConfigurationSyncedReason,
				HasFinalizer:          true,
				ExpectReady:           true,
			}),
			Entry("Pausing an associated BMH", testCaseHostClaimReconcile{
				HostClaim:         NewHostclaim(HostclaimName, WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "ns/bmh", metal3api.PausedAnnotation: ""}),
				BareMetalHost:     NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef),
				AssociationReason: metal3api.HostPausedReason,
			}),
			Entry("Fail to pause an associated BMH (simulated error)", testCaseHostClaimReconcile{
				HostClaim:         NewHostclaim(HostclaimName, WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "a/b/c", metal3api.PausedAnnotation: ""}),
				BareMetalHost:     NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef),
				AssociationReason: metal3api.PauseAnnotationSetFailedReason,
			}),
			// Note: slightly missleading in this case as the problem is the access to BMH
			Entry("Bad access to BMH (simulated error)", testCaseHostClaimReconcile{
				HostClaim:         NewHostclaim(HostclaimName, WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "a/b/c"}),
				BareMetalHost:     NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef),
				AssociationReason: metal3api.PauseAnnotationRemoveFailedReason,
			}),
			Entry("Deletion standard case", testCaseHostClaimReconcile{
				HostClaim: NewHostclaim(
					HostclaimName,
					WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "ns/bmh"},
					WithFinalizers{metal3api.HostClaimFinalizer}),
				BareMetalHost: NewBaremetalhost(
					baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef, WithCleaningMode("metadata")),
				DeleteRequested:      true,
				ExpectDeleteSucceeds: true,
			}),
			Entry("Deletion cleaningMode reset", testCaseHostClaimReconcile{
				HostClaim: NewHostclaim(
					HostclaimName,
					WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "ns/bmh"},
					WithFinalizers{metal3api.HostClaimFinalizer}),
				BareMetalHost: NewBaremetalhost(
					baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef),
				DeleteRequested:      true,
				ExpectRequeue:        true,
				ExpectDeleteSucceeds: false,
			}),
			Entry("Starting Reconciliation with a ready BMH (conflict on bmh update)", testCaseHostClaimReconcile{
				HostClaim:         NewHostclaim(HostclaimName),
				BareMetalHost:     NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable),
				AssociationReason: metal3api.BareMetalHostNotSynchronizedReason,
				ExpectRequeue:     true,
				WithErrorAfter:    1,
			}),
			// There is an error but nothing is changed in hostclaim (no condition)
			Entry("Starting Reconciliation with a ready BMH (conflict on annotation)", testCaseHostClaimReconcile{
				HostClaim:      NewHostclaim(HostclaimName),
				BareMetalHost:  NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable),
				ExpectError:    true,
				WithErrorAfter: 2,
			}),
			// should we hide conflict errors ? other than logs, it has no clear impact.
			Entry("Starting Reconciliation with a ready BMH (conflict on final hostclaim patch)", testCaseHostClaimReconcile{
				HostClaim:         NewHostclaim(HostclaimName),
				BareMetalHost:     NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable),
				AssociationReason: metal3api.BareMetalHostAssociatedReason,
				ExpectError:       true,
				WithErrorAfter:    3,
			}),
			Entry("Deletion with conflict", testCaseHostClaimReconcile{
				HostClaim: NewHostclaim(
					HostclaimName,
					WithAnnotations{hostclaimPkg.BareMetalHostAnnotation: "ns/bmh"},
					WithFinalizers{metal3api.HostClaimFinalizer}),
				BareMetalHost: NewBaremetalhost(
					baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef),
				DeleteRequested:      true,
				WithErrorAfter:       1,
				ExpectRequeue:        true,
				ExpectDeleteSucceeds: false,
			}),
		)

		It("Test BareMetalHostToHostClaims (with consumer ref)", func() {
			reqList := BareMetalHostToHostClaims(context.TODO(), NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable, defaultConsumerRef))
			Expect(reqList).To(HaveLen(1))
			Expect(reqList[0].Name).To(Equal(HostclaimName))
			Expect(reqList[0].Namespace).To(Equal(HostclaimNamespace))
		})

		It("Test BareMetalHostToHostClaims (no consumer ref)", func() {
			reqList := BareMetalHostToHostClaims(context.TODO(), NewBaremetalhost(baremetalName, baremetalNamespace, metal3api.StateAvailable))
			Expect(reqList).To(HaveLen(0))
		})
	},
)

func TestManagers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Manager Suite")
}
