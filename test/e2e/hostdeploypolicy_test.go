//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"path"
	"time"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func checkNoAssociation(specName string, hostClaim *metal3api.HostClaim) {
	currentHC := metal3api.HostClaim{}
	key := client.ObjectKeyFromObject(hostClaim)
	Expect(clusterProxy.GetClient().Get(ctx, key, &currentHC)).To(Succeed())
	Expect(currentHC.Status.BareMetalHost).To(BeNil())
}

func createEmptyHostclaim(namespace string) *metal3api.HostClaim {
	hostClaim := &metal3api.HostClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim",
			Namespace: namespace,
		},
		Spec: metal3api.HostClaimSpec{
			HostSelector: metal3api.HostSelector{
				MatchLabels: map[string]string{selectorLabel: selectorValue},
			},
		},
	}
	err := clusterProxy.GetClient().Create(ctx, hostClaim)
	Expect(err).NotTo(HaveOccurred())
	return hostClaim
}

var _ = Describe("Restrict HostClaims association with HostDeployPolicies.", Label("required", "hostclaim"),
	func() {
		const defaultDelay = 5 * time.Second
		var (
			specName        = "hdp"
			secretName      = "bmc-credentials"
			namespaceBMH    *corev1.Namespace
			namespaceClaimA *corev1.Namespace
			namespaceClaimB *corev1.Namespace
			cancelWatches   context.CancelFunc
			toCleanupBMH    []client.Object
			toCleanupClaimA []client.Object
			toCleanupClaimB []client.Object
		)

		BeforeEach(func() {
			toCleanupBMH = nil
			toCleanupClaimA = nil
			toCleanupClaimB = nil
			namespaceBMH, cancelWatches = framework.CreateNamespaceAndWatchEvents(ctx, framework.CreateNamespaceAndWatchEventsInput{
				Creator:             clusterProxy.GetClient(),
				ClientSet:           clusterProxy.GetClientSet(),
				Name:                specName + "-infra",
				LogFolder:           artifactFolder,
				IgnoreAlreadyExists: true,
			})
			namespaceClaimA = framework.CreateNamespace(ctx, framework.CreateNamespaceInput{
				Creator:             clusterProxy.GetClient(),
				Name:                specName + "-tenanta",
				Labels:              map[string]string{"tenant": "a"},
				IgnoreAlreadyExists: true,
			})
			namespaceClaimB = framework.CreateNamespace(ctx, framework.CreateNamespaceInput{
				Creator:             clusterProxy.GetClient(),
				Name:                specName + "-tenantb",
				Labels:              map[string]string{"tenant": "b"},
				IgnoreAlreadyExists: true,
			})
		})

		It("Create a claim, associate it to a BMH", func() {
			By("Creating a secret with BMH credentials")
			bmcCredentialsData := map[string]string{
				"username": bmc.User,
				"password": bmc.Password,
			}
			secret := CreateSecret(ctx, clusterProxy.GetClient(), namespaceBMH.Name, secretName, bmcCredentialsData)
			toCleanupBMH = append(toCleanupBMH, secret)

			By("Creating a BMH with inspection disabled and hardware details added")
			hardwareDetails := hardwareDetailsFor(&bmc)
			bmh := createBmh("bmh", namespaceBMH.Name, map[string]string{selectorLabel: selectorValue}, hardwareDetails)
			err := clusterProxy.GetClient().Create(ctx, bmh)
			Expect(err).NotTo(HaveOccurred())
			toCleanupBMH = append(toCleanupBMH, bmh)

			By("Waiting for the BMH to become available")
			WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
				Client: clusterProxy.GetClient(),
				Bmh:    *bmh,
				State:  metal3api.StateAvailable,
			}, e2eConfig.GetIntervals(specName, "wait-available")...)

			By("Creating hostDeployPolicies on namespace")
			hostDeployPolicy := createHostDeployPolicy(namespaceBMH.Name, namespaceClaimB.Name)
			toCleanupBMH = append(toCleanupBMH, hostDeployPolicy)

			By("Creating hostClaim A - not authorized")
			hostClaimA := createEmptyHostclaim(namespaceClaimA.Name)
			toCleanupClaimA = append(toCleanupClaimA, hostClaimA)
			time.Sleep(defaultDelay)
			By("Creating hostClaim B - can associate")
			hostClaimB := createEmptyHostclaim(namespaceClaimB.Name)
			toCleanupClaimB = append(toCleanupClaimB, hostClaimB)

			By("Waiting for the HostClaim B to become associated")
			checkAssociation(specName, hostClaimB, bmh)
			checkNoAssociation(specName, hostClaimA)

			By("Deleting the hostclaims")
			err = clusterProxy.GetClient().Delete(ctx, hostClaimA)
			Expect(err).NotTo(HaveOccurred())
			err = clusterProxy.GetClient().Delete(ctx, hostClaimB)
			Expect(err).NotTo(HaveOccurred())
			checkNoConsumer(specName, bmh)

			By("Modifying the HostDeployPolicies to use namespace labels")
			hostDeployPolicy.Spec = metal3api.HostDeployPolicySpec{
				HostClaimNamespaces: &metal3api.HostClaimNamespaces{
					HasLabels: []metal3api.NameValuePair{
						{Name: "tenant", Value: "b"},
					},
				},
			}
			err = clusterProxy.GetClient().Update(ctx, hostDeployPolicy)
			Expect(err).NotTo(HaveOccurred())

			By("Creating hostClaims (A not authorized - B can bind)")
			hostClaimA = createEmptyHostclaim(namespaceClaimA.Name)
			time.Sleep(defaultDelay)
			hostClaimB = createEmptyHostclaim(namespaceClaimB.Name)

			By("Waiting for the HostClaim B to become associated")
			checkAssociation(specName, hostClaimB, bmh)
			checkNoAssociation(specName, hostClaimA)

			By("Deleting the hostclaims")
			err = clusterProxy.GetClient().Delete(ctx, hostClaimA)
			Expect(err).NotTo(HaveOccurred())
			err = clusterProxy.GetClient().Delete(ctx, hostClaimB)
			Expect(err).NotTo(HaveOccurred())
			checkNoConsumer(specName, bmh)

			By("Modifying the HostDeployPolicies to use regular expressions")
			hostDeployPolicy.Spec = metal3api.HostDeployPolicySpec{
				HostClaimNamespaces: &metal3api.HostClaimNamespaces{
					NameMatches: "b$",
				},
			}
			err = clusterProxy.GetClient().Update(ctx, hostDeployPolicy)
			Expect(err).NotTo(HaveOccurred())

			By("Creating hostClaims (A not authorized - B can bind)")
			hostClaimA = createEmptyHostclaim(namespaceClaimA.Name)
			time.Sleep(defaultDelay)
			hostClaimB = createEmptyHostclaim(namespaceClaimB.Name)

			By("Waiting for the HostClaim B to become associated")
			checkAssociation(specName, hostClaimB, bmh)
			checkNoAssociation(specName, hostClaimA)

		})

		AfterEach(func() {
			DumpResources(ctx, e2eConfig, clusterProxy, path.Join(artifactFolder, specName))
			if !skipCleanup {
				Cleanup(ctx, clusterProxy, namespaceBMH, cancelWatches, e2eConfig, toCleanupBMH)
				Cleanup(ctx, clusterProxy, namespaceClaimA, nil, e2eConfig, toCleanupClaimA)
				Cleanup(ctx, clusterProxy, namespaceClaimB, nil, e2eConfig, toCleanupClaimB)
			}
		})
	},
)
