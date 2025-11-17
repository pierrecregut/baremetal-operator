//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/json"
	"path"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/util/deprecated/v1beta1/patch"
)

func createBmh(name, namespace string, labels map[string]string) *metal3api.BareMetalHost {
	bmh := &metal3api.BareMetalHost{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				metal3api.InspectAnnotationPrefix:   "disabled",
				metal3api.HardwareDetailsAnnotation: hardwareDetails,
			},
			Labels: labels,
		},
		Spec: metal3api.BareMetalHostSpec{
			Online: false,
			BMC: metal3api.BMCDetails{
				Address:                        bmc.Address,
				CredentialsName:                "bmc-credentials",
				DisableCertificateVerification: bmc.DisableCertificateVerification,
			},
			BootMode:              metal3api.Legacy,
			BootMACAddress:        bmc.BootMacAddress,
			AutomatedCleaningMode: "disabled",
			RootDeviceHints:       &bmc.RootDeviceHints,
		},
	}
	return bmh
}

func createHardwareData(name, namespace string, labels map[string]string) *metal3api.HardwareData {
	var hwDetails = &metal3api.HardwareDetails{}
	err := json.Unmarshal([]byte(hardwareDetails), hwDetails)
	Expect(err).NotTo(HaveOccurred())
	hardwareData := &metal3api.HardwareData{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: metal3api.HardwareDataSpec{
			HardwareDetails: hwDetails,
		},
	}
	return hardwareData
}

var _ = Describe("Associate a hostclaim to a BMH, provision the BMH, unprovision it and delete the claim.", Label("required", "hostclaim"),
	func() {
		var (
			specName       = "multi"
			secretName     = "bmc-credentials"
			namespace      *corev1.Namespace
			namespaceClaim *corev1.Namespace
			cancelWatches  context.CancelFunc
		)

		BeforeEach(func() {
			namespace, cancelWatches = framework.CreateNamespaceAndWatchEvents(ctx, framework.CreateNamespaceAndWatchEventsInput{
				Creator:             clusterProxy.GetClient(),
				ClientSet:           clusterProxy.GetClientSet(),
				Name:                specName + "-bmh",
				LogFolder:           artifactFolder,
				IgnoreAlreadyExists: true,
			})
			namespaceClaim = framework.CreateNamespace(ctx, framework.CreateNamespaceInput{
				Creator:             clusterProxy.GetClient(),
				Name:                specName + "-claim",
				IgnoreAlreadyExists: true,
			})
		})

		It("Create a claim, associate it to a BMH", func() {
			By("Creating a secret with BMH credentials")
			bmcCredentialsData := map[string]string{
				"username": bmc.User,
				"password": bmc.Password,
			}
			CreateSecret(ctx, clusterProxy.GetClient(), namespace.Name, secretName, bmcCredentialsData)

			By("Creating a BMH with inspection disabled and hardware details added")
			bmh := createBmh("bmh", namespace.Name, map[string]string{"test.meta3.io/class": "selected"})
			err := clusterProxy.GetClient().Create(ctx, bmh)
			Expect(err).NotTo(HaveOccurred())
			hwData := createHardwareData("bmh", namespace.Name, map[string]string{"test.meta3.io/class": "selected"})
			err = clusterProxy.GetClient().Create(ctx, hwData)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for the BMH to become available")
			WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
				Client: clusterProxy.GetClient(),
				Bmh:    *bmh,
				State:  metal3api.StateAvailable,
			}, e2eConfig.GetIntervals(specName, "wait-available")...)
			By("Creating a suitable hostDeployPolicy")
			hostDeployPolicy := &metal3api.HostDeployPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      specName,
					Namespace: namespace.Name,
				},
				Spec: metal3api.HostDeployPolicySpec{
					HostClaimNamespaces: &metal3api.HostClaimNamespaces{
						Names: []string{namespaceClaim.Name},
					},
				},
			}
			err = clusterProxy.GetClient().Create(ctx, hostDeployPolicy)
			Expect(err).NotTo(HaveOccurred())
			By("Creating a hostClaim")
			hostClaim := &metal3api.HostClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      specName,
					Namespace: namespaceClaim.Name,
				},
				Spec: metal3api.HostClaimSpec{
					Image: &metal3api.Image{
						URL:      e2eConfig.GetVariable("IMAGE_URL"),
						Checksum: e2eConfig.GetVariable("IMAGE_CHECKSUM"),
					},
					HostSelector: metal3api.HostSelector{
						MatchLabels: map[string]string{"test.meta3.io/class": "selected"},
					},
					PoweredOn: true,
				},
			}
			err = clusterProxy.GetClient().Create(ctx, hostClaim)
			Expect(err).NotTo(HaveOccurred())
			By("Waiting for the BMH to become provisionned")
			WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
				Client: clusterProxy.GetClient(),
				Bmh:    *bmh,
				State:  metal3api.StateProvisioned,
			}, e2eConfig.GetIntervals(specName, "wait-available")...)

			By("Rebooting the bmh associated to hostclaim")

			By("Deprovisioning the hostclaim")
			helper, err := patch.NewHelper(hostClaim, clusterProxy.GetClient())
			Expect(err).NotTo(HaveOccurred())
			hostClaim.Spec.Image = nil
			hostClaim.Spec.PoweredOn = false
			err = helper.Patch(ctx, hostClaim)
			Expect(err).NotTo(HaveOccurred())
			By("Waiting for the BMH to become available again")
			WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
				Client: clusterProxy.GetClient(),
				Bmh:    *bmh,
				State:  metal3api.StateAvailable,
			}, e2eConfig.GetIntervals(specName, "wait-available")...)

			By("Deleting the hostclaim")
			err = clusterProxy.GetClient().Delete(ctx, hostClaim)
			Expect(err).NotTo(HaveOccurred())
			By("Deleting the bmh")
			err = clusterProxy.GetClient().Delete(ctx, bmh)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			DumpResources(ctx, e2eConfig, clusterProxy, path.Join(artifactFolder, specName))
			if !skipCleanup {
				isNamespaced := e2eConfig.GetBoolVariable("NAMESPACE_SCOPED")
				Cleanup(ctx, clusterProxy, namespace, cancelWatches, isNamespaced, e2eConfig.GetIntervals("default", "wait-namespace-deleted")...)
			}
		})
	},
)
