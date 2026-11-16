package controllers

import (
	"context"
	"fmt"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	"github.com/metal3-io/baremetal-operator/pkg/secretutils"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

const (
	// Events.
	EventAuthFormatUnsupported = "ImageAuthFormatUnsupported"
	EventAuthParseError        = "ImageAuthParseError"

	// ociAuthSecretKey is the SecretAccessError key for an image pull Secret.
	ociAuthSecretKey = "OCI auth"
)

// ImageAuthValidator validates image authentication secrets.
type ImageAuthValidator struct {
	recorder record.EventRecorder
}

// NewImageAuthValidator creates a new ImageAuthValidator.
func NewImageAuthValidator(recorder record.EventRecorder) *ImageAuthValidator {
	return &ImageAuthValidator{recorder: recorder}
}

// Validate validates the image authentication secret for the given image and returns the
// base64-encoded credentials in the format expected by Ironic. secretNamespace/secretName identify
// the secret to use: the caller resolves these (preferring a HostClaim's own reference and namespace
// when the host is HostClaim-managed; see imageAuthSecretRef in baremetalhost_controller.go) since
// the BareMetalHost's own Spec.Image.OCIAuthSecretName is cleared in that case. bmh is only used so
// that events stay attached to the BareMetalHost object being reconciled.
func (v *ImageAuthValidator) Validate(ctx context.Context, bmh *metal3api.BareMetalHost, image *metal3api.Image, secretNamespace, secretName string, secretMgr secretutils.SecretManager) (string, error) {
	if image == nil || !image.IsOCI() || secretName == "" {
		return "", nil
	}

	key := types.NamespacedName{Namespace: secretNamespace, Name: secretName}
	sec, err := secretMgr.ObtainSecret(ctx, key)
	if err != nil {
		// Match configuration-data Secrets: missing or inaccessible credentials
		// retry in place. Other retrieval failures stay unwrapped.
		if k8serrors.IsNotFound(err) || k8serrors.IsForbidden(err) {
			return "", SecretAccessError{secret: secretName, key: ociAuthSecretKey, err: err}
		}
		return "", err
	}

	if sec.Type != corev1.SecretTypeDockerConfigJson && sec.Type != corev1.SecretTypeDockercfg {
		if v.recorder != nil {
			v.recorder.Eventf(bmh, corev1.EventTypeWarning, EventAuthFormatUnsupported,
				"Secret \"%s/%s\" has unsupported type %q", secretNamespace, secretName, sec.Type)
		}
		return "", fmt.Errorf("secret \"%s/%s\" has unsupported type %q (expected %s or %s)",
			secretNamespace, secretName, sec.Type, corev1.SecretTypeDockerConfigJson, corev1.SecretTypeDockercfg)
	}

	credentials, err := secretutils.ExtractRegistryCredentials(sec, image.URL)
	if err != nil {
		if v.recorder != nil {
			v.recorder.Eventf(bmh, corev1.EventTypeWarning, EventAuthParseError,
				"Failed to extract credentials from secret \"%s/%s\": %v", secretNamespace, secretName, err)
		}
		return "", fmt.Errorf("failed to extract credentials from secret \"%s/%s\": %w", secretNamespace, secretName, err)
	}

	return credentials, nil
}
