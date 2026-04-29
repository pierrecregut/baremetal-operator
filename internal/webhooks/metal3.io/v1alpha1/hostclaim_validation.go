package webhooks

import (
	"errors"
	"fmt"
	"strings"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/validation"
)

// validateHost validates BareMetalHost resource for creation.
func (webhook *HostClaimWebhook) validateHostClaim(hostclaim *metal3api.HostClaim) []error {
	var errStrings []string
	for lblKey, lblValue := range hostclaim.Spec.HostSelector.MatchLabels {
		for _, err := range validation.IsQualifiedName(lblKey) {
			errStrings = append(errStrings, fmt.Sprintf("%s=%s: %s", lblKey, lblValue, err))
		}
		for _, err := range validation.IsValidLabelValue(lblValue) {
			errStrings = append(errStrings, fmt.Sprintf("%s=%s: %s", lblKey, lblValue, err))
		}
	}
	for i, hsr := range hostclaim.Spec.HostSelector.MatchExpressions {
		for _, err := range validation.IsQualifiedName(hsr.Key) {
			errStrings = append(errStrings, fmt.Sprintf("matchExpr %d: %s", i+1, err))
		}
		switch hsr.Operator {
		case selection.Equals, selection.DoubleEquals, selection.NotEquals:
			if len(hsr.Values) != 1 {
				errStrings = append(errStrings, fmt.Sprintf(
					"matchExpr %d: exactly one value for operator %s in match expression on label key %s",
					i+1, hsr.Operator, hsr.Key))
			} else {
				for _, err := range validation.IsValidLabelValue(hsr.Values[0]) {
					errStrings = append(errStrings, fmt.Sprintf("matchExpr %d (value %q): %s", i+1, hsr.Values[0], err))
				}
			}
		case selection.In, selection.NotIn:
			for _, v := range hsr.Values {
				for _, err := range validation.IsValidLabelValue(v) {
					errStrings = append(errStrings, fmt.Sprintf("matchExpr %d (value %q): %s", i+1, v, err))
				}
			}
		case selection.Exists, selection.DoesNotExist:
			if len(hsr.Values) != 0 {
				errStrings = append(errStrings, fmt.Sprintf(
					"matchExpr %d: values not authorized for operator %s in match expression on label key %s",
					i+1, hsr.Operator, hsr.Key))
			}
		default:
			errStrings = append(errStrings, fmt.Sprintf(
				"matchExpr %d: invalid operation %s in match expression on label key %s", i+1, hsr.Operator, hsr.Key))
		}
	}
	var errs = make([]error, len(errStrings))
	for i := range errStrings {
		errs[i] = errors.New(errStrings[i])
	}

	if err := validateHostclaimAnnotations(hostclaim); err != nil {
		errs = append(errs, err...)
	}

	if hostclaim.Spec.Image != nil {
		if err := validateImage(hostclaim.Spec.Image); err != nil {
			errs = append(errs, err...)
		}
	}

	return errs
}

func validateHostclaimAnnotations(hostclaim *metal3api.HostClaim) []error {
	var errs []error
	var err error

	for annotation, value := range hostclaim.Annotations {
		switch {
		case strings.HasPrefix(annotation, metal3api.RebootAnnotationPrefix+"/") || annotation == metal3api.RebootAnnotationPrefix:
			err = validateRebootAnnotation(value)
		// TODO: should we check Detached annotation.
		default:
			err = nil
		}
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}
