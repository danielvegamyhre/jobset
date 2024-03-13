/*
Copyright 2023 The Kubernetes Authors.
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

package v1alpha2

import (
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/capabilities"
)

func ValidatePodTemplateSpec(spec *corev1.PodTemplateSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, unversionedvalidation.ValidateLabels(spec.Labels, fldPath.Child("labels"))...)
	allErrs = append(allErrs, ValidateAnnotations(spec.Annotations, fldPath.Child("annotations"))...)
	allErrs = append(allErrs, ValidatePodSpecificAnnotations(spec.Annotations, &spec.Spec, fldPath.Child("annotations"), opts)...)
	allErrs = append(allErrs, ValidatePodSpec(&spec.Spec, nil, fldPath.Child("spec"))...)
	allErrs = append(allErrs, validateSeccompAnnotationsAndFields(spec.ObjectMeta, &spec.Spec, fldPath.Child("spec"))...)

	if len(spec.Spec.EphemeralContainers) > 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("spec", "ephemeralContainers"), "ephemeral containers not allowed in pod template"))
	}

	return allErrs
}

func ValidateReadOnlyPersistentDisks(volumes []corev1.Volume, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i := range volumes {
		vol := &volumes[i]
		idxPath := fldPath.Index(i)
		if vol.GCEPersistentDisk != nil {
			if !vol.GCEPersistentDisk.ReadOnly {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("gcePersistentDisk", "readOnly"), false, "must be true for replicated pods > 1; GCE PD can only be mounted on multiple machines if it is read-only"))
			}
		}
		// TODO: What to do for AWS?  It doesn't support replicas
	}
	return allErrs
}

// ValidateTaintsInNodeAnnotations tests that the serialized taints in Node.Annotations has valid data
func ValidateTaintsInNodeAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	taints, err := helper.GetTaintsFromNodeAnnotations(annotations)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, corev1.TaintsAnnotationKey, err.Error()))
		return allErrs
	}

	if len(taints) > 0 {
		allErrs = append(allErrs, validateNodeTaints(taints, fldPath.Child(corev1.TaintsAnnotationKey))...)
	}

	return allErrs
}

// validateNodeTaints tests if given taints have valid data.
func validateNodeTaints(taints []corev1.Taint, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}

	uniqueTaints := map[corev1.TaintEffect]sets.String{}

	for i, currTaint := range taints {
		idxPath := fldPath.Index(i)
		// validate the taint key
		allErrors = append(allErrors, unversionedvalidation.ValidateLabelName(currTaint.Key, idxPath.Child("key"))...)
		// validate the taint value
		if errs := validation.IsValidLabelValue(currTaint.Value); len(errs) != 0 {
			allErrors = append(allErrors, field.Invalid(idxPath.Child("value"), currTaint.Value, strings.Join(errs, ";")))
		}
		// validate the taint effect
		allErrors = append(allErrors, validateTaintEffect(&currTaint.Effect, false, idxPath.Child("effect"))...)

		// validate if taint is unique by <key, effect>
		if len(uniqueTaints[currTaint.Effect]) > 0 && uniqueTaints[currTaint.Effect].Has(currTaint.Key) {
			duplicatedError := field.Duplicate(idxPath, currTaint)
			duplicatedError.Detail = "taints must be unique by key and effect pair"
			allErrors = append(allErrors, duplicatedError)
			continue
		}

		// add taint to existingTaints for uniqueness check
		if len(uniqueTaints[currTaint.Effect]) == 0 {
			uniqueTaints[currTaint.Effect] = sets.String{}
		}
		uniqueTaints[currTaint.Effect].Insert(currTaint.Key)
	}
	return allErrors
}

func ValidateNodeSpecificAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if annotations[corev1.TaintsAnnotationKey] != "" {
		allErrs = append(allErrs, ValidateTaintsInNodeAnnotations(annotations, fldPath)...)
	}

	if annotations[corev1.PreferAvoidPodsAnnotationKey] != "" {
		allErrs = append(allErrs, ValidateAvoidPodsInNodeAnnotations(annotations, fldPath)...)
	}
	return allErrs
}

// ValidateNode tests if required fields in the node are set.
func ValidateNode(node *corev1.Node) field.ErrorList {
	fldPath := field.NewPath("metadata")
	allErrs := ValidateObjectMeta(&node.ObjectMeta, false, ValidateNodeName, fldPath)
	allErrs = append(allErrs, ValidateNodeSpecificAnnotations(node.ObjectMeta.Annotations, fldPath.Child("annotations"))...)
	if len(node.Spec.Taints) > 0 {
		allErrs = append(allErrs, validateNodeTaints(node.Spec.Taints, fldPath.Child("taints"))...)
	}

	// Only validate spec.
	// All status fields are optional and can be updated later.
	// That said, if specified, we need to ensure they are valid.
	allErrs = append(allErrs, ValidateNodeResources(node)...)

	// validate PodCIDRS only if we need to
	if len(node.Spec.PodCIDRs) > 0 {
		podCIDRsField := field.NewPath("spec", "podCIDRs")

		// all PodCIDRs should be valid ones
		for idx, value := range node.Spec.PodCIDRs {
			if _, err := ValidateCIDR(value); err != nil {
				allErrs = append(allErrs, field.Invalid(podCIDRsField.Index(idx), node.Spec.PodCIDRs, "must be valid CIDR"))
			}
		}

		// if more than PodCIDR then
		// - validate for dual stack
		// - validate for duplication
		if len(node.Spec.PodCIDRs) > 1 {
			dualStack, err := netutils.IsDualStackCIDRStrings(node.Spec.PodCIDRs)
			if err != nil {
				allErrs = append(allErrs, field.InternalError(podCIDRsField, fmt.Errorf("invalid PodCIDRs. failed to check with dual stack with error:%v", err)))
			}
			if !dualStack || len(node.Spec.PodCIDRs) > 2 {
				allErrs = append(allErrs, field.Invalid(podCIDRsField, node.Spec.PodCIDRs, "may specify no more than one CIDR for each IP family"))
			}

			// PodCIDRs must not contain duplicates
			seen := sets.String{}
			for i, value := range node.Spec.PodCIDRs {
				if seen.Has(value) {
					allErrs = append(allErrs, field.Duplicate(podCIDRsField.Index(i), value))
				}
				seen.Insert(value)
			}
		}
	}

	return allErrs
}

// ValidateNodeResources is used to make sure a node has valid capacity and allocatable values.
func ValidateNodeResources(node *corev1.Node) field.ErrorList {
	allErrs := field.ErrorList{}

	// Validate resource quantities in capacity.
	for k, v := range node.Status.Capacity {
		resPath := field.NewPath("status", "capacity", string(k))
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(k), v, resPath)...)
	}

	// Validate resource quantities in allocatable.
	for k, v := range node.Status.Allocatable {
		resPath := field.NewPath("status", "allocatable", string(k))
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(k), v, resPath)...)
	}
	return allErrs
}

// ValidateNodeUpdate tests to make sure a node update can be applied.  Modifies oldNode.
func ValidateNodeUpdate(node, oldNode *corev1.Node) field.ErrorList {
	fldPath := field.NewPath("metadata")
	allErrs := ValidateObjectMetaUpdate(&node.ObjectMeta, &oldNode.ObjectMeta, fldPath)
	allErrs = append(allErrs, ValidateNodeSpecificAnnotations(node.ObjectMeta.Annotations, fldPath.Child("annotations"))...)

	// TODO: Enable the code once we have better core object.status update model. Currently,
	// anyone can update node status.
	// if !apiequality.Semantic.DeepEqual(node.Status, corev1.NodeStatus{}) {
	// 	allErrs = append(allErrs, field.Invalid("status", node.Status, "must be empty"))
	// }

	allErrs = append(allErrs, ValidateNodeResources(node)...)

	// Validate no duplicate addresses in node status.
	addresses := make(map[corev1.NodeAddress]bool)
	for i, address := range node.Status.Addresses {
		if _, ok := addresses[address]; ok {
			allErrs = append(allErrs, field.Duplicate(field.NewPath("status", "addresses").Index(i), address))
		}
		addresses[address] = true
	}

	// Allow the controller manager to assign a CIDR to a node if it doesn't have one.
	if len(oldNode.Spec.PodCIDRs) > 0 {
		// compare the entire slice
		if len(oldNode.Spec.PodCIDRs) != len(node.Spec.PodCIDRs) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "podCIDRs"), "node updates may not change podCIDR except from \"\" to valid"))
		} else {
			for idx, value := range oldNode.Spec.PodCIDRs {
				if value != node.Spec.PodCIDRs[idx] {
					allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "podCIDRs"), "node updates may not change podCIDR except from \"\" to valid"))
				}
			}
		}
	}

	// Allow controller manager updating provider ID when not set
	if len(oldNode.Spec.ProviderID) > 0 && oldNode.Spec.ProviderID != node.Spec.ProviderID {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "providerID"), "node updates may not change providerID except from \"\" to valid"))
	}

	if node.Spec.ConfigSource != nil {
		allErrs = append(allErrs, validateNodeConfigSourceSpec(node.Spec.ConfigSource, field.NewPath("spec", "configSource"))...)
	}
	if node.Status.Config != nil {
		allErrs = append(allErrs, validateNodeConfigStatus(node.Status.Config, field.NewPath("status", "config"))...)
	}

	// update taints
	if len(node.Spec.Taints) > 0 {
		allErrs = append(allErrs, validateNodeTaints(node.Spec.Taints, fldPath.Child("taints"))...)
	}

	if node.Spec.DoNotUseExternalID != oldNode.Spec.DoNotUseExternalID {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "externalID"), "may not be updated"))
	}

	// status and metadata are allowed change (barring restrictions above), so separately test spec field.
	// spec only has a few fields, so check the ones we don't allow changing
	//  1. PodCIDRs - immutable after first set - checked above
	//  2. ProviderID - immutable after first set - checked above
	//  3. Unschedulable - allowed to change
	//  4. Taints - allowed to change
	//  5. ConfigSource - allowed to change (and checked above)
	//  6. DoNotUseExternalID - immutable - checked above

	return allErrs
}

// validation specific to Node.Spec.ConfigSource
// The field ConfigSource is deprecated and will not be used. The validation is kept in place
// for the backward compatibility
func validateNodeConfigSourceSpec(source *corev1.NodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	count := int(0)
	if source.ConfigMap != nil {
		count++
		allErrs = append(allErrs, validateConfigMapNodeConfigSourceSpec(source.ConfigMap, fldPath.Child("configMap"))...)
	}
	// add more subfields here in the future as they are added to NodeConfigSource

	// exactly one reference subfield must be non-nil
	if count != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath, source, "exactly one reference subfield must be non-nil"))
	}
	return allErrs
}

// validation specific to Node.Spec.ConfigSource.ConfigMap
// The field ConfigSource is deprecated and will not be used. The validation is kept in place
// for the backward compatibility
func validateConfigMapNodeConfigSourceSpec(source *corev1.ConfigMapNodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// uid and resourceVersion must not be set in spec
	if string(source.UID) != "" {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("uid"), "uid must not be set in spec"))
	}
	if source.ResourceVersion != "" {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("resourceVersion"), "resourceVersion must not be set in spec"))
	}
	return append(allErrs, validateConfigMapNodeConfigSource(source, fldPath)...)
}

// validation specififc to Node.Status.Config
func validateNodeConfigStatus(status *corev1.NodeConfigStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if status.Assigned != nil {
		allErrs = append(allErrs, validateNodeConfigSourceStatus(status.Assigned, fldPath.Child("assigned"))...)
	}
	if status.Active != nil {
		allErrs = append(allErrs, validateNodeConfigSourceStatus(status.Active, fldPath.Child("active"))...)
	}
	if status.LastKnownGood != nil {
		allErrs = append(allErrs, validateNodeConfigSourceStatus(status.LastKnownGood, fldPath.Child("lastKnownGood"))...)
	}
	return allErrs
}

// validation specific to Node.Status.Config.(Active|Assigned|LastKnownGood)
func validateNodeConfigSourceStatus(source *corev1.NodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	count := int(0)
	if source.ConfigMap != nil {
		count++
		allErrs = append(allErrs, validateConfigMapNodeConfigSourceStatus(source.ConfigMap, fldPath.Child("configMap"))...)
	}
	// add more subfields here in the future as they are added to NodeConfigSource

	// exactly one reference subfield must be non-nil
	if count != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath, source, "exactly one reference subfield must be non-nil"))
	}
	return allErrs
}

// validation specific to Node.Status.Config.(Active|Assigned|LastKnownGood).ConfigMap
func validateConfigMapNodeConfigSourceStatus(source *corev1.ConfigMapNodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// uid and resourceVersion must be set in status
	if string(source.UID) == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("uid"), "uid must be set in status"))
	}
	if source.ResourceVersion == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("resourceVersion"), "resourceVersion must be set in status"))
	}
	return append(allErrs, validateConfigMapNodeConfigSource(source, fldPath)...)
}

// common validation
func validateConfigMapNodeConfigSource(source *corev1.ConfigMapNodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// validate target configmap namespace
	if source.Namespace == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("namespace"), "namespace must be set"))
	} else {
		for _, msg := range ValidateNameFunc(ValidateNamespaceName)(source.Namespace, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("namespace"), source.Namespace, msg))
		}
	}
	// validate target configmap name
	if source.Name == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "name must be set"))
	} else {
		for _, msg := range ValidateNameFunc(ValidateConfigMapName)(source.Name, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), source.Name, msg))
		}
	}
	// validate kubeletConfigKey against rules for configMap key names
	if source.KubeletConfigKey == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("kubeletConfigKey"), "kubeletConfigKey must be set"))
	} else {
		for _, msg := range validation.IsConfigMapKey(source.KubeletConfigKey) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("kubeletConfigKey"), source.KubeletConfigKey, msg))
		}
	}
	return allErrs
}

// Validate compute resource typename.
// Refer to docs/design/resources.md for more details.
func validateResourceName(value string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsQualifiedName(value) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg))
	}
	if len(allErrs) != 0 {
		return allErrs
	}

	if len(strings.Split(value, "/")) == 1 {
		if !helper.IsStandardResourceName(value) {
			return append(allErrs, field.Invalid(fldPath, value, "must be a standard resource type or fully qualified"))
		}
	}

	return allErrs
}

// Validate container resource name
// Refer to docs/design/resources.md for more details.
func validateContainerResourceName(value string, fldPath *field.Path) field.ErrorList {
	allErrs := validateResourceName(value, fldPath)

	if len(strings.Split(value, "/")) == 1 {
		if !helper.IsStandardContainerResourceName(value) {
			return append(allErrs, field.Invalid(fldPath, value, "must be a standard resource for containers"))
		}
	} else if !helper.IsNativeResource(corev1.ResourceName(value)) {
		if !helper.IsExtendedResourceName(corev1.ResourceName(value)) {
			return append(allErrs, field.Invalid(fldPath, value, "doesn't follow extended resource name standard"))
		}
	}
	return allErrs
}

// Validate resource names that can go in a resource quota
// Refer to docs/design/resources.md for more details.
func ValidateResourceQuotaResourceName(value string, fldPath *field.Path) field.ErrorList {
	allErrs := validateResourceName(value, fldPath)

	if len(strings.Split(value, "/")) == 1 {
		if !helper.IsStandardQuotaResourceName(value) {
			return append(allErrs, field.Invalid(fldPath, value, isInvalidQuotaResource))
		}
	}
	return allErrs
}

// Validate limit range types
func validateLimitRangeTypeName(value string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsQualifiedName(value) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg))
	}
	if len(allErrs) != 0 {
		return allErrs
	}

	if len(strings.Split(value, "/")) == 1 {
		if !helper.IsStandardLimitRangeType(value) {
			return append(allErrs, field.Invalid(fldPath, value, "must be a standard limit type or fully qualified"))
		}
	}

	return allErrs
}

// Validate limit range resource name
// limit types (other than Pod/Container) could contain storage not just cpu or memory
func validateLimitRangeResourceName(limitType corev1.LimitType, value string, fldPath *field.Path) field.ErrorList {
	switch limitType {
	case corev1.LimitTypePod, corev1.LimitTypeContainer:
		return validateContainerResourceName(value, fldPath)
	default:
		return validateResourceName(value, fldPath)
	}
}

// ValidateLimitRange tests if required fields in the LimitRange are set.
func ValidateLimitRange(limitRange *corev1.LimitRange) field.ErrorList {
	allErrs := ValidateObjectMeta(&limitRange.ObjectMeta, true, ValidateLimitRangeName, field.NewPath("metadata"))

	// ensure resource names are properly qualified per docs/design/resources.md
	limitTypeSet := map[corev1.LimitType]bool{}
	fldPath := field.NewPath("spec", "limits")
	for i := range limitRange.Spec.Limits {
		idxPath := fldPath.Index(i)
		limit := &limitRange.Spec.Limits[i]
		allErrs = append(allErrs, validateLimitRangeTypeName(string(limit.Type), idxPath.Child("type"))...)

		_, found := limitTypeSet[limit.Type]
		if found {
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("type"), limit.Type))
		}
		limitTypeSet[limit.Type] = true

		keys := sets.String{}
		min := map[string]resource.Quantity{}
		max := map[string]resource.Quantity{}
		defaults := map[string]resource.Quantity{}
		defaultRequests := map[string]resource.Quantity{}
		maxLimitRequestRatios := map[string]resource.Quantity{}

		for k, q := range limit.Max {
			allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, string(k), idxPath.Child("max").Key(string(k)))...)
			keys.Insert(string(k))
			max[string(k)] = q
		}
		for k, q := range limit.Min {
			allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, string(k), idxPath.Child("min").Key(string(k)))...)
			keys.Insert(string(k))
			min[string(k)] = q
		}

		if limit.Type == corev1.LimitTypePod {
			if len(limit.Default) > 0 {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("default"), "may not be specified when `type` is 'Pod'"))
			}
			if len(limit.DefaultRequest) > 0 {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("defaultRequest"), "may not be specified when `type` is 'Pod'"))
			}
		} else {
			for k, q := range limit.Default {
				allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, string(k), idxPath.Child("default").Key(string(k)))...)
				keys.Insert(string(k))
				defaults[string(k)] = q
			}
			for k, q := range limit.DefaultRequest {
				allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, string(k), idxPath.Child("defaultRequest").Key(string(k)))...)
				keys.Insert(string(k))
				defaultRequests[string(k)] = q
			}
		}

		if limit.Type == corev1.LimitTypePersistentVolumeClaim {
			_, minQuantityFound := limit.Min[corev1.ResourceStorage]
			_, maxQuantityFound := limit.Max[corev1.ResourceStorage]
			if !minQuantityFound && !maxQuantityFound {
				allErrs = append(allErrs, field.Required(idxPath.Child("limits"), "either minimum or maximum storage value is required, but neither was provided"))
			}
		}

		for k, q := range limit.MaxLimitRequestRatio {
			allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, string(k), idxPath.Child("maxLimitRequestRatio").Key(string(k)))...)
			keys.Insert(string(k))
			maxLimitRequestRatios[string(k)] = q
		}

		for k := range keys {
			minQuantity, minQuantityFound := min[k]
			maxQuantity, maxQuantityFound := max[k]
			defaultQuantity, defaultQuantityFound := defaults[k]
			defaultRequestQuantity, defaultRequestQuantityFound := defaultRequests[k]
			maxRatio, maxRatioFound := maxLimitRequestRatios[k]

			if minQuantityFound && maxQuantityFound && minQuantity.Cmp(maxQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("min").Key(string(k)), minQuantity, fmt.Sprintf("min value %s is greater than max value %s", minQuantity.String(), maxQuantity.String())))
			}

			if defaultRequestQuantityFound && minQuantityFound && minQuantity.Cmp(defaultRequestQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("defaultRequest").Key(string(k)), defaultRequestQuantity, fmt.Sprintf("min value %s is greater than default request value %s", minQuantity.String(), defaultRequestQuantity.String())))
			}

			if defaultRequestQuantityFound && maxQuantityFound && defaultRequestQuantity.Cmp(maxQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("defaultRequest").Key(string(k)), defaultRequestQuantity, fmt.Sprintf("default request value %s is greater than max value %s", defaultRequestQuantity.String(), maxQuantity.String())))
			}

			if defaultRequestQuantityFound && defaultQuantityFound && defaultRequestQuantity.Cmp(defaultQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("defaultRequest").Key(string(k)), defaultRequestQuantity, fmt.Sprintf("default request value %s is greater than default limit value %s", defaultRequestQuantity.String(), defaultQuantity.String())))
			}

			if defaultQuantityFound && minQuantityFound && minQuantity.Cmp(defaultQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("default").Key(string(k)), minQuantity, fmt.Sprintf("min value %s is greater than default value %s", minQuantity.String(), defaultQuantity.String())))
			}

			if defaultQuantityFound && maxQuantityFound && defaultQuantity.Cmp(maxQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("default").Key(string(k)), maxQuantity, fmt.Sprintf("default value %s is greater than max value %s", defaultQuantity.String(), maxQuantity.String())))
			}
			if maxRatioFound && maxRatio.Cmp(*resource.NewQuantity(1, resource.DecimalSI)) < 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("maxLimitRequestRatio").Key(string(k)), maxRatio, fmt.Sprintf("ratio %s is less than 1", maxRatio.String())))
			}
			if maxRatioFound && minQuantityFound && maxQuantityFound {
				maxRatioValue := float64(maxRatio.Value())
				minQuantityValue := minQuantity.Value()
				maxQuantityValue := maxQuantity.Value()
				if maxRatio.Value() < resource.MaxMilliValue && minQuantityValue < resource.MaxMilliValue && maxQuantityValue < resource.MaxMilliValue {
					maxRatioValue = float64(maxRatio.MilliValue()) / 1000
					minQuantityValue = minQuantity.MilliValue()
					maxQuantityValue = maxQuantity.MilliValue()
				}
				maxRatioLimit := float64(maxQuantityValue) / float64(minQuantityValue)
				if maxRatioValue > maxRatioLimit {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("maxLimitRequestRatio").Key(string(k)), maxRatio, fmt.Sprintf("ratio %s is greater than max/min = %f", maxRatio.String(), maxRatioLimit)))
				}
			}

			// for GPU, hugepages and other resources that are not allowed to overcommit,
			// the default value and defaultRequest value must match if both are specified
			if !helper.IsOvercommitAllowed(corev1.ResourceName(k)) && defaultQuantityFound && defaultRequestQuantityFound && defaultQuantity.Cmp(defaultRequestQuantity) != 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("defaultRequest").Key(string(k)), defaultRequestQuantity, fmt.Sprintf("default value %s must equal to defaultRequest value %s in %s", defaultQuantity.String(), defaultRequestQuantity.String(), k)))
			}
		}
	}

	return allErrs
}

// ValidateServiceAccount tests if required fields in the ServiceAccount are set.
func ValidateServiceAccount(serviceAccount *corev1.ServiceAccount) field.ErrorList {
	allErrs := ValidateObjectMeta(&serviceAccount.ObjectMeta, true, ValidateServiceAccountName, field.NewPath("metadata"))
	return allErrs
}

// ValidateServiceAccountUpdate tests if required fields in the ServiceAccount are set.
func ValidateServiceAccountUpdate(newServiceAccount, oldServiceAccount *corev1.ServiceAccount) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newServiceAccount.ObjectMeta, &oldServiceAccount.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateServiceAccount(newServiceAccount)...)
	return allErrs
}

// ValidateSecret tests if required fields in the Secret are set.
func ValidateSecret(secret *corev1.Secret) field.ErrorList {
	allErrs := ValidateObjectMeta(&secret.ObjectMeta, true, ValidateSecretName, field.NewPath("metadata"))

	dataPath := field.NewPath("data")
	totalSize := 0
	for key, value := range secret.Data {
		for _, msg := range validation.IsConfigMapKey(key) {
			allErrs = append(allErrs, field.Invalid(dataPath.Key(key), key, msg))
		}
		totalSize += len(value)
	}
	if totalSize > corev1.MaxSecretSize {
		allErrs = append(allErrs, field.TooLong(dataPath, "", corev1.MaxSecretSize))
	}

	switch secret.Type {
	case corev1.SecretTypeServiceAccountToken:
		// Only require Annotations[kubernetes.io/service-account.name]
		// Additional fields (like Annotations[kubernetes.io/service-account.uid] and Data[token]) might be contributed later by a controller loop
		if value := secret.Annotations[corev1.ServiceAccountNameKey]; len(value) == 0 {
			allErrs = append(allErrs, field.Required(field.NewPath("metadata", "annotations").Key(corev1.ServiceAccountNameKey), ""))
		}
	case corev1.SecretTypeOpaque, "":
	// no-op
	case corev1.SecretTypeDockercfg:
		dockercfgBytes, exists := secret.Data[corev1.DockerConfigKey]
		if !exists {
			allErrs = append(allErrs, field.Required(dataPath.Key(corev1.DockerConfigKey), ""))
			break
		}

		// make sure that the content is well-formed json.
		if err := json.Unmarshal(dockercfgBytes, &map[string]interface{}{}); err != nil {
			allErrs = append(allErrs, field.Invalid(dataPath.Key(corev1.DockerConfigKey), "<secret contents redacted>", err.Error()))
		}
	case corev1.SecretTypeDockerConfigJSON:
		dockerConfigJSONBytes, exists := secret.Data[corev1.DockerConfigJSONKey]
		if !exists {
			allErrs = append(allErrs, field.Required(dataPath.Key(corev1.DockerConfigJSONKey), ""))
			break
		}

		// make sure that the content is well-formed json.
		if err := json.Unmarshal(dockerConfigJSONBytes, &map[string]interface{}{}); err != nil {
			allErrs = append(allErrs, field.Invalid(dataPath.Key(corev1.DockerConfigJSONKey), "<secret contents redacted>", err.Error()))
		}
	case corev1.SecretTypeBasicAuth:
		_, usernameFieldExists := secret.Data[corev1.BasicAuthUsernameKey]
		_, passwordFieldExists := secret.Data[corev1.BasicAuthPasswordKey]

		// username or password might be empty, but the field must be present
		if !usernameFieldExists && !passwordFieldExists {
			allErrs = append(allErrs, field.Required(dataPath.Key(corev1.BasicAuthUsernameKey), ""))
			allErrs = append(allErrs, field.Required(dataPath.Key(corev1.BasicAuthPasswordKey), ""))
			break
		}
	case corev1.SecretTypeSSHAuth:
		if len(secret.Data[corev1.SSHAuthPrivateKey]) == 0 {
			allErrs = append(allErrs, field.Required(dataPath.Key(corev1.SSHAuthPrivateKey), ""))
			break
		}

	case corev1.SecretTypeTLS:
		if _, exists := secret.Data[corev1.TLSCertKey]; !exists {
			allErrs = append(allErrs, field.Required(dataPath.Key(corev1.TLSCertKey), ""))
		}
		if _, exists := secret.Data[corev1.TLSPrivateKeyKey]; !exists {
			allErrs = append(allErrs, field.Required(dataPath.Key(corev1.TLSPrivateKeyKey), ""))
		}
	// TODO: Verify that the key matches the cert.
	default:
		// no-op
	}

	return allErrs
}

// ValidateSecretUpdate tests if required fields in the Secret are set.
func ValidateSecretUpdate(newSecret, oldSecret *corev1.Secret) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newSecret.ObjectMeta, &oldSecret.ObjectMeta, field.NewPath("metadata"))

	allErrs = append(allErrs, ValidateImmutableField(newSecret.Type, oldSecret.Type, field.NewPath("type"))...)
	if oldSecret.Immutable != nil && *oldSecret.Immutable {
		if newSecret.Immutable == nil || !*newSecret.Immutable {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("immutable"), "field is immutable when `immutable` is set"))
		}
		if !reflect.DeepEqual(newSecret.Data, oldSecret.Data) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("data"), "field is immutable when `immutable` is set"))
		}
		// We don't validate StringData, as it was already converted back to Data
		// before validation is happening.
	}

	allErrs = append(allErrs, ValidateSecret(newSecret)...)
	return allErrs
}

// ValidateConfigMapName can be used to check whether the given ConfigMap name is valid.
// Prefix indicates this name will be used as part of generation, in which case
// trailing dashes are allowed.
var ValidateConfigMapName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateConfigMap tests whether required fields in the ConfigMap are set.
func ValidateConfigMap(cfg *corev1.ConfigMap) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateObjectMeta(&cfg.ObjectMeta, true, ValidateConfigMapName, field.NewPath("metadata"))...)

	totalSize := 0

	for key, value := range cfg.Data {
		for _, msg := range validation.IsConfigMapKey(key) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("data").Key(key), key, msg))
		}
		// check if we have a duplicate key in the other bag
		if _, isValue := cfg.BinaryData[key]; isValue {
			msg := "duplicate of key present in binaryData"
			allErrs = append(allErrs, field.Invalid(field.NewPath("data").Key(key), key, msg))
		}
		totalSize += len(value)
	}
	for key, value := range cfg.BinaryData {
		for _, msg := range validation.IsConfigMapKey(key) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("binaryData").Key(key), key, msg))
		}
		totalSize += len(value)
	}
	if totalSize > corev1.MaxSecretSize {
		// pass back "" to indicate that the error refers to the whole object.
		allErrs = append(allErrs, field.TooLong(field.NewPath(""), cfg, corev1.MaxSecretSize))
	}

	return allErrs
}

// ValidateConfigMapUpdate tests if required fields in the ConfigMap are set.
func ValidateConfigMapUpdate(newCfg, oldCfg *corev1.ConfigMap) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateObjectMetaUpdate(&newCfg.ObjectMeta, &oldCfg.ObjectMeta, field.NewPath("metadata"))...)

	if oldCfg.Immutable != nil && *oldCfg.Immutable {
		if newCfg.Immutable == nil || !*newCfg.Immutable {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("immutable"), "field is immutable when `immutable` is set"))
		}
		if !reflect.DeepEqual(newCfg.Data, oldCfg.Data) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("data"), "field is immutable when `immutable` is set"))
		}
		if !reflect.DeepEqual(newCfg.BinaryData, oldCfg.BinaryData) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("binaryData"), "field is immutable when `immutable` is set"))
		}
	}

	allErrs = append(allErrs, ValidateConfigMap(newCfg)...)
	return allErrs
}

func validateBasicResource(quantity resource.Quantity, fldPath *field.Path) field.ErrorList {
	if quantity.Value() < 0 {
		return field.ErrorList{field.Invalid(fldPath, quantity.Value(), "must be a valid resource quantity")}
	}
	return field.ErrorList{}
}

// Validates resource requirement spec.
func ValidateResourceRequirements(requirements *corev1.ResourceRequirements, podClaimNames sets.String, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	limPath := fldPath.Child("limits")
	reqPath := fldPath.Child("requests")
	limContainsCPUOrMemory := false
	reqContainsCPUOrMemory := false
	limContainsHugePages := false
	reqContainsHugePages := false
	supportedQoSComputeResources := sets.NewString(string(corev1.ResourceCPU), string(corev1.ResourceMemory))
	for resourceName, quantity := range requirements.Limits {

		fldPath := limPath.Key(string(resourceName))
		// Validate resource name.
		allErrs = append(allErrs, validateContainerResourceName(string(resourceName), fldPath)...)

		// Validate resource quantity.
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(resourceName), quantity, fldPath)...)

		if helper.IsHugePageResourceName(resourceName) {
			limContainsHugePages = true
			if err := validateResourceQuantityHugePageValue(resourceName, quantity); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath, quantity.String(), err.Error()))
			}
		}

		if supportedQoSComputeResources.Has(string(resourceName)) {
			limContainsCPUOrMemory = true
		}
	}
	for resourceName, quantity := range requirements.Requests {
		fldPath := reqPath.Key(string(resourceName))
		// Validate resource name.
		allErrs = append(allErrs, validateContainerResourceName(string(resourceName), fldPath)...)
		// Validate resource quantity.
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(resourceName), quantity, fldPath)...)

		// Check that request <= limit.
		limitQuantity, exists := requirements.Limits[resourceName]
		if exists {
			// For non overcommitable resources, not only requests can't exceed limits, they also can't be lower, i.e. must be equal.
			if quantity.Cmp(limitQuantity) != 0 && !helper.IsOvercommitAllowed(resourceName) {
				allErrs = append(allErrs, field.Invalid(reqPath, quantity.String(), fmt.Sprintf("must be equal to %s limit", resourceName)))
			} else if quantity.Cmp(limitQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(reqPath, quantity.String(), fmt.Sprintf("must be less than or equal to %s limit", resourceName)))
			}
		} else if !helper.IsOvercommitAllowed(resourceName) {
			allErrs = append(allErrs, field.Required(limPath, "Limit must be set for non overcommitable resources"))
		}
		if helper.IsHugePageResourceName(resourceName) {
			reqContainsHugePages = true
			if err := validateResourceQuantityHugePageValue(resourceName, quantity, opts); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath, quantity.String(), err.Error()))
			}
		}
		if supportedQoSComputeResources.Has(string(resourceName)) {
			reqContainsCPUOrMemory = true
		}

	}
	if !limContainsCPUOrMemory && !reqContainsCPUOrMemory && (reqContainsHugePages || limContainsHugePages) {
		allErrs = append(allErrs, field.Forbidden(fldPath, "HugePages require cpu or memory"))
	}

	allErrs = append(allErrs, validateResourceClaimNames(requirements.Claims, podClaimNames, fldPath.Child("claims"))...)

	return allErrs
}

// validateResourceClaimNames checks that the names in
// ResourceRequirements.Claims have a corresponding entry in
// PodSpec.ResourceClaims.
func validateResourceClaimNames(claims []corev1.ResourceClaim, podClaimNames sets.String, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	names := sets.String{}
	for i, claim := range claims {
		name := claim.Name
		if name == "" {
			allErrs = append(allErrs, field.Required(fldPath.Index(i), ""))
		} else {
			if names.Has(name) {
				allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), name))
			} else {
				names.Insert(name)
			}
			if !podClaimNames.Has(name) {
				// field.NotFound doesn't accept an
				// explanation. Adding one here is more
				// user-friendly.
				error := field.NotFound(fldPath.Index(i), name)
				error.Detail = "must be one of the names in pod.spec.resourceClaims"
				if len(podClaimNames) == 0 {
					error.Detail += " which is empty"
				} else {
					error.Detail += ": " + strings.Join(podClaimNames.List(), ", ")
				}
				allErrs = append(allErrs, error)
			}
		}
	}
	return allErrs
}

func validateResourceQuantityHugePageValue(name corev1.ResourceName, quantity resource.Quantity, opts PodValidationOptions) error {
	if !helper.IsHugePageResourceName(name) {
		return nil
	}

	if !opts.AllowIndivisibleHugePagesValues && !helper.IsHugePageResourceValueDivisible(name, quantity) {
		return fmt.Errorf("%s is not positive integer multiple of %s", quantity.String(), name)
	}

	return nil
}

// validateResourceQuotaScopes ensures that each enumerated hard resource constraint is valid for set of scopes
func validateResourceQuotaScopes(resourceQuotaSpec *corev1.ResourceQuotaSpec, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(resourceQuotaSpec.Scopes) == 0 {
		return allErrs
	}
	hardLimits := sets.NewString()
	for k := range resourceQuotaSpec.Hard {
		hardLimits.Insert(string(k))
	}
	fldPath := fld.Child("scopes")
	scopeSet := sets.NewString()
	for _, scope := range resourceQuotaSpec.Scopes {
		if !helper.IsStandardResourceQuotaScope(string(scope)) {
			allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.Scopes, "unsupported scope"))
		}
		for _, k := range hardLimits.List() {
			if helper.IsStandardQuotaResourceName(k) && !helper.IsResourceQuotaScopeValidForResource(scope, k) {
				allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.Scopes, "unsupported scope applied to resource"))
			}
		}
		scopeSet.Insert(string(scope))
	}
	invalidScopePairs := []sets.String{
		sets.NewString(string(corev1.ResourceQuotaScopeBestEffort), string(corev1.ResourceQuotaScopeNotBestEffort)),
		sets.NewString(string(corev1.ResourceQuotaScopeTerminating), string(corev1.ResourceQuotaScopeNotTerminating)),
	}
	for _, invalidScopePair := range invalidScopePairs {
		if scopeSet.HasAll(invalidScopePair.List()...) {
			allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.Scopes, "conflicting scopes"))
		}
	}
	return allErrs
}

// validateScopedResourceSelectorRequirement tests that the match expressions has valid data
func validateScopedResourceSelectorRequirement(resourceQuotaSpec *corev1.ResourceQuotaSpec, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	hardLimits := sets.NewString()
	for k := range resourceQuotaSpec.Hard {
		hardLimits.Insert(string(k))
	}
	fldPath := fld.Child("matchExpressions")
	scopeSet := sets.NewString()
	for _, req := range resourceQuotaSpec.ScopeSelector.MatchExpressions {
		if !helper.IsStandardResourceQuotaScope(string(req.ScopeName)) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("scopeName"), req.ScopeName, "unsupported scope"))
		}
		for _, k := range hardLimits.List() {
			if helper.IsStandardQuotaResourceName(k) && !helper.IsResourceQuotaScopeValidForResource(req.ScopeName, k) {
				allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.ScopeSelector, "unsupported scope applied to resource"))
			}
		}
		switch req.ScopeName {
		case corev1.ResourceQuotaScopeBestEffort, corev1.ResourceQuotaScopeNotBestEffort, corev1.ResourceQuotaScopeTerminating, corev1.ResourceQuotaScopeNotTerminating, corev1.ResourceQuotaScopeCrossNamespacePodAffinity:
			if req.Operator != corev1.ScopeSelectorOpExists {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("operator"), req.Operator,
					"must be 'Exist' when scope is any of ResourceQuotaScopeTerminating, ResourceQuotaScopeNotTerminating, ResourceQuotaScopeBestEffort, ResourceQuotaScopeNotBestEffort or ResourceQuotaScopeCrossNamespacePodAffinity"))
			}
		}

		switch req.Operator {
		case corev1.ScopeSelectorOpIn, corev1.ScopeSelectorOpNotIn:
			if len(req.Values) == 0 {
				allErrs = append(allErrs, field.Required(fldPath.Child("values"),
					"must be at least one value when `operator` is 'In' or 'NotIn' for scope selector"))
			}
		case corev1.ScopeSelectorOpExists, corev1.ScopeSelectorOpDoesNotExist:
			if len(req.Values) != 0 {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("values"), req.Values,
					"must be no value when `operator` is 'Exist' or 'DoesNotExist' for scope selector"))
			}
		default:
			allErrs = append(allErrs, field.Invalid(fldPath.Child("operator"), req.Operator, "not a valid selector operator"))
		}
		scopeSet.Insert(string(req.ScopeName))
	}
	invalidScopePairs := []sets.String{
		sets.NewString(string(corev1.ResourceQuotaScopeBestEffort), string(corev1.ResourceQuotaScopeNotBestEffort)),
		sets.NewString(string(corev1.ResourceQuotaScopeTerminating), string(corev1.ResourceQuotaScopeNotTerminating)),
	}
	for _, invalidScopePair := range invalidScopePairs {
		if scopeSet.HasAll(invalidScopePair.List()...) {
			allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.Scopes, "conflicting scopes"))
		}
	}

	return allErrs
}

// validateScopeSelector tests that the specified scope selector has valid data
func validateScopeSelector(resourceQuotaSpec *corev1.ResourceQuotaSpec, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if resourceQuotaSpec.ScopeSelector == nil {
		return allErrs
	}
	allErrs = append(allErrs, validateScopedResourceSelectorRequirement(resourceQuotaSpec, fld.Child("scopeSelector"))...)
	return allErrs
}

// ValidateResourceQuota tests if required fields in the ResourceQuota are set.
func ValidateResourceQuota(resourceQuota *corev1.ResourceQuota) field.ErrorList {
	allErrs := ValidateObjectMeta(&resourceQuota.ObjectMeta, true, ValidateResourceQuotaName, field.NewPath("metadata"))

	allErrs = append(allErrs, ValidateResourceQuotaSpec(&resourceQuota.Spec, field.NewPath("spec"))...)
	allErrs = append(allErrs, ValidateResourceQuotaStatus(&resourceQuota.Status, field.NewPath("status"))...)

	return allErrs
}

func ValidateResourceQuotaStatus(status *corev1.ResourceQuotaStatus, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	fldPath := fld.Child("hard")
	for k, v := range status.Hard {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(string(k), resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(k), v, resPath)...)
	}
	fldPath = fld.Child("used")
	for k, v := range status.Used {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(string(k), resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(k), v, resPath)...)
	}

	return allErrs
}

func ValidateResourceQuotaSpec(resourceQuotaSpec *corev1.ResourceQuotaSpec, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	fldPath := fld.Child("hard")
	for k, v := range resourceQuotaSpec.Hard {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(string(k), resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(k), v, resPath)...)
	}

	allErrs = append(allErrs, validateResourceQuotaScopes(resourceQuotaSpec, fld)...)
	allErrs = append(allErrs, validateScopeSelector(resourceQuotaSpec, fld)...)

	return allErrs
}

// ValidateResourceQuantityValue enforces that specified quantity is valid for specified resource
func ValidateResourceQuantityValue(resource string, value resource.Quantity, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateNonnegativeQuantity(value, fldPath)...)
	if helper.IsIntegerResourceName(resource) {
		if value.MilliValue()%int64(1000) != int64(0) {
			allErrs = append(allErrs, field.Invalid(fldPath, value, isNotIntegerErrorMsg))
		}
	}
	return allErrs
}

// ValidateResourceQuotaUpdate tests to see if the update is legal for an end user to make.
func ValidateResourceQuotaUpdate(newResourceQuota, oldResourceQuota *corev1.ResourceQuota) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newResourceQuota.ObjectMeta, &oldResourceQuota.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateResourceQuotaSpec(&newResourceQuota.Spec, field.NewPath("spec"))...)

	// ensure scopes cannot change, and that resources are still valid for scope
	fldPath := field.NewPath("spec", "scopes")
	oldScopes := sets.NewString()
	newScopes := sets.NewString()
	for _, scope := range newResourceQuota.Spec.Scopes {
		newScopes.Insert(string(scope))
	}
	for _, scope := range oldResourceQuota.Spec.Scopes {
		oldScopes.Insert(string(scope))
	}
	if !oldScopes.Equal(newScopes) {
		allErrs = append(allErrs, field.Invalid(fldPath, newResourceQuota.Spec.Scopes, fieldImmutableErrorMsg))
	}

	return allErrs
}

// ValidateResourceQuotaStatusUpdate tests to see if the status update is legal for an end user to make.
func ValidateResourceQuotaStatusUpdate(newResourceQuota, oldResourceQuota *corev1.ResourceQuota) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newResourceQuota.ObjectMeta, &oldResourceQuota.ObjectMeta, field.NewPath("metadata"))
	if len(newResourceQuota.ResourceVersion) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("resourceVersion"), ""))
	}
	fldPath := field.NewPath("status", "hard")
	for k, v := range newResourceQuota.Status.Hard {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(string(k), resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(k), v, resPath)...)
	}
	fldPath = field.NewPath("status", "used")
	for k, v := range newResourceQuota.Status.Used {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(string(k), resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(string(k), v, resPath)...)
	}
	return allErrs
}

// ValidateNamespace tests if required fields are set.
func ValidateNamespace(namespace *corev1.Namespace) field.ErrorList {
	allErrs := ValidateObjectMeta(&namespace.ObjectMeta, false, ValidateNamespaceName, field.NewPath("metadata"))
	for i := range namespace.Spec.Finalizers {
		allErrs = append(allErrs, validateFinalizerName(string(namespace.Spec.Finalizers[i]), field.NewPath("spec", "finalizers"))...)
	}
	return allErrs
}

// Validate finalizer names
func validateFinalizerName(stringValue string, fldPath *field.Path) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateFinalizerName(stringValue, fldPath)
	allErrs = append(allErrs, validateKubeFinalizerName(stringValue, fldPath)...)
	return allErrs
}

// validateKubeFinalizerName checks for "standard" names of legacy finalizer
func validateKubeFinalizerName(stringValue string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(strings.Split(stringValue, "/")) == 1 {
		if !helper.IsStandardFinalizerName(stringValue) {
			return append(allErrs, field.Invalid(fldPath, stringValue, "name is neither a standard finalizer name nor is it fully qualified"))
		}
	}

	return allErrs
}

// ValidateNamespaceUpdate tests to make sure a namespace update can be applied.
func ValidateNamespaceUpdate(newNamespace *corev1.Namespace, oldNamespace *corev1.Namespace) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newNamespace.ObjectMeta, &oldNamespace.ObjectMeta, field.NewPath("metadata"))
	return allErrs
}

// ValidateNamespaceStatusUpdate tests to see if the update is legal for an end user to make.
func ValidateNamespaceStatusUpdate(newNamespace, oldNamespace *corev1.Namespace) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newNamespace.ObjectMeta, &oldNamespace.ObjectMeta, field.NewPath("metadata"))
	if newNamespace.DeletionTimestamp.IsZero() {
		if newNamespace.Status.Phase != corev1.NamespaceActive {
			allErrs = append(allErrs, field.Invalid(field.NewPath("status", "Phase"), newNamespace.Status.Phase, "may only be 'Active' if `deletionTimestamp` is empty"))
		}
	} else {
		if newNamespace.Status.Phase != corev1.NamespaceTerminating {
			allErrs = append(allErrs, field.Invalid(field.NewPath("status", "Phase"), newNamespace.Status.Phase, "may only be 'Terminating' if `deletionTimestamp` is not empty"))
		}
	}
	return allErrs
}

// ValidateNamespaceFinalizeUpdate tests to see if the update is legal for an end user to make.
func ValidateNamespaceFinalizeUpdate(newNamespace, oldNamespace *corev1.Namespace) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newNamespace.ObjectMeta, &oldNamespace.ObjectMeta, field.NewPath("metadata"))

	fldPath := field.NewPath("spec", "finalizers")
	for i := range newNamespace.Spec.Finalizers {
		idxPath := fldPath.Index(i)
		allErrs = append(allErrs, validateFinalizerName(string(newNamespace.Spec.Finalizers[i]), idxPath)...)
	}
	return allErrs
}

// ValidateEndpoints validates Endpoints on create and update.
func ValidateEndpoints(endpoints *corev1.Endpoints) field.ErrorList {
	allErrs := ValidateObjectMeta(&endpoints.ObjectMeta, true, ValidateEndpointsName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateEndpointsSpecificAnnotations(endpoints.Annotations, field.NewPath("annotations"))...)
	allErrs = append(allErrs, validateEndpointSubsets(endpoints.Subsets, field.NewPath("subsets"))...)
	return allErrs
}

// ValidateEndpointsCreate validates Endpoints on create.
func ValidateEndpointsCreate(endpoints *corev1.Endpoints) field.ErrorList {
	return ValidateEndpoints(endpoints)
}

// ValidateEndpointsUpdate validates Endpoints on update. NodeName changes are
// allowed during update to accommodate the case where nodeIP or PodCIDR is
// reused. An existing endpoint ip will have a different nodeName if this
// happens.
func ValidateEndpointsUpdate(newEndpoints, oldEndpoints *corev1.Endpoints) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newEndpoints.ObjectMeta, &oldEndpoints.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateEndpoints(newEndpoints)...)
	return allErrs
}

func validateEndpointSubsets(subsets []corev1.EndpointSubset, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i := range subsets {
		ss := &subsets[i]
		idxPath := fldPath.Index(i)

		// EndpointSubsets must include endpoint address. For headless service, we allow its endpoints not to have ports.
		if len(ss.Addresses) == 0 && len(ss.NotReadyAddresses) == 0 {
			// TODO: consider adding a RequiredOneOf() error for this and similar cases
			allErrs = append(allErrs, field.Required(idxPath, "must specify `addresses` or `notReadyAddresses`"))
		}
		for addr := range ss.Addresses {
			allErrs = append(allErrs, validateEndpointAddress(&ss.Addresses[addr], idxPath.Child("addresses").Index(addr))...)
		}
		for addr := range ss.NotReadyAddresses {
			allErrs = append(allErrs, validateEndpointAddress(&ss.NotReadyAddresses[addr], idxPath.Child("notReadyAddresses").Index(addr))...)
		}
		for port := range ss.Ports {
			allErrs = append(allErrs, validateEndpointPort(&ss.Ports[port], len(ss.Ports) > 1, idxPath.Child("ports").Index(port))...)
		}
	}

	return allErrs
}

func validateEndpointAddress(address *corev1.EndpointAddress, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsValidIP(address.IP) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("ip"), address.IP, msg))
	}
	if len(address.Hostname) > 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(address.Hostname, fldPath.Child("hostname"))...)
	}
	// During endpoint update, verify that NodeName is a DNS subdomain and transition rules allow the update
	if address.NodeName != nil {
		for _, msg := range ValidateNodeName(*address.NodeName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeName"), *address.NodeName, msg))
		}
	}
	allErrs = append(allErrs, ValidateNonSpecialIP(address.IP, fldPath.Child("ip"))...)
	return allErrs
}

// ValidateNonSpecialIP is used to validate Endpoints, EndpointSlices, and
// external IPs. Specifically, this disallows unspecified and loopback addresses
// are nonsensical and link-local addresses tend to be used for node-centric
// purposes (e.g. metadata service).
//
// IPv6 references
// - https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml
// - https://www.iana.org/assignments/ipv6-multicast-addresses/ipv6-multicast-addresses.xhtml
func ValidateNonSpecialIP(ipAddress string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	ip := netutils.ParseIPSloppy(ipAddress)
	if ip == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, "must be a valid IP address"))
		return allErrs
	}
	if ip.IsUnspecified() {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, fmt.Sprintf("may not be unspecified (%v)", ipAddress)))
	}
	if ip.IsLoopback() {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, "may not be in the loopback range (127.0.0.0/8, ::1/128)"))
	}
	if ip.IsLinkLocalUnicast() {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, "may not be in the link-local range (169.254.0.0/16, fe80::/10)"))
	}
	if ip.IsLinkLocalMulticast() {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, "may not be in the link-local multicast range (224.0.0.0/24, ff02::/10)"))
	}
	return allErrs
}

func validateEndpointPort(port *corev1.EndpointPort, requireName bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if requireName && len(port.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else if len(port.Name) != 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(port.Name, fldPath.Child("name"))...)
	}
	for _, msg := range validation.IsValidPortNum(int(port.Port)) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("port"), port.Port, msg))
	}
	if len(port.Protocol) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("protocol"), ""))
	} else if !supportedPortProtocols.Has(string(port.Protocol)) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("protocol"), port.Protocol, supportedPortProtocols.List()))
	}
	if port.AppProtocol != nil {
		allErrs = append(allErrs, ValidateQualifiedName(*port.AppProtocol, fldPath.Child("appProtocol"))...)
	}
	return allErrs
}

// ValidateSecurityContext ensures the security context contains valid settings
func ValidateSecurityContext(sc *corev1.SecurityContext, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// this should only be true for testing since SecurityContext is defaulted by the core
	if sc == nil {
		return allErrs
	}

	if sc.Privileged != nil {
		if *sc.Privileged && !capabilities.Get().AllowPrivileged {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("privileged"), "disallowed by cluster policy"))
		}
	}

	if sc.RunAsUser != nil {
		for _, msg := range validation.IsValidUserID(*sc.RunAsUser) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("runAsUser"), *sc.RunAsUser, msg))
		}
	}

	if sc.RunAsGroup != nil {
		for _, msg := range validation.IsValidGroupID(*sc.RunAsGroup) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("runAsGroup"), *sc.RunAsGroup, msg))
		}
	}

	if sc.ProcMount != nil {
		if err := ValidateProcMountType(fldPath.Child("procMount"), *sc.ProcMount); err != nil {
			allErrs = append(allErrs, err)
		}

	}
	allErrs = append(allErrs, validateSeccompProfileField(sc.SeccompProfile, fldPath.Child("seccompProfile"))...)
	if sc.AllowPrivilegeEscalation != nil && !*sc.AllowPrivilegeEscalation {
		if sc.Privileged != nil && *sc.Privileged {
			allErrs = append(allErrs, field.Invalid(fldPath, sc, "cannot set `allowPrivilegeEscalation` to false and `privileged` to true"))
		}

		if sc.Capabilities != nil {
			for _, cap := range sc.Capabilities.Add {
				if string(cap) == "CAP_SYS_ADMIN" {
					allErrs = append(allErrs, field.Invalid(fldPath, sc, "cannot set `allowPrivilegeEscalation` to false and `capabilities.Add` CAP_SYS_ADMIN"))
				}
			}
		}
	}

	allErrs = append(allErrs, validateWindowsSecurityContextOptions(sc.WindowsOptions, fldPath.Child("windowsOptions"))...)

	return allErrs
}

// maxGMSACredentialSpecLength is the max length, in bytes, for the actual contents
// of a GMSA cred spec. In general, those shouldn't be more than a few hundred bytes,
// so we want to give plenty of room here while still providing an upper bound.
// The runAsUserName field will be used to execute the given container's entrypoint, and
// it can be formatted as "DOMAIN/USER", where the DOMAIN is optional, maxRunAsUserNameDomainLength
// is the max character length for the user's DOMAIN, and maxRunAsUserNameUserLength
// is the max character length for the USER itself. Both the DOMAIN and USER have their
// own restrictions, and more information about them can be found here:
// https://support.microsoft.com/en-us/help/909264/naming-conventions-in-active-directory-for-computers-domains-sites-and
// https://docs.microsoft.com/en-us/previous-versions/windows/it-pro/windows-2000-server/bb726984(v=technet.10)
const (
	maxGMSACredentialSpecLengthInKiB = 64
	maxGMSACredentialSpecLength      = maxGMSACredentialSpecLengthInKiB * 1024
	maxRunAsUserNameDomainLength     = 256
	maxRunAsUserNameUserLength       = 104
)

var (
	// control characters are not permitted in the runAsUserName field.
	ctrlRegex = regexp.MustCompile(`[[:cntrl:]]+`)

	// a valid NetBios Domain name cannot start with a dot, has at least 1 character,
	// at most 15 characters, and it cannot the characters: \ / : * ? " < > |
	validNetBiosRegex = regexp.MustCompile(`^[^\\/:\*\?"<>|\.][^\\/:\*\?"<>|]{0,14}$`)

	// a valid DNS name contains only alphanumeric characters, dots, and dashes.
	dnsLabelFormat                 = `[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?`
	dnsSubdomainFormat             = fmt.Sprintf(`^%s(?:\.%s)*$`, dnsLabelFormat, dnsLabelFormat)
	validWindowsUserDomainDNSRegex = regexp.MustCompile(dnsSubdomainFormat)

	// a username is invalid if it contains the characters: " / \ [ ] : ; | = , + * ? < > @
	// or it contains only dots or spaces.
	invalidUserNameCharsRegex      = regexp.MustCompile(`["/\\:;|=,\+\*\?<>@\[\]]`)
	invalidUserNameDotsSpacesRegex = regexp.MustCompile(`^[\. ]+$`)
)

func validateWindowsSecurityContextOptions(windowsOptions *corev1.WindowsSecurityContextOptions, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if windowsOptions == nil {
		return allErrs
	}

	if windowsOptions.GMSACredentialSpecName != nil {
		// gmsaCredentialSpecName must be the name of a custom resource
		for _, msg := range validation.IsDNS1123Subdomain(*windowsOptions.GMSACredentialSpecName) {
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("gmsaCredentialSpecName"), windowsOptions.GMSACredentialSpecName, msg))
		}
	}

	if windowsOptions.GMSACredentialSpec != nil {
		if l := len(*windowsOptions.GMSACredentialSpec); l == 0 {
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("gmsaCredentialSpec"), windowsOptions.GMSACredentialSpec, "gmsaCredentialSpec cannot be an empty string"))
		} else if l > maxGMSACredentialSpecLength {
			errMsg := fmt.Sprintf("gmsaCredentialSpec size must be under %d KiB", maxGMSACredentialSpecLengthInKiB)
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("gmsaCredentialSpec"), windowsOptions.GMSACredentialSpec, errMsg))
		}
	}

	if windowsOptions.RunAsUserName != nil {
		if l := len(*windowsOptions.RunAsUserName); l == 0 {
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, "runAsUserName cannot be an empty string"))
		} else if ctrlRegex.MatchString(*windowsOptions.RunAsUserName) {
			errMsg := "runAsUserName cannot contain control characters"
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
		} else if parts := strings.Split(*windowsOptions.RunAsUserName, "\\"); len(parts) > 2 {
			errMsg := "runAsUserName cannot contain more than one backslash"
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
		} else {
			var (
				hasDomain = false
				domain    = ""
				user      string
			)
			if len(parts) == 1 {
				user = parts[0]
			} else {
				hasDomain = true
				domain = parts[0]
				user = parts[1]
			}

			if len(domain) >= maxRunAsUserNameDomainLength {
				errMsg := fmt.Sprintf("runAsUserName's Domain length must be under %d characters", maxRunAsUserNameDomainLength)
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}

			if hasDomain && !(validNetBiosRegex.MatchString(domain) || validWindowsUserDomainDNSRegex.MatchString(domain)) {
				errMsg := "runAsUserName's Domain doesn't match the NetBios nor the DNS format"
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}

			if l := len(user); l == 0 {
				errMsg := "runAsUserName's User cannot be empty"
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			} else if l > maxRunAsUserNameUserLength {
				errMsg := fmt.Sprintf("runAsUserName's User length must not be longer than %d characters", maxRunAsUserNameUserLength)
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}

			if invalidUserNameDotsSpacesRegex.MatchString(user) {
				errMsg := `runAsUserName's User cannot contain only periods or spaces`
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}

			if invalidUserNameCharsRegex.MatchString(user) {
				errMsg := `runAsUserName's User cannot contain the following characters: "/\:;|=,+*?<>@[]`
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}
		}
	}

	return allErrs
}

func validateWindowsHostProcessPod(podSpec *corev1.PodSpec, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// Keep track of container and hostProcess container count for validate
	containerCount := 0
	hostProcessContainerCount := 0

	var podHostProcess *bool
	if podSpec.SecurityContext != nil && podSpec.SecurityContext.WindowsOptions != nil {
		podHostProcess = podSpec.SecurityContext.WindowsOptions.HostProcess
	}

	hostNetwork := false
	if podSpec.SecurityContext != nil {
		hostNetwork = podSpec.SecurityContext.HostNetwork
	}

	podshelper.VisitContainersWithPath(podSpec, fieldPath, func(c *corev1.Container, cFieldPath *field.Path) bool {
		containerCount++

		var containerHostProcess *bool = nil
		if c.SecurityContext != nil && c.SecurityContext.WindowsOptions != nil {
			containerHostProcess = c.SecurityContext.WindowsOptions.HostProcess
		}

		if podHostProcess != nil && containerHostProcess != nil && *podHostProcess != *containerHostProcess {
			errMsg := fmt.Sprintf("pod hostProcess value must be identical if both are specified, was %v", *podHostProcess)
			allErrs = append(allErrs, field.Invalid(cFieldPath.Child("securityContext", "windowsOptions", "hostProcess"), *containerHostProcess, errMsg))
		}

		switch {
		case containerHostProcess != nil && *containerHostProcess:
			// Container explicitly sets hostProcess=true
			hostProcessContainerCount++
		case containerHostProcess == nil && podHostProcess != nil && *podHostProcess:
			// Container inherits hostProcess=true from pod settings
			hostProcessContainerCount++
		}

		return true
	})

	if hostProcessContainerCount > 0 {
		// At present, if a Windows Pods contains any HostProcess containers than all containers must be
		// HostProcess containers (explicitly set or inherited).
		if hostProcessContainerCount != containerCount {
			errMsg := "If pod contains any hostProcess containers then all containers must be HostProcess containers"
			allErrs = append(allErrs, field.Invalid(fieldPath, "", errMsg))
		}

		// At present Windows Pods which contain HostProcess containers must also set HostNetwork.
		if !hostNetwork {
			errMsg := "hostNetwork must be true if pod contains any hostProcess containers"
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("hostNetwork"), hostNetwork, errMsg))
		}

		if !capabilities.Get().AllowPrivileged {
			errMsg := "hostProcess containers are disallowed by cluster policy"
			allErrs = append(allErrs, field.Forbidden(fieldPath, errMsg))
		}
	}

	return allErrs
}

// validateOS validates the OS field within pod spec
func validateOS(podSpec *corev1.PodSpec, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	os := podSpec.OS
	if os == nil {
		return allErrs
	}
	if len(os.Name) == 0 {
		return append(allErrs, field.Required(fldPath.Child("name"), "cannot be empty"))
	}
	osName := string(os.Name)
	if !validOS.Has(osName) {
		allErrs = append(allErrs, field.NotSupported(fldPath, osName, validOS.List()))
	}
	return allErrs
}

func ValidatePodLogOptions(opts *corev1.PodLogOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if opts.TailLines != nil && *opts.TailLines < 0 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("tailLines"), *opts.TailLines, isNegativeErrorMsg))
	}
	if opts.LimitBytes != nil && *opts.LimitBytes < 1 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("limitBytes"), *opts.LimitBytes, "must be greater than 0"))
	}
	switch {
	case opts.SinceSeconds != nil && opts.SinceTime != nil:
		allErrs = append(allErrs, field.Forbidden(field.NewPath(""), "at most one of `sinceTime` or `sinceSeconds` may be specified"))
	case opts.SinceSeconds != nil:
		if *opts.SinceSeconds < 1 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("sinceSeconds"), *opts.SinceSeconds, "must be greater than 0"))
		}
	}
	return allErrs
}

// ValidateLoadBalancerStatus validates required fields on a LoadBalancerStatus
func ValidateLoadBalancerStatus(status *corev1.LoadBalancerStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, ingress := range status.Ingress {
		idxPath := fldPath.Child("ingress").Index(i)
		if len(ingress.IP) > 0 {
			if isIP := (netutils.ParseIPSloppy(ingress.IP) != nil); !isIP {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("ip"), ingress.IP, "must be a valid IP address"))
			}
		}
		if len(ingress.Hostname) > 0 {
			for _, msg := range validation.IsDNS1123Subdomain(ingress.Hostname) {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("hostname"), ingress.Hostname, msg))
			}
			if isIP := (netutils.ParseIPSloppy(ingress.Hostname) != nil); isIP {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("hostname"), ingress.Hostname, "must be a DNS name, not an IP address"))
			}
		}
	}
	return allErrs
}

// validateVolumeNodeAffinity tests that the PersistentVolume.NodeAffinity has valid data
// returns:
// - true if volumeNodeAffinity is set
// - errorList if there are validation errors
func validateVolumeNodeAffinity(nodeAffinity *corev1.VolumeNodeAffinity, fldPath *field.Path) (bool, field.ErrorList) {
	allErrs := field.ErrorList{}

	if nodeAffinity == nil {
		return false, allErrs
	}

	if nodeAffinity.Required != nil {
		allErrs = append(allErrs, ValidateNodeSelector(nodeAffinity.Required, fldPath.Child("required"))...)
	} else {
		allErrs = append(allErrs, field.Required(fldPath.Child("required"), "must specify required node constraints"))
	}

	return true, allErrs
}

// ValidateCIDR validates whether a CIDR matches the conventions expected by net.ParseCIDR
func ValidateCIDR(cidr string) (*net.IPNet, error) {
	_, net, err := netutils.ParseCIDRSloppy(cidr)
	if err != nil {
		return nil, err
	}
	return net, nil
}

func IsDecremented(update, old *int32) bool {
	if update == nil && old != nil {
		return true
	}
	if update == nil || old == nil {
		return false
	}
	return *update < *old
}

// ValidateProcMountType tests that the argument is a valid ProcMountType.
func ValidateProcMountType(fldPath *field.Path, procMountType corev1.ProcMountType) *field.Error {
	switch procMountType {
	case corev1.DefaultProcMount, corev1.UnmaskedProcMount:
		return nil
	default:
		return field.NotSupported(fldPath, procMountType, []string{string(corev1.DefaultProcMount), string(corev1.UnmaskedProcMount)})
	}
}

var (
	supportedScheduleActions = sets.NewString(string(corev1.DoNotSchedule), string(corev1.ScheduleAnyway))
)

// validateTopologySpreadConstraints validates given TopologySpreadConstraints.
func validateTopologySpreadConstraints(constraints []corev1.TopologySpreadConstraint, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for i, constraint := range constraints {
		subFldPath := fldPath.Index(i)
		if err := ValidateMaxSkew(subFldPath.Child("maxSkew"), constraint.MaxSkew); err != nil {
			allErrs = append(allErrs, err)
		}
		if err := ValidateTopologyKey(subFldPath.Child("topologyKey"), constraint.TopologyKey); err != nil {
			allErrs = append(allErrs, err)
		}
		if err := ValidateWhenUnsatisfiable(subFldPath.Child("whenUnsatisfiable"), constraint.WhenUnsatisfiable); err != nil {
			allErrs = append(allErrs, err)
		}
		// tuple {topologyKey, whenUnsatisfiable} denotes one kind of spread constraint
		if err := ValidateSpreadConstraintNotRepeat(subFldPath.Child("{topologyKey, whenUnsatisfiable}"), constraint, constraints[i+1:]); err != nil {
			allErrs = append(allErrs, err)
		}
		allErrs = append(allErrs, validateMinDomains(subFldPath.Child("minDomains"), constraint.MinDomains, constraint.WhenUnsatisfiable)...)
		if err := validateNodeInclusionPolicy(subFldPath.Child("nodeAffinityPolicy"), constraint.NodeAffinityPolicy); err != nil {
			allErrs = append(allErrs, err)
		}
		if err := validateNodeInclusionPolicy(subFldPath.Child("nodeTaintsPolicy"), constraint.NodeTaintsPolicy); err != nil {
			allErrs = append(allErrs, err)
		}
		allErrs = append(allErrs, validateMatchLabelKeys(subFldPath.Child("matchLabelKeys"), constraint.MatchLabelKeys, constraint.LabelSelector)...)
	}

	return allErrs
}

// ValidateMaxSkew tests that the argument is a valid MaxSkew.
func ValidateMaxSkew(fldPath *field.Path, maxSkew int32) *field.Error {
	if maxSkew <= 0 {
		return field.Invalid(fldPath, maxSkew, isNotPositiveErrorMsg)
	}
	return nil
}

// validateMinDomains tests that the argument is a valid MinDomains.
func validateMinDomains(fldPath *field.Path, minDomains *int32, action corev1.UnsatisfiableConstraintAction) field.ErrorList {
	if minDomains == nil {
		return nil
	}
	var allErrs field.ErrorList
	if *minDomains <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, minDomains, isNotPositiveErrorMsg))
	}
	// When MinDomains is non-nil, whenUnsatisfiable must be DoNotSchedule.
	if action != corev1.DoNotSchedule {
		allErrs = append(allErrs, field.Invalid(fldPath, minDomains, fmt.Sprintf("can only use minDomains if whenUnsatisfiable=%s, not %s", string(corev1.DoNotSchedule), string(action))))
	}
	return allErrs
}

// ValidateTopologyKey tests that the argument is a valid TopologyKey.
func ValidateTopologyKey(fldPath *field.Path, topologyKey string) *field.Error {
	if len(topologyKey) == 0 {
		return field.Required(fldPath, "can not be empty")
	}
	return nil
}

// ValidateWhenUnsatisfiable tests that the argument is a valid UnsatisfiableConstraintAction.
func ValidateWhenUnsatisfiable(fldPath *field.Path, action corev1.UnsatisfiableConstraintAction) *field.Error {
	if !supportedScheduleActions.Has(string(action)) {
		return field.NotSupported(fldPath, action, supportedScheduleActions.List())
	}
	return nil
}

// ValidateSpreadConstraintNotRepeat tests that if `constraint` duplicates with `existingConstraintPairs`
// on TopologyKey and WhenUnsatisfiable fields.
func ValidateSpreadConstraintNotRepeat(fldPath *field.Path, constraint corev1.TopologySpreadConstraint, restingConstraints []corev1.TopologySpreadConstraint) *field.Error {
	for _, restingConstraint := range restingConstraints {
		if constraint.TopologyKey == restingConstraint.TopologyKey &&
			constraint.WhenUnsatisfiable == restingConstraint.WhenUnsatisfiable {
			return field.Duplicate(fldPath, fmt.Sprintf("{%v, %v}", constraint.TopologyKey, constraint.WhenUnsatisfiable))
		}
	}
	return nil
}

var (
	supportedPodTopologySpreadNodePolicies = sets.NewString(string(corev1.NodeInclusionPolicyIgnore), string(corev1.NodeInclusionPolicyHonor))
)

// validateNodeAffinityPolicy tests that the argument is a valid NodeInclusionPolicy.
func validateNodeInclusionPolicy(fldPath *field.Path, policy *corev1.NodeInclusionPolicy) *field.Error {
	if policy == nil {
		return nil
	}

	if !supportedPodTopologySpreadNodePolicies.Has(string(*policy)) {
		return field.NotSupported(fldPath, policy, supportedPodTopologySpreadNodePolicies.List())
	}
	return nil
}

// validateMatchLabelKeys tests that the elements are a valid label name and are not already included in labelSelector.
func validateMatchLabelKeys(fldPath *field.Path, matchLabelKeys []string, labelSelector *metav1.LabelSelector) field.ErrorList {
	if len(matchLabelKeys) == 0 {
		return nil
	}

	labelSelectorKeys := sets.String{}
	if labelSelector != nil {
		for key := range labelSelector.MatchLabels {
			labelSelectorKeys.Insert(key)
		}
		for _, matchExpression := range labelSelector.MatchExpressions {
			labelSelectorKeys.Insert(matchExpression.Key)
		}
	}

	allErrs := field.ErrorList{}
	for i, key := range matchLabelKeys {
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelName(key, fldPath.Index(i))...)
		if labelSelectorKeys.Has(key) {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), key, "exists in both matchLabelKeys and labelSelector"))
		}
	}

	return allErrs
}

// ValidateServiceClusterIPsRelatedFields validates .spec.ClusterIPs,,
// .spec.IPFamilies, .spec.ipFamilyPolicy.  This is exported because it is used
// during IP init and allocation.
func ValidateServiceClusterIPsRelatedFields(service *corev1.Service) field.ErrorList {
	// ClusterIP, ClusterIPs, IPFamilyPolicy and IPFamilies are validated prior (all must be unset) for ExternalName service
	if service.Spec.Type == corev1.ServiceTypeExternalName {
		return field.ErrorList{}
	}

	allErrs := field.ErrorList{}
	hasInvalidIPs := false

	specPath := field.NewPath("spec")
	clusterIPsField := specPath.Child("clusterIPs")
	ipFamiliesField := specPath.Child("ipFamilies")
	ipFamilyPolicyField := specPath.Child("ipFamilyPolicy")

	// Make sure ClusterIP and ClusterIPs are synced.  For most cases users can
	// just manage one or the other and we'll handle the rest (see PrepareFor*
	// in strategy).
	if len(service.Spec.ClusterIP) != 0 {
		// If ClusterIP is set, ClusterIPs[0] must match.
		if len(service.Spec.ClusterIPs) == 0 {
			allErrs = append(allErrs, field.Required(clusterIPsField, ""))
		} else if service.Spec.ClusterIPs[0] != service.Spec.ClusterIP {
			allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "first value must match `clusterIP`"))
		}
	} else { // ClusterIP == ""
		// If ClusterIP is not set, ClusterIPs must also be unset.
		if len(service.Spec.ClusterIPs) != 0 {
			allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "must be empty when `clusterIP` is not specified"))
		}
	}

	// ipfamilies stand alone validation
	// must be either IPv4 or IPv6
	seen := sets.String{}
	for i, ipFamily := range service.Spec.IPFamilies {
		if !supportedServiceIPFamily.Has(string(ipFamily)) {
			allErrs = append(allErrs, field.NotSupported(ipFamiliesField.Index(i), ipFamily, supportedServiceIPFamily.List()))
		}
		// no duplicate check also ensures that ipfamilies is dualstacked, in any order
		if seen.Has(string(ipFamily)) {
			allErrs = append(allErrs, field.Duplicate(ipFamiliesField.Index(i), ipFamily))
		}
		seen.Insert(string(ipFamily))
	}

	// IPFamilyPolicy stand alone validation
	// note: nil is ok, defaulted in alloc check registry/core/service/*
	if service.Spec.IPFamilyPolicy != nil {
		// must have a supported value
		if !supportedServiceIPFamilyPolicy.Has(string(*(service.Spec.IPFamilyPolicy))) {
			allErrs = append(allErrs, field.NotSupported(ipFamilyPolicyField, service.Spec.IPFamilyPolicy, supportedServiceIPFamilyPolicy.List()))
		}
	}

	// clusterIPs stand alone validation
	// valid ips with None and empty string handling
	// duplication check is done as part of DualStackvalidation below
	for i, clusterIP := range service.Spec.ClusterIPs {
		// valid at first location only. if and only if len(clusterIPs) == 1
		if i == 0 && clusterIP == corev1.ClusterIPNone {
			if len(service.Spec.ClusterIPs) > 1 {
				hasInvalidIPs = true
				allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "'None' must be the first and only value"))
			}
			continue
		}

		// is it valid ip?
		errorMessages := validation.IsValidIP(clusterIP)
		hasInvalidIPs = (len(errorMessages) != 0) || hasInvalidIPs
		for _, msg := range errorMessages {
			allErrs = append(allErrs, field.Invalid(clusterIPsField.Index(i), clusterIP, msg))
		}
	}

	// max two
	if len(service.Spec.ClusterIPs) > 2 {
		allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "may only hold up to 2 values"))
	}

	// at this stage if there is an invalid ip or misplaced none/empty string
	// it will skew the error messages (bad index || dualstackness of already bad ips). so we
	// stop here if there are errors in clusterIPs validation
	if hasInvalidIPs {
		return allErrs
	}

	// must be dual stacked ips if they are more than one ip
	if len(service.Spec.ClusterIPs) > 1 /* meaning: it does not have a None or empty string */ {
		dualStack, err := netutils.IsDualStackIPStrings(service.Spec.ClusterIPs)
		if err != nil { // though we check for that earlier. safe > sorry
			allErrs = append(allErrs, field.InternalError(clusterIPsField, fmt.Errorf("failed to check for dual stack with error:%v", err)))
		}

		// We only support one from each IP family (i.e. max two IPs in this list).
		if !dualStack {
			allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "may specify no more than one IP for each IP family"))
		}
	}

	// match clusterIPs to their families, if they were provided
	if !isHeadlessService(service) && len(service.Spec.ClusterIPs) > 0 && len(service.Spec.IPFamilies) > 0 {
		for i, ip := range service.Spec.ClusterIPs {
			if i > (len(service.Spec.IPFamilies) - 1) {
				break // no more families to check
			}

			// 4=>6
			if service.Spec.IPFamilies[i] == corev1.IPv4Protocol && netutils.IsIPv6String(ip) {
				allErrs = append(allErrs, field.Invalid(clusterIPsField.Index(i), ip, fmt.Sprintf("expected an IPv4 value as indicated by `ipFamilies[%v]`", i)))
			}
			// 6=>4
			if service.Spec.IPFamilies[i] == corev1.IPv6Protocol && !netutils.IsIPv6String(ip) {
				allErrs = append(allErrs, field.Invalid(clusterIPsField.Index(i), ip, fmt.Sprintf("expected an IPv6 value as indicated by `ipFamilies[%v]`", i)))
			}
		}
	}

	return allErrs
}

// specific validation for clusterIPs in cases of user upgrading or downgrading to/from dualstack
func validateUpgradeDowngradeClusterIPs(oldService, service *corev1.Service) field.ErrorList {
	allErrs := make(field.ErrorList, 0)

	// bail out early for ExternalName
	if service.Spec.Type == corev1.ServiceTypeExternalName || oldService.Spec.Type == corev1.ServiceTypeExternalName {
		return allErrs
	}
	newIsHeadless := isHeadlessService(service)
	oldIsHeadless := isHeadlessService(oldService)

	if oldIsHeadless && newIsHeadless {
		return allErrs
	}

	switch {
	// no change in ClusterIP lengths
	// compare each
	case len(oldService.Spec.ClusterIPs) == len(service.Spec.ClusterIPs):
		for i, ip := range oldService.Spec.ClusterIPs {
			if ip != service.Spec.ClusterIPs[i] {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "clusterIPs").Index(i), service.Spec.ClusterIPs, "may not change once set"))
			}
		}

	// something has been released (downgraded)
	case len(oldService.Spec.ClusterIPs) > len(service.Spec.ClusterIPs):
		// primary ClusterIP has been released
		if len(service.Spec.ClusterIPs) == 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "clusterIPs").Index(0), service.Spec.ClusterIPs, "primary clusterIP can not be unset"))
		}

		// test if primary clusterIP has changed
		if len(oldService.Spec.ClusterIPs) > 0 &&
			len(service.Spec.ClusterIPs) > 0 &&
			service.Spec.ClusterIPs[0] != oldService.Spec.ClusterIPs[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "clusterIPs").Index(0), service.Spec.ClusterIPs, "may not change once set"))
		}

		// test if secondary ClusterIP has been released. has this service been downgraded correctly?
		// user *must* set IPFamilyPolicy == SingleStack
		if len(service.Spec.ClusterIPs) == 1 {
			if service.Spec.IPFamilyPolicy == nil || *(service.Spec.IPFamilyPolicy) != corev1.IPFamilyPolicySingleStack {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy, "must be set to 'SingleStack' when releasing the secondary clusterIP"))
			}
		}
	case len(oldService.Spec.ClusterIPs) < len(service.Spec.ClusterIPs):
		// something has been added (upgraded)
		// test if primary clusterIP has changed
		if len(oldService.Spec.ClusterIPs) > 0 &&
			service.Spec.ClusterIPs[0] != oldService.Spec.ClusterIPs[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "clusterIPs").Index(0), service.Spec.ClusterIPs, "may not change once set"))
		}
		// we don't check for Policy == RequireDualStack here since, Validation/Creation func takes care of it
	}
	return allErrs
}

// specific validation for ipFamilies in cases of user upgrading or downgrading to/from dualstack
func validateUpgradeDowngradeIPFamilies(oldService, service *corev1.Service) field.ErrorList {
	allErrs := make(field.ErrorList, 0)
	// bail out early for ExternalName
	if service.Spec.Type == corev1.ServiceTypeExternalName || oldService.Spec.Type == corev1.ServiceTypeExternalName {
		return allErrs
	}

	oldIsHeadless := isHeadlessService(oldService)
	newIsHeadless := isHeadlessService(service)

	// if changed to/from headless, then bail out
	if newIsHeadless != oldIsHeadless {
		return allErrs
	}
	// headless can change families
	if newIsHeadless {
		return allErrs
	}

	switch {
	case len(oldService.Spec.IPFamilies) == len(service.Spec.IPFamilies):
		// no change in ClusterIP lengths
		// compare each

		for i, ip := range oldService.Spec.IPFamilies {
			if ip != service.Spec.IPFamilies[i] {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilies").Index(0), service.Spec.IPFamilies, "may not change once set"))
			}
		}

	case len(oldService.Spec.IPFamilies) > len(service.Spec.IPFamilies):
		// something has been released (downgraded)

		// test if primary ipfamily has been released
		if len(service.Spec.ClusterIPs) == 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilies").Index(0), service.Spec.IPFamilies, "primary ipFamily can not be unset"))
		}

		// test if primary ipFamily has changed
		if len(service.Spec.IPFamilies) > 0 &&
			service.Spec.IPFamilies[0] != oldService.Spec.IPFamilies[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilies").Index(0), service.Spec.ClusterIPs, "may not change once set"))
		}

		// test if secondary IPFamily has been released. has this service been downgraded correctly?
		// user *must* set IPFamilyPolicy == SingleStack
		if len(service.Spec.IPFamilies) == 1 {
			if service.Spec.IPFamilyPolicy == nil || *(service.Spec.IPFamilyPolicy) != corev1.IPFamilyPolicySingleStack {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy, "must be set to 'SingleStack' when releasing the secondary ipFamily"))
			}
		}
	case len(oldService.Spec.IPFamilies) < len(service.Spec.IPFamilies):
		// something has been added (upgraded)

		// test if primary ipFamily has changed
		if len(oldService.Spec.IPFamilies) > 0 &&
			len(service.Spec.IPFamilies) > 0 &&
			service.Spec.IPFamilies[0] != oldService.Spec.IPFamilies[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilies").Index(0), service.Spec.ClusterIPs, "may not change once set"))
		}
		// we don't check for Policy == RequireDualStack here since, Validation/Creation func takes care of it
	}
	return allErrs
}

func isHeadlessService(service *corev1.Service) bool {
	return service != nil &&
		len(service.Spec.ClusterIPs) == 1 &&
		service.Spec.ClusterIPs[0] == corev1.ClusterIPNone
}

// validateLoadBalancerClassField validation for loadBalancerClass
func validateLoadBalancerClassField(oldService, service *corev1.Service) field.ErrorList {
	allErrs := make(field.ErrorList, 0)
	if oldService != nil {
		// validate update op
		if isTypeLoadBalancer(oldService) && isTypeLoadBalancer(service) {
			// old and new are both LoadBalancer
			if !sameLoadBalancerClass(oldService, service) {
				// can't change loadBalancerClass
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "loadBalancerClass"), service.Spec.LoadBalancerClass, "may not change once set"))
			}
		}
	}

	if isTypeLoadBalancer(service) {
		// check LoadBalancerClass format
		if service.Spec.LoadBalancerClass != nil {
			allErrs = append(allErrs, ValidateQualifiedName(*service.Spec.LoadBalancerClass, field.NewPath("spec", "loadBalancerClass"))...)
		}
	} else {
		// check if LoadBalancerClass set for non LoadBalancer type of service
		if service.Spec.LoadBalancerClass != nil {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "loadBalancerClass"), "may only be used when `type` is 'LoadBalancer'"))
		}
	}
	return allErrs
}

// isTypeLoadBalancer tests service type is loadBalancer or not
func isTypeLoadBalancer(service *corev1.Service) bool {
	return service.Spec.Type == corev1.ServiceTypeLoadBalancer
}

// sameLoadBalancerClass check two services have the same loadBalancerClass or not
func sameLoadBalancerClass(oldService, service *corev1.Service) bool {
	if oldService.Spec.LoadBalancerClass == nil && service.Spec.LoadBalancerClass == nil {
		return true
	}
	if oldService.Spec.LoadBalancerClass == nil || service.Spec.LoadBalancerClass == nil {
		return false
	}
	return *oldService.Spec.LoadBalancerClass == *service.Spec.LoadBalancerClass
}

func ValidatePodAffinityTermSelector(podAffinityTerm corev1.PodAffinityTerm, allowInvalidLabelValueInSelector bool, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	labelSelectorValidationOptions := unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: allowInvalidLabelValueInSelector}
	allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(podAffinityTerm.LabelSelector, labelSelectorValidationOptions, fldPath.Child("labelSelector"))...)
	allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(podAffinityTerm.NamespaceSelector, labelSelectorValidationOptions, fldPath.Child("namespaceSelector"))...)
	return allErrs
}
