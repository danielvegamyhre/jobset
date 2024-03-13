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
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

// maxParallelismForIndexJob is the maximum parallelism that an Indexed Job
// is allowed to have. This threshold allows to cap the length of
// .status.completedIndexes.
const maxParallelismForIndexedJob = 100000

const (
	// maximum number of rules in pod failure policy
	maxPodFailurePolicyRules = 20

	// maximum number of values for a OnExitCodes requirement in pod failure policy
	maxPodFailurePolicyOnExitCodesValues = 255

	// maximum number of patterns for a OnPodConditions requirement in pod failure policy
	maxPodFailurePolicyOnPodConditionsPatterns = 20
)

var (
	supportedPodFailurePolicyActions sets.String = sets.NewString(
		string(batchv1.PodFailurePolicyActionCount),
		string(batchv1.PodFailurePolicyActionFailJob),
		string(batchv1.PodFailurePolicyActionIgnore))

	supportedPodFailurePolicyOnExitCodesOperator sets.String = sets.NewString(
		string(batchv1.PodFailurePolicyOnExitCodesOpIn),
		string(batchv1.PodFailurePolicyOnExitCodesOpNotIn))

	supportedPodFailurePolicyOnPodConditionsStatus sets.String = sets.NewString(
		string(corev1.ConditionFalse),
		string(corev1.ConditionTrue),
		string(corev1.ConditionUnknown))
)

func validateJobTemplateSpec(spec *batchv1.JobTemplateSpec, fldPath *field.Path) ([]error, error) {
	return validateJobSpec(&spec.Spec, fldPath.Child("spec"))
}

// validateJobSpec includes most of the validation logic from upstream Batch validation in 1.26 (oldest
// k8s minor version JobSet supports) from here:
// https://github.com/kubernetes/kubernetes/blob/5e75c65d899846e423181393404586e9ff9aeb68/pkg/apis/batch/validation/validation.go#L168
// TODO: use exported validation logic if/when it becomes available upstream.
func validateJobSpec(spec *batchv1.JobSpec, fldPath *field.Path) ([]error, error) {
	var allErrs []error
	if spec.Parallelism != nil {
		allErrs = append(allErrs, validateNonNegativeField(int64(*spec.Parallelism), fldPath.Child("parallelism")))
	}
	if spec.Completions != nil {
		allErrs = append(allErrs, validateNonNegativeField(int64(*spec.Completions), fldPath.Child("completions")))
	}
	if spec.ActiveDeadlineSeconds != nil {
		allErrs = append(allErrs, validateNonNegativeField(int64(*spec.ActiveDeadlineSeconds), fldPath.Child("activeDeadlineSeconds")))
	}
	if spec.BackoffLimit != nil {
		allErrs = append(allErrs, validateNonNegativeField(int64(*spec.BackoffLimit), fldPath.Child("backoffLimit")))
	}
	if spec.TTLSecondsAfterFinished != nil {
		allErrs = append(allErrs, validateNonNegativeField(int64(*spec.TTLSecondsAfterFinished), fldPath.Child("ttlSecondsAfterFinished")))
	}
	if spec.CompletionMode != nil {
		if *spec.CompletionMode != batchv1.NonIndexedCompletion && *spec.CompletionMode != batchv1.IndexedCompletion {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("completionMode"), spec.CompletionMode, []string{string(batchv1.NonIndexedCompletion), string(batchv1.IndexedCompletion)}))
		}
		if *spec.CompletionMode == batchv1.IndexedCompletion {
			if spec.Completions == nil {
				allErrs = append(allErrs, field.Required(fldPath.Child("completions"), fmt.Sprintf("when completion mode is %s", batchv1.IndexedCompletion)))
			}
			if spec.Parallelism != nil && *spec.Parallelism > maxParallelismForIndexedJob {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("parallelism"), *spec.Parallelism, fmt.Sprintf("must be less than or equal to %d when completion mode is %s", maxParallelismForIndexedJob, batchv1.IndexedCompletion)))
			}
		}
	}

	if spec.PodFailurePolicy != nil {
		allErrs = append(allErrs, validatePodFailurePolicy(spec, fldPath.Child("podFailurePolicy"))...)
	}

	allErrs = append(allErrs, apivalidation.ValidatePodTemplateSpec(&spec.Template, fldPath.Child("template"))...)

	// spec.Template.Spec.RestartPolicy can be defaulted as RestartPolicyAlways
	// by SetDefaults_PodSpec function when the user does not explicitly specify a value for it,
	// so we check both empty and RestartPolicyAlways cases here
	if spec.Template.Spec.RestartPolicy == corev1.RestartPolicyAlways || spec.Template.Spec.RestartPolicy == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("template", "spec", "restartPolicy"),
			fmt.Sprintf("valid values: %q, %q", corev1.RestartPolicyOnFailure, corev1.RestartPolicyNever)))
	} else if spec.Template.Spec.RestartPolicy != corev1.RestartPolicyOnFailure && spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("template", "spec", "restartPolicy"),
			spec.Template.Spec.RestartPolicy, []string{string(corev1.RestartPolicyOnFailure), string(corev1.RestartPolicyNever)}))
	} else if spec.PodFailurePolicy != nil && spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("template", "spec", "restartPolicy"),
			spec.Template.Spec.RestartPolicy, fmt.Sprintf("only %q is supported when podFailurePolicy is specified", corev1.RestartPolicyNever)))
	}
	return allErrs, nil
}

func validatePodFailurePolicy(spec *batchv1.JobSpec, fldPath *field.Path) []error {
	var allErrs []error
	rulesPath := fldPath.Child("rules")
	if len(spec.PodFailurePolicy.Rules) > maxPodFailurePolicyRules {
		allErrs = append(allErrs, field.TooMany(rulesPath, len(spec.PodFailurePolicy.Rules), maxPodFailurePolicyRules))
	}
	containerNames := sets.NewString()
	for _, containerSpec := range spec.Template.Spec.Containers {
		containerNames.Insert(containerSpec.Name)
	}
	for _, containerSpec := range spec.Template.Spec.InitContainers {
		containerNames.Insert(containerSpec.Name)
	}
	for i, rule := range spec.PodFailurePolicy.Rules {
		allErrs = append(allErrs, validatePodFailurePolicyRule(&rule, rulesPath.Index(i), containerNames)...)
	}
	return allErrs
}

func validatePodFailurePolicyRule(rule *batchv1.PodFailurePolicyRule, rulePath *field.Path, containerNames sets.String) []error {
	var allErrs []error
	actionPath := rulePath.Child("action")
	if rule.Action == "" {
		allErrs = append(allErrs, field.Required(actionPath, fmt.Sprintf("valid values: %q", supportedPodFailurePolicyActions.List())))
	} else if !supportedPodFailurePolicyActions.Has(string(rule.Action)) {
		allErrs = append(allErrs, field.NotSupported(actionPath, rule.Action, supportedPodFailurePolicyActions.List()))
	}
	if rule.OnExitCodes != nil {
		allErrs = append(allErrs, validatePodFailurePolicyRuleOnExitCodes(rule.OnExitCodes, rulePath.Child("onExitCodes"), containerNames)...)
	}
	if len(rule.OnPodConditions) > 0 {
		allErrs = append(allErrs, validatePodFailurePolicyRuleOnPodConditions(rule.OnPodConditions, rulePath.Child("onPodConditions"))...)
	}
	if rule.OnExitCodes != nil && len(rule.OnPodConditions) > 0 {
		allErrs = append(allErrs, field.Invalid(rulePath, field.OmitValueType{}, "specifying both OnExitCodes and OnPodConditions is not supported"))
	}
	if rule.OnExitCodes == nil && len(rule.OnPodConditions) == 0 {
		allErrs = append(allErrs, field.Invalid(rulePath, field.OmitValueType{}, "specifying one of OnExitCodes and OnPodConditions is required"))
	}
	return allErrs
}

func validatePodFailurePolicyRuleOnPodConditions(onPodConditions []batchv1.PodFailurePolicyOnPodConditionsPattern, onPodConditionsPath *field.Path) []error {
	var allErrs []error
	if len(onPodConditions) > maxPodFailurePolicyOnPodConditionsPatterns {
		allErrs = append(allErrs, field.TooMany(onPodConditionsPath, len(onPodConditions), maxPodFailurePolicyOnPodConditionsPatterns))
	}
	for j, pattern := range onPodConditions {
		patternPath := onPodConditionsPath.Index(j)
		statusPath := patternPath.Child("status")
		allErrs = append(allErrs, apivalidation.ValidateQualifiedName(string(pattern.Type), patternPath.Child("type"))...)
		if pattern.Status == "" {
			allErrs = append(allErrs, field.Required(statusPath, fmt.Sprintf("valid values: %q", supportedPodFailurePolicyOnPodConditionsStatus.List())))
		} else if !supportedPodFailurePolicyOnPodConditionsStatus.Has(string(pattern.Status)) {
			allErrs = append(allErrs, field.NotSupported(statusPath, pattern.Status, supportedPodFailurePolicyOnPodConditionsStatus.List()))
		}
	}
	return allErrs
}

func validatePodFailurePolicyRuleOnExitCodes(onExitCode *batchv1.PodFailurePolicyOnExitCodesRequirement, onExitCodesPath *field.Path, containerNames sets.String) []error {
	var allErrs []error
	operatorPath := onExitCodesPath.Child("operator")
	if onExitCode.Operator == "" {
		allErrs = append(allErrs, field.Required(operatorPath, fmt.Sprintf("valid values: %q", supportedPodFailurePolicyOnExitCodesOperator.List())))
	} else if !supportedPodFailurePolicyOnExitCodesOperator.Has(string(onExitCode.Operator)) {
		allErrs = append(allErrs, field.NotSupported(operatorPath, onExitCode.Operator, supportedPodFailurePolicyOnExitCodesOperator.List()))
	}
	if onExitCode.ContainerName != nil && !containerNames.Has(*onExitCode.ContainerName) {
		allErrs = append(allErrs, field.Invalid(onExitCodesPath.Child("containerName"), *onExitCode.ContainerName, "must be one of the container or initContainer names in the pod template"))
	}
	valuesPath := onExitCodesPath.Child("values")
	if len(onExitCode.Values) == 0 {
		allErrs = append(allErrs, field.Invalid(valuesPath, onExitCode.Values, "at least one value is required"))
	} else if len(onExitCode.Values) > maxPodFailurePolicyOnExitCodesValues {
		allErrs = append(allErrs, field.TooMany(valuesPath, len(onExitCode.Values), maxPodFailurePolicyOnExitCodesValues))
	}
	isOrdered := true
	uniqueValues := sets.NewInt32()
	for j, exitCodeValue := range onExitCode.Values {
		valuePath := valuesPath.Index(j)
		if onExitCode.Operator == batchv1.PodFailurePolicyOnExitCodesOpIn && exitCodeValue == 0 {
			allErrs = append(allErrs, field.Invalid(valuePath, exitCodeValue, "must not be 0 for the In operator"))
		}
		if uniqueValues.Has(exitCodeValue) {
			allErrs = append(allErrs, field.Duplicate(valuePath, exitCodeValue))
		} else {
			uniqueValues.Insert(exitCodeValue)
		}
		if j > 0 && onExitCode.Values[j-1] > exitCodeValue {
			isOrdered = false
		}
	}
	if !isOrdered {
		allErrs = append(allErrs, field.Invalid(valuesPath, onExitCode.Values, "must be ordered"))
	}

	return allErrs
}

func validateNonNegativeField(value int64, fldPath *field.Path) error {
	if value < 0 {
		return field.Invalid(fldPath, value, "must be non-negative")
	}
	return nil
}
