//go:build !ignore_autogenerated
// +build !ignore_autogenerated

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

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha2

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *FailurePolicy) DeepCopyInto(out *FailurePolicy) {
	*out = *in
	if in.Rules != nil {
		in, out := &in.Rules, &out.Rules
		*out = make([]FailurePolicyRule, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new FailurePolicy.
func (in *FailurePolicy) DeepCopy() *FailurePolicy {
	if in == nil {
		return nil
	}
	out := new(FailurePolicy)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *FailurePolicyRule) DeepCopyInto(out *FailurePolicyRule) {
	*out = *in
	if in.OnJobFailureReasons != nil {
		in, out := &in.OnJobFailureReasons, &out.OnJobFailureReasons
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new FailurePolicyRule.
func (in *FailurePolicyRule) DeepCopy() *FailurePolicyRule {
	if in == nil {
		return nil
	}
	out := new(FailurePolicyRule)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *JobSet) DeepCopyInto(out *JobSet) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new JobSet.
func (in *JobSet) DeepCopy() *JobSet {
	if in == nil {
		return nil
	}
	out := new(JobSet)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *JobSet) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *JobSetList) DeepCopyInto(out *JobSetList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]JobSet, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new JobSetList.
func (in *JobSetList) DeepCopy() *JobSetList {
	if in == nil {
		return nil
	}
	out := new(JobSetList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *JobSetList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *JobSetSpec) DeepCopyInto(out *JobSetSpec) {
	*out = *in
	if in.ReplicatedJobs != nil {
		in, out := &in.ReplicatedJobs, &out.ReplicatedJobs
		*out = make([]ReplicatedJob, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.Network != nil {
		in, out := &in.Network, &out.Network
		*out = new(Network)
		(*in).DeepCopyInto(*out)
	}
	if in.SuccessPolicy != nil {
		in, out := &in.SuccessPolicy, &out.SuccessPolicy
		*out = new(SuccessPolicy)
		(*in).DeepCopyInto(*out)
	}
	if in.FailurePolicy != nil {
		in, out := &in.FailurePolicy, &out.FailurePolicy
		*out = new(FailurePolicy)
		(*in).DeepCopyInto(*out)
	}
	if in.Suspend != nil {
		in, out := &in.Suspend, &out.Suspend
		*out = new(bool)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new JobSetSpec.
func (in *JobSetSpec) DeepCopy() *JobSetSpec {
	if in == nil {
		return nil
	}
	out := new(JobSetSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *JobSetStatus) DeepCopyInto(out *JobSetStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]v1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.ReplicatedJobsStatus != nil {
		in, out := &in.ReplicatedJobsStatus, &out.ReplicatedJobsStatus
		*out = make([]ReplicatedJobStatus, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new JobSetStatus.
func (in *JobSetStatus) DeepCopy() *JobSetStatus {
	if in == nil {
		return nil
	}
	out := new(JobSetStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Network) DeepCopyInto(out *Network) {
	*out = *in
	if in.EnableDNSHostnames != nil {
		in, out := &in.EnableDNSHostnames, &out.EnableDNSHostnames
		*out = new(bool)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Network.
func (in *Network) DeepCopy() *Network {
	if in == nil {
		return nil
	}
	out := new(Network)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReplicatedJob) DeepCopyInto(out *ReplicatedJob) {
	*out = *in
	in.Template.DeepCopyInto(&out.Template)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReplicatedJob.
func (in *ReplicatedJob) DeepCopy() *ReplicatedJob {
	if in == nil {
		return nil
	}
	out := new(ReplicatedJob)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReplicatedJobStatus) DeepCopyInto(out *ReplicatedJobStatus) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReplicatedJobStatus.
func (in *ReplicatedJobStatus) DeepCopy() *ReplicatedJobStatus {
	if in == nil {
		return nil
	}
	out := new(ReplicatedJobStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SuccessPolicy) DeepCopyInto(out *SuccessPolicy) {
	*out = *in
	if in.TargetReplicatedJobs != nil {
		in, out := &in.TargetReplicatedJobs, &out.TargetReplicatedJobs
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SuccessPolicy.
func (in *SuccessPolicy) DeepCopy() *SuccessPolicy {
	if in == nil {
		return nil
	}
	out := new(SuccessPolicy)
	in.DeepCopyInto(out)
	return out
}
