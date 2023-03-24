/*
Copyright 2023.

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
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	jobsetv1 "tutorial.kubebuilder.io/project/api/v1"
)

// JobSetReconciler reconciles a JobSet object
type JobSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

var (
	jobOwnerKey             = ".metadata.controller"
	apiGVStr                = jobsetv1.GroupVersion.String()
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
)

type realClock struct{}

func (_ realClock) Now() time.Time { return time.Now() }

// clock knows how to get the current time.
// It can be used to fake out timing for testing.
type Clock interface {
	Now() time.Time
}

//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=jobsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=jobsets/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=jobsets/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the JobSet object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *JobSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var jobSet jobsetv1.JobSet
	if err := r.Get(ctx, req.NamespacedName, &jobSet); err != nil {
		log.Error(err, "unable to fetch JobSet")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// List all active jobs and update the status.
	var childJobs batchv1.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to list child Jobs")
		return ctrl.Result{}, err
	}

	// find the active list of jobs
	var activeJobs []*batchv1.Job
	var successfulJobs []*batchv1.Job
	var failedJobs []*batchv1.Job

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": // ongoing
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case batchv1.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case batchv1.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
		}
	}

	// Update jobSet with its active jobs.
	jobSet.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		jobSet.Status.Active = append(jobSet.Status.Active, *jobRef)
	}

	log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	// Update status of CRD
	if err := r.Status().Update(ctx, &jobSet); err != nil {
		log.Error(err, "unable to update JobSet status")
		return ctrl.Result{}, err
	}

	r.cleanUpOldJobs(ctx, failedJobs, successfulJobs, log)

	// Each job should only start when the previous job is ready.
	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		// Only start job if previous job is ready and this job is not yet active (i.e. finishedType == "")
		if i > 0 && childJobs.Items[i-1].Status.Ready != nil && finishedType != "" {
			newJob, err := r.constructJobFromTemplate(&jobSet, childJobs, i)
			if err != nil {
				log.Error(err, "error constructing job from template", "job", job)
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, newJob); err != nil {
				log.Error(err, "unable to create Job for JobSet", "job", job)
				return ctrl.Result{}, err
			}
			log.V(1).Info("created Job for JobSet run", "job", job)
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *JobSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&jobsetv1.JobSet{}).
		Complete(r)
}

func (r *JobSetReconciler) constructJobFromTemplate(jobSet *jobsetv1.JobSet, childJobs batchv1.JobList, jobIdx int) (*batchv1.Job, error) {
	jobTemplate := jobSet.Spec.Jobs[jobIdx]
	// We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
	name := fmt.Sprintf("%s-%d", jobTemplate.Name, time.Now().Unix())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
			Name:        name,
			Namespace:   jobSet.Namespace,
		},
		Spec: *jobTemplate.Template.Spec.DeepCopy(),
	}
	// set controller owner reference for garbage collection and reconcilation
	if err := ctrl.SetControllerReference(jobSet, job, r.Scheme); err != nil {
		return nil, err
	}
	return job, nil
}

func isJobFinished(job *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

func (r *JobSetReconciler) cleanUpOldJobs(ctx context.Context, failedJobs, successfulJobs []*batchv1.Job, log logr.Logger) {
	// Clean up failed jobs
	sort.Slice(failedJobs, func(i, j int) bool {
		if failedJobs[i].Status.StartTime == nil {
			return failedJobs[j].Status.StartTime != nil
		}
		return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
	})
	for _, job := range failedJobs {
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to delete old failed job", "job", job)
		} else {
			log.V(0).Info("deleted old failed job", "job", job)
		}
	}

	// Clean up succeeded jobs
	sort.Slice(successfulJobs, func(i, j int) bool {
		if successfulJobs[i].Status.StartTime == nil {
			return successfulJobs[j].Status.StartTime != nil
		}
		return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
	})
	for _, job := range successfulJobs {
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); (err) != nil {
			log.Error(err, "unable to delete old successful job", "job", job)
		} else {
			log.V(0).Info("deleted old successful job", "job", job)
		}
	}
}
