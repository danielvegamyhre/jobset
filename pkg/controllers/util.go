package controllers

import (
	"fmt"

	jobsetv1alpha "batch.x-k8s.io/jobset/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func isJobFinished(job *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

func jobIndex(jobSet *jobsetv1alpha.JobSet, job *batchv1.Job) (int, error) {
	for i, jobTemplate := range jobSet.Spec.Jobs {
		// Job naming convention: <jobset>-<job>
		fullJobName := fmt.Sprintf("%s-%s", jobSet.Name, jobTemplate.Template.Name)
		if job.Name == fullJobName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("JobSet %s does not contain Job %s", jobSet.Name, job.Name)
}

// TODO: is there a better way to check this?
func shouldSkipJob(jobName string, jobs *childJobs) bool {
	// skip if already active
	for _, job := range jobs.active {
		if job.Name == jobName {
			return true
		}
	}
	// skip if already succeeded
	for _, job := range jobs.successful {
		if job.Name == jobName {
			return true
		}
	}
	return false
}
