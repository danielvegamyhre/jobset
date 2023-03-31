package controllers

import (
	"context"
	"fmt"
	"time"

	jobsetv1alpha "batch.x-k8s.io/jobset/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"
	"k8s.io/utils/pointer"
)

// Define utility constants for object names and testing timeouts/durations and intervals.
const (
	jobSetNamespace = "default"
	timeout         = 30 * time.Second
	interval        = time.Millisecond * 250
)

var _ = Describe("JobSet controller", func() {
	ctx := context.Background()

	// Delete jobsets after each test case
	AfterEach(func() {
		if err := k8sClient.DeleteAllOf(ctx, &jobsetv1alpha.JobSet{}); err != nil {
			klog.Errorf("error deleting jobsets: %v", err)
		}
	})

	Context("When updating JobSet Status", func() {
		It("Should increase JobSet Status.Active count when new Jobs are created", func() {
			By("By creating a new JobSet")
			jobSetName := "simple"
			jobSet := makeJobSet(jobSetName)
			Expect(k8sClient.Create(ctx, jobSet)).Should(Succeed())

			// We'll need to retry getting this newly created JobSet, given that creation may not immediately happen.
			jobSetLookupKey := types.NamespacedName{Name: jobSetName, Namespace: jobSetNamespace}
			createdJobSet := &jobsetv1alpha.JobSet{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, jobSetLookupKey, createdJobSet)
				if err != nil {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())

			By("Check JobSet eventually has 3 active jobs")
			Eventually(func() (int, error) {
				err := k8sClient.Get(ctx, jobSetLookupKey, createdJobSet)
				if err != nil {
					return -1, err
				}
				return len(createdJobSet.Status.Active), nil
			}, 10*time.Second, interval).Should(Equal(3))
		})
	})

	Context("When sequential startup is enabled", func() {
		It("should start jobs only when the previous job is ready or completed", func() {
			By("By creating a new JobSet")
			jobSetName := "sequential-startup"
			jobSet := makeJobSet(jobSetName)
			// add initContainer to 1st and 2nd jobs to force some delay before pods are ready
			jobSet.Spec.Jobs[0].Template.Spec.Template.Spec.InitContainers = []corev1.Container{
				{
					Name:    "sleep",
					Image:   "busybox:latest",
					Command: []string{"sleep", "5"},
				},
			}
			jobSet.Spec.Jobs[1].Template.Spec.Template.Spec.InitContainers = []corev1.Container{
				{
					Name:    "sleep",
					Image:   "busybox:latest",
					Command: []string{"sleep", "5"},
				},
			}
			Expect(k8sClient.Create(ctx, jobSet)).Should(Succeed())

			var createdJobSet jobsetv1alpha.JobSet
			jobSetLookupKey := types.NamespacedName{Name: jobSetName, Namespace: jobSetNamespace}
			// We'll need to retry getting this newly created JobSet, given that creation may not immediately happen.
			Eventually(func() bool {
				err := k8sClient.Get(ctx, jobSetLookupKey, &createdJobSet)
				if err != nil {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())

			var (
				job1 batchv1.Job
				job2 batchv1.Job
			)

			job1Name := fmt.Sprintf("%s-%s", jobSet.Name, jobSet.Spec.Jobs[0].Template.Name)
			job2Name := fmt.Sprintf("%s-%s", jobSet.Name, jobSet.Spec.Jobs[1].Template.Name)
			job1LookupKey := types.NamespacedName{Name: job1Name, Namespace: jobSetNamespace}
			job2LookupKey := types.NamespacedName{Name: job2Name, Namespace: jobSetNamespace}

			klog.Infof("job1LookupKey: %s", job1LookupKey.Name)

			By("Initially first job will be active but second will not be created until first is ready")
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, job1LookupKey, &job1); err != nil {
					klog.Errorf("error getting job %s: %v", job1.Name, err)
					return false
				}
				_, condition := isJobFinished(&job1)

				klog.Infof("job %s status: %s", job1.Name, condition)

				// job 1 is active but not ready
				if condition == "" {
					// expect error getting job2 since it shouldn't exist yet
					if err := k8sClient.Get(ctx, job2LookupKey, &job2); err != nil {
						return true
					}
					klog.Infof("")
				}
				return false
			}, timeout, interval).Should(BeTrue())

			By("Once first job is ready or completed, 2nd job should be created")
			Eventually(func() (int, error) {
				err := k8sClient.Get(ctx, jobSetLookupKey, &createdJobSet)
				if err != nil {
					return -1, err
				}
				return len(createdJobSet.Status.Active), nil
			}, timeout, interval).Should(Equal(3))
		})
	})
})

// returns JobSet with 3 dummy jobs
func makeJobSet(name string) *jobsetv1alpha.JobSet {
	indexedCompletionMode := batchv1.IndexedCompletion
	return &jobsetv1alpha.JobSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "batch.x-k8s.io/v1alpha",
			Kind:       "JobSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: jobSetNamespace,
		},
		Spec: jobsetv1alpha.JobSetSpec{
			SequentialStartup: pointer.Bool(true),
			Jobs: []jobsetv1alpha.JobTemplate{
				{
					Name:    "job-template-0",
					Network: &jobsetv1alpha.Network{HeadlessService: pointer.Bool(true)},
					Template: &batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "job-0",
							Namespace: jobSetNamespace,
						},
						Spec: batchv1.JobSpec{
							CompletionMode: &indexedCompletionMode,
							Parallelism:    pointer.Int32(1),
							Completions:    pointer.Int32(1),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									RestartPolicy: "Never",
									Containers: []corev1.Container{
										{
											Name:  "test-container-0-name",
											Image: "busybox:latest",
										},
									},
								},
							},
						},
					},
				},
				{
					Name:    "job-template-1",
					Network: &jobsetv1alpha.Network{HeadlessService: pointer.Bool(true)},
					Template: &batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "job-1",
							Namespace: jobSetNamespace,
						},
						Spec: batchv1.JobSpec{
							CompletionMode: &indexedCompletionMode,
							Parallelism:    pointer.Int32(4),
							Completions:    pointer.Int32(4),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									RestartPolicy: "Never",
									Containers: []corev1.Container{
										{
											Name:  "test-container-1-name",
											Image: "busybox:latest",
										},
									},
								},
							},
						},
					},
				},
				{
					Name:    "job-template-2",
					Network: &jobsetv1alpha.Network{HeadlessService: pointer.Bool(true)},
					Template: &batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "job-2",
							Namespace: jobSetNamespace,
						},
						Spec: batchv1.JobSpec{
							CompletionMode: &indexedCompletionMode,
							Parallelism:    pointer.Int32(4),
							Completions:    pointer.Int32(4),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									RestartPolicy: "Never",
									Containers: []corev1.Container{
										{
											Name:  "test-container-2-name",
											Image: "busybox:latest",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
