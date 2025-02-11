package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	pb_k8s "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/k8s"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
)

type KubernetesClient struct {
	client *kubernetes.Clientset
}

func getK8SConfig(info *K8SCfg) (*rest.Config, error) {
	logger := common.GetLogger(context.Background())
	var cfg *rest.Config
	switch info.ClusterType {
	case pb_k8s.KubernetesType_INTERNAL.String():
		if len(info.HostConfigPath) <= 0 {
			info.HostConfigPath = "/root"
		}
		configPath := filepath.Join(info.HostConfigPath, ".kube", "config")
		localCfg, err := clientcmd.BuildConfigFromFlags("", configPath)
		if nil != err {
			logger.Errorf("init local config from [%s] failed: %+v", configPath, err)
			return nil, err
		}
		cfg = localCfg
	case pb_k8s.KubernetesType_EXTERNAL.String():
		cfg = &rest.Config{
			BearerToken: info.ClusterToken,
			Host:        info.ClusterAddr,
		}
		cfg.TLSClientConfig.Insecure = true
	default:
		logger.Errorf("not support input cluster type: [%s]", info.ClusterType)
		return nil, fmt.Errorf(fmt.Sprintf("Illegal K8S ClusterType [%s]", info.ClusterType))
	}
	cfg.QPS = 1e3
	cfg.Burst = 1e3
	if cfg.GroupVersion == nil {
		cfg.GroupVersion = &schema.GroupVersion{
			Version: "v1",
		}
	}
	if cfg.NegotiatedSerializer == nil {
		cfg.NegotiatedSerializer = scheme.Codecs
	}
	if cfg.APIPath == "" {
		cfg.APIPath = "/api"
	}
	return cfg, nil
}
func (k8s *KubernetesClient) InitClient(k8sCfg *K8SCfg) error {
	logger := common.GetLogger(context.Background())
	kubeConfig, err := getK8SConfig(k8sCfg)
	if err != nil {
		logger.Errorf("get rest config error: %+v", err)
		return err
	}
	client, err := kubernetes.NewForConfig(kubeConfig)
	if nil != err {
		logger.Errorf("init k8s by config [%+v] failed: [%+v]", kubeConfig, err)
		return err
	}
	k8s.client = client
	return nil
}

// CreateNamespaceIfNotExist create namespace if not exist
func (c *KubernetesClient) CreateNamespaceIfNotExist(ctx context.Context, nameSpace string) error {
	logger := common.GetLogger(context.Background())
	_, err := c.client.CoreV1().Namespaces().Get(ctx, nameSpace, metav1.GetOptions{})
	if !k8serror.IsNotFound(err) {
		logger.Infof("namespace[%s] has created before", nameSpace)
		return nil
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: nameSpace,
			Labels: map[string]string{
				"Owner":     "tiocr",
				"NameSpace": nameSpace,
			},
		},
	}
	_, err = c.client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if err != nil && !k8serror.IsAlreadyExists(err) {
		logger.Errorf("CreateNamespace[%s] error: %+v", nameSpace, err)
		return err
	}
	logger.Info("namespace [%s] create success", nameSpace)
	return nil
}

// ApplyJob if k8s job is exit pass
func (c *KubernetesClient) ApplyJob(ctx context.Context, object *batchv1.Job) (*batchv1.Job, bool, error) {
	exitObj, err := c.GetJob(ctx, object.GetObjectMeta().GetNamespace(), object.Name, metav1.GetOptions{})
	if err == nil && exitObj != nil && exitObj.Status.Active > 0 {
		return exitObj, true, nil
	}
	newObj, err := c.CreateJob(ctx, object)
	if nil != err {
		common.GetLogger(ctx).Errorf("create job failed: %+v", err)
		return newObj, false, err
	}
	return newObj, false, err
}

// CreateJob create k8s job
func (c *KubernetesClient) CreateJob(ctx context.Context, object *batchv1.Job) (*batchv1.Job, error) {
	return c.client.BatchV1().Jobs(object.GetObjectMeta().GetNamespace()).Create(ctx, object, metav1.CreateOptions{})
}

// GetJob get k8s job
func (c *KubernetesClient) GetJob(ctx context.Context, namespace, jobName string, options metav1.GetOptions) (*batchv1.Job, error) {
	return c.client.BatchV1().Jobs(namespace).Get(ctx, jobName, options)
}

// UpdateJob update k8s job
func (c *KubernetesClient) UpdateJob(ctx context.Context, namespace string, pbBatchJob *batchv1.Job) (*batchv1.Job, error) {
	return c.client.BatchV1().Jobs(namespace).Update(ctx, pbBatchJob, metav1.UpdateOptions{})
}

// DeleteJob delete job
func (c *KubernetesClient) DeleteJob(ctx context.Context, namespace, name string, options metav1.DeleteOptions) error {
	return c.client.BatchV1().Jobs(namespace).Delete(ctx, name, options)
}

// ListJobs list jobs
func (c *KubernetesClient) ListJobs(ctx context.Context, namespace string, options metav1.ListOptions) (*batchv1.JobList, error) {
	return c.client.BatchV1().Jobs(namespace).List(ctx, options)
}

// ApplyJob if k8s job is exit pass
func (c *KubernetesClient) ApplyPod(ctx context.Context, object *corev1.Pod) (*corev1.Pod, bool, error) {
	existPod, err := c.GetPod(ctx, object.GetObjectMeta().GetNamespace(), object.Name, metav1.GetOptions{})
	if err == nil && existPod != nil && existPod.Name == object.Name {
		common.GetLogger(ctx).Warnf("the pod [%s] has created before", object.Name)
		return existPod, true, nil
	}
	newObj, err := c.CreatePod(ctx, object)
	if nil != err {
		common.GetLogger(ctx).Errorf("create pod failed: %+v", err)
		return newObj, false, err
	}
	return newObj, false, nil
}

// CreatePod create k8s job
func (c *KubernetesClient) CreatePod(ctx context.Context, object *corev1.Pod) (*corev1.Pod, error) {
	return c.client.CoreV1().Pods(object.GetObjectMeta().GetNamespace()).Create(ctx, object, metav1.CreateOptions{})
}

// GetPod get k8s job
func (c *KubernetesClient) GetPod(ctx context.Context, namespace, jobName string, options metav1.GetOptions) (*corev1.Pod, error) {
	return c.client.CoreV1().Pods(namespace).Get(ctx, jobName, options)
}

// UpdateJob update k8s job
func (c *KubernetesClient) UpdatePod(ctx context.Context, namespace string, pod *corev1.Pod) (*corev1.Pod, error) {
	return c.client.CoreV1().Pods(namespace).Update(ctx, pod, metav1.UpdateOptions{})
}

// DeleteJob delete job
func (c *KubernetesClient) DeletePod(ctx context.Context, namespace, name string, options metav1.DeleteOptions) error {
	common.GetLogger(ctx).Debugf("start delete pod: [%s/%s]", namespace, name)
	return c.client.CoreV1().Pods(namespace).Delete(ctx, name, options)
}

// ListPods list pods
func (c *KubernetesClient) ListPods(ctx context.Context, namespace string, opts *metav1.ListOptions) (*corev1.PodList, error) {
	return c.client.CoreV1().Pods(namespace).List(ctx, *opts)
}

func (c *KubernetesClient) GetPodFinishedTime(ctx context.Context, pod *corev1.Pod, podName string) string {
	for _, container := range pod.Spec.Containers {
		if container.Name != podName {
			continue
		}
		podByte, _ := json.Marshal(pod)
		common.GetLogger(ctx).Debugf("try get pod [%s] finished time", string(podByte))
	}
	return ""
}

func (c *KubernetesClient) ListNodes(ctx context.Context, opts metav1.ListOptions) (*corev1.NodeList, error) {
	return c.client.CoreV1().Nodes().List(ctx, opts)
}
