package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	pb_k8s "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/k8s"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	agent_model "git.woa.com/yt-industry-ai/poseidon/internal/agent/model"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	pkg_types "git.woa.com/yt-industry-ai/poseidon/types"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// K8SHandler ExternalHandler的K8S实现
type K8SHandler struct {
	opt    *Option           // k8s 适配器参数
	client *KubernetesClient // k8s client
}

func NewK8SHandler(ctx context.Context, opt *Option) (*K8SHandler, error) {
	logger := common.GetLogger(ctx)
	if len(opt.DefaultK8SCfg.DefaultNameSpace) <= 0 {
		opt.DefaultK8SCfg.DefaultNameSpace = "tiocr"
	}
	client := &KubernetesClient{}
	if err := client.InitClient(opt.DefaultK8SCfg); nil != err {
		logger.Errorf("init k8s client failed: %+v", err)
		return nil, common.Errorf(ctx, code.Code_InternalError__K8SInitFailed)
	}
	hander := &K8SHandler{
		opt:    opt,
		client: client,
	}
	return hander, nil
}

// StartJob 启动Job，对应 k8s 接口 [创建训练任务接口 + 启动训练任务接口]
func (x *K8SHandler) StartJob(
	ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob,
) (externalJobId string, retry bool, err error) {
	logger := common.GetLogger(ctx)

	logger.Debugf("start create k8s task")

	// 根据任务类型获取参数转换器，将 job 转换为 k8s 任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return "", true, common.Errorf(ctx, code.Code_InternalError__K8SInitFailed)
	}
	jobInfo, err := parser.GetK8STaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to k8s task info fail")
		return "", true, common.Errorf(ctx, code.Code_InternalError__K8SInitFailed)
	}

	// 检查参数
	if len(jobInfo.StartCmd) == 0 {
		jobInfo.StartCmd = strings.Join(containerJob.JobCtx.Config.Cmd, " ")
	}
	if len(jobInfo.ImageName) == 0 {
		jobInfo.ImageName = containerJob.JobCtx.Config.ImageName
	}
	if len(jobInfo.ImageNamePrefixReplace) > 0 {
		replaces := strings.Split(jobInfo.ImageNamePrefixReplace, "/")
		if len(replaces) == 2 {
			jobInfo.ImageName = strings.ReplaceAll(jobInfo.ImageName, replaces[0], replaces[1])
		} else {
			logger.Errorf("illegal image name replace by prefix [%s]", jobInfo.ImageNamePrefixReplace)
		}
	}
	if jobInfo.EnvConfig == nil {
		jobInfo.EnvConfig = []*pb_k8s.TaskEnv{}
	}
	// 添加 Container.Prepare 注入的环境变量
	for k, v := range containerJob.JobCtx.Config.Env {
		jobInfo.EnvConfig = append(jobInfo.EnvConfig, &pb_k8s.TaskEnv{
			Name:  k,
			Value: v,
		})
	}

	// 1. 创建namespace
	if err := x.client.CreateNamespaceIfNotExist(ctx, x.opt.DefaultK8SCfg.DefaultNameSpace); nil != err {
		logger.Errorf("try create namespace failed: %+v", err)
		return "", true, common.Errorf(ctx, code.Code_InternalError__K8SClientFailed)
	}
	// 2. 检查job 是否存在
	podRun, err := x.client.GetPod(ctx, x.opt.DefaultK8SCfg.DefaultNameSpace, jobInfo.PodName, metav1.GetOptions{})
	if nil != err {
		if k8serror.IsNotFound(err) {
			logger.Debugf("job[%s/%s] not found, err: %+v", x.opt.DefaultK8SCfg.DefaultNameSpace, jobInfo.PodName, err)
		} else {
			logger.Errorf("get job [%s/%s] failed: [%+v]", x.opt.DefaultK8SCfg.DefaultNameSpace, jobInfo.PodName, err)
			return "", true, common.Errorf(ctx, code.Code_InternalError__K8SClientFailed)
		}
	}
	if nil != podRun && podRun.Name == jobInfo.PodName {
		logger.Infof("job [%s] has created before: %+v", jobInfo.PodName, podRun)
		return podRun.Name, false, nil
	}
	// 3. 创建pod
	pod := x.parseTaskInfoToPod(ctx, job, jobInfo, containerJob.JobCtx.Config, x.opt.DefaultK8SCfg)
	podNew, exists, err := x.client.ApplyPod(ctx, pod)
	if nil != err {
		logger.Errorf("create job pod failed: [%+v]", err)
		return "", true, common.Errorf(ctx, code.Code_InternalError__K8SClientFailed)
	}
	logger.Debugf("pod new [%+v], exists [%+v]", podNew, exists)

	logger.Debugf("end create k8s task: [%s]", jobInfo.PodName)
	return jobInfo.PodName, false, nil
}

// DescribeJob 查看Job，对应 k8s 接口 [获取训练任务详情信息接口]
func (x *K8SHandler) DescribeJob(
	ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob,
) (*model.Job, bool, error) {
	logger := common.GetLogger(ctx)

	// 根据任务类型获取参数转换器，将 job 转换为 k8s 任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return nil, false, common.Errorf(ctx, code.Code_InvalidParameter__K8SParserNotExist)
	}
	jobInfo, err := parser.GetK8STaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to k8s task info fail")
		return nil, false, common.Errorf(ctx, code.Code_InternalError__K8SClientFailed)
	}
	podRun, err := x.client.GetPod(ctx, x.opt.DefaultK8SCfg.DefaultNameSpace, jobInfo.PodName, metav1.GetOptions{})
	if nil != err {
		if k8s_errors.IsNotFound(err) || nil == podRun || podRun.Name != jobInfo.PodName {
			logger.Debugf("pod [%s] not exists", jobInfo.PodName)
			return nil, false, common.Errorf(ctx, code.Code_ResourceNotFound__JobNotExist)
		}
		logger.Errorf("get job [%s/%s] failed: [%+v]", x.opt.DefaultK8SCfg.DefaultNameSpace, jobInfo.PodName, err)
		return nil, false, common.Errorf(ctx, code.Code_InternalError__K8SClientFailed, err)
	}
	jobInfo.CreateTime = podRun.CreationTimestamp.Format("2006-01-02 15:04:05")
	respJob := &model.Job{}
	respJob.GetJobLive().StartTime = podRun.CreationTimestamp.Format("2006-01-02 15:04:05")
	// respJob.GetJobLive().EndTime = time.Now().Format("2006-01-02 15:04:05")
	switch podRun.Status.Phase {
	case corev1.PodPending:
		ok, msg := x.checkContainerOK(ctx, podRun)
		if ok {
			respJob.JobStatus = types.JobStatus_JOB_STATUS_PREPARING
		} else {
			respJob.JobStatus = types.JobStatus_JOB_STATUS_FAILED
			respJob.GetJobLive().Error = common.ErrorfProto(ctx, code.Code_InternalError__K8STaskFailed, msg)
			respJob.GetJobLive().EndTime = x.client.GetPodFinishedTime(ctx, podRun, ContainerDefaultName)
		}
		break
	case corev1.PodRunning:
		respJob.JobStatus = types.JobStatus_JOB_STATUS_RUNNING
		break
	case corev1.PodSucceeded:
		time.Sleep(2 * time.Minute)
		respJob.JobStatus = types.JobStatus_JOB_STATUS_SUCCESS
		respJob.GetJobLive().EndTime = x.client.GetPodFinishedTime(ctx, podRun, ContainerDefaultName)
		break
	case corev1.PodFailed:
		time.Sleep(1 * time.Minute)
		respJob.JobStatus = types.JobStatus_JOB_STATUS_FAILED
		errFileName := path.Join(containerJob.JobDir(), "error.json")
		fileInfo, err := os.Stat(errFileName)
		errTrainMsg := ""
		if nil != err {
			if os.IsNotExist(err) {
				logger.Errorf("errfile [%s] not exists", errFileName)
			}
		} else {
			if fileInfo.Mode().IsRegular() {
				fileContent, err := os.ReadFile(errFileName)
				if err != nil {
					logger.Errorf("read errfile [%s] failed: %+v", errFileName, err)
				} else {
					errTrainMsg = string(fileContent)
				}
			}
		}
		msg := fmt.Sprintf("%s; %s; %s", podRun.Status.Message, podRun.Status.Reason, errTrainMsg)
		respJob.GetJobLive().Error = common.ErrorfProto(ctx, code.Code_InternalError__K8STaskFailed, msg)
		respJob.GetJobLive().EndTime = x.client.GetPodFinishedTime(ctx, podRun, ContainerDefaultName)
		break
	}
	podByte, _ := json.Marshal(podRun)
	jobByte, _ := json.Marshal(respJob)
	logger.Debugf("describe pod podName [%s] status [%s] pod [%s] respJob [%s]", jobInfo.PodName, respJob.JobStatus, string(podByte), string(jobByte))
	return respJob, false, nil
}
func (x *K8SHandler) checkContainerOK(ctx context.Context, podRun *corev1.Pod) (bool, string) {
	logger := common.GetLogger(ctx)
	for _, container := range podRun.Status.ContainerStatuses {
		if container.Name == ContainerDefaultName {
			if nil != container.State.Waiting && container.State.Waiting.Reason == "ImagePullBackOff" {
				msg := fmt.Sprintf("%s: %s", container.State.Waiting.Reason, container.State.Waiting.Message)
				logger.Errorf("container create failed: %s", msg)
				return false, msg
			}
		}
	}
	return true, ""
}

// StopJob 停止Job，对应 k8s 接口 [停止训练任务接口]
func (x *K8SHandler) StopJob(
	ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob,
) (retry bool, err error) {
	logger := common.GetLogger(ctx)

	// 根据任务类型获取参数转换器，将 job 转换为 k8s 任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return false, common.Errorf(ctx, code.Code_InvalidParameter__K8SParserNotExist)
	}
	info, err := parser.GetK8STaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to k8s task info fail")
		return false, err
	}
	logger.Debugf("start delete pod: [%s]", info.PodName)
	// check pod if exists
	podRun, err := x.client.GetPod(ctx, x.opt.DefaultK8SCfg.DefaultNameSpace, info.PodName, metav1.GetOptions{})
	if nil != err {
		if k8s_errors.IsNotFound(err) || nil == podRun || podRun.Name != info.PodName {
			logger.Debugf("pod [%s] not exists", info.PodName)
			return false, nil
		}
		logger.Errorf("get job [%s/%s] failed: [%+v]", x.opt.DefaultK8SCfg.DefaultNameSpace, info.PodName, err)
		return false, common.Errorf(ctx, code.Code_InternalError__K8SClientFailed, err)
	}
	if len(x.opt.DefaultK8SCfg.FinalizerKey) > 0 {
		// start update pod for remove finalizer key
		podRun.ObjectMeta.Finalizers = []string{}
		podUpdate, err := x.client.UpdatePod(ctx, x.opt.DefaultK8SCfg.DefaultNameSpace, podRun)
		if err != nil {
			if k8s_errors.IsNotFound(err) || nil == podUpdate || podUpdate.Name != info.PodName {
				logger.Debugf("pod [%s] not exists", info.PodName)
				return false, nil
			}
			logger.WithError(err).Errorf("delete pod finalizer fail")
			return false, err
		}
		time.Sleep(10 * time.Second)
		podUpdateByte, _ := json.Marshal(podUpdate)
		logger.Debugf("update pod finalizer [%+v] success: [%s]", podUpdate.ObjectMeta.Finalizers, string(podUpdateByte))
	}

	if err := x.client.DeletePod(ctx, x.opt.DefaultK8SCfg.DefaultNameSpace, info.PodName, metav1.DeleteOptions{}); nil != err {
		if k8s_errors.IsNotFound(err) {
			logger.Debugf("delete pod[%s] not found: %+v", info.PodName, err)
			return false, nil
		}
		logger.Errorf("delete pod [%s] failed: %+v", info.PodName, err)
		return false, common.Errorf(ctx, code.Code_InternalError__K8STaskFailed)
	}
	logger.Debugf("end delete pod: [%s]", info.PodName)
	return false, nil
}

func (x *K8SHandler) parseTaskInfoToPod(ctx context.Context, job *model.Job, info *pb_k8s.KubernetesTaskInfo, cfg *pkg_types.ContainerConfig, k8sCfg *K8SCfg) *corev1.Pod {
	hostMounts := parseMountVolumes(ctx, k8sCfg, cfg.Mounts, cfg.ReadonlyMounts, info)

	gpuResourceKey := x.opt.DefaultK8SCfg.GpuResourceKey
	if len(gpuResourceKey) <= 0 {
		gpuResourceKey = CudaCoreResourceKey
	}
	nodeGpuInfo, ok := k8sCfg.NodeGpus[job.Resources[0].HostName]
	if !ok {
		nodeGpuInfo = GpuInfo{}
	}
	defaultTerminationGracePeriodSeconds := int64(300)
	privileged := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        info.PodName,
			Namespace:   x.opt.DefaultK8SCfg.DefaultNameSpace,
			Labels:      makeJobMetaLabels(info, gpuResourceKey),
			Annotations: makeJobMetaAnnotation(info, gpuResourceKey),
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			NodeName:                      "",
			NodeSelector:                  makeJobNodeSelector(k8sCfg, job.Resources[0].HostName),
			Volumes:                       makePodVolumns(ctx, hostMounts),
			TerminationGracePeriodSeconds: &defaultTerminationGracePeriodSeconds,
			Containers: []corev1.Container{
				{
					Name:            ContainerDefaultName,
					Image:           info.ImageName,
					ImagePullPolicy: corev1.PullIfNotPresent,
					VolumeMounts:    makeContainerVolumns(hostMounts),
					Env:             makeContainerEnv(info.EnvConfig),
					Resources: corev1.ResourceRequirements{
						Limits:   containerResourceList(info, gpuResourceKey, nodeGpuInfo),
						Requests: containerResourceList(info, gpuResourceKey, nodeGpuInfo),
					},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			// ImagePullSecrets: []corev1.LocalObjectReference{{Name: k8sCfg.ImagePullSecKey}},
		},
	}
	if len(info.StartCmd) > 0 {
		pod.Spec.Containers[0].Command = strings.Split(info.StartCmd, "&&")
	}
	if len(k8sCfg.ImagePullSecKey) > 0 {
		pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: k8sCfg.ImagePullSecKey}}
	}
	if info.Port > 0 {
		pod.Spec.Containers[0].Ports = []corev1.ContainerPort{
			{
				Name:          "serving-port",
				ContainerPort: info.Port,
			},
		}
	}
	if len(k8sCfg.FinalizerKey) > 0 {
		pod.ObjectMeta.Finalizers = []string{k8sCfg.FinalizerKey}
	}
	podYaml, _ := yaml.Marshal(pod)
	podByte, _ := json.Marshal(pod)
	common.GetLogger(ctx).Debugf("pod yaml: \n\n[%s]\n\n", string(podYaml))
	common.GetLogger(ctx).Debugf("pod json: \n\n[%s]\n\n", string(podByte))
	return pod
}

// labels
func makeJobMetaLabels(task *pb_k8s.KubernetesTaskInfo, resourceGpuKey string) map[string]string {
	labels := map[string]string{
		LabelOwner:     task.Uin,
		LabelTaskName:  task.PodName,
		LabelProjectId: strings.ReplaceAll(task.ProjectId, "@", ""),
		LabelTask:      LabelDefaultOwner,
	}
	if task.GpuNum > 0 && len(task.GpuType) > 0 {
		labels[resourceGpuKey] = task.GpuType
	}
	return labels
}

// annotation
func makeJobMetaAnnotation(task *pb_k8s.KubernetesTaskInfo, resourceGpuKey string) map[string]string {
	annotation := map[string]string{
		AnnotationTaskID:     task.TaskId,
		AnnotationJobID:      task.PodId,
		AnnotationJobName:    task.PodName,
		AnnotationUpdateTime: time.Now().Format(time.RFC3339),
		AnnotationCreator:    task.Uin,
		AnnotationProjectId:  strings.ReplaceAll(task.ProjectId, "@", ""),
	}
	if task.GpuNum > 0 && len(task.GpuType) > 0 {
		annotation[resourceGpuKey] = task.GpuType
	}
	return annotation
}

func makeJobNodeSelector(k8sCfg *K8SCfg, nodeName string) map[string]string {
	nodeSelector := map[string]string{}
	for k, v := range k8sCfg.NodeSelector {
		nodeSelector[k] = v
	}
	if len(k8sCfg.ResourceNodeKey) > 0 {
		nodeSelector[k8sCfg.ResourceNodeKey] = nodeName
	}
	return nodeSelector
}

type MountVolume struct {
	mountType     pb_k8s.MountType
	name          string
	hostPath      string
	containerPath string
	readonly      bool
	shmSize       int32

	cephParam pb_k8s.MountCephPathParam
	nfsParam  pb_k8s.MountNfsPathParam
}

func parseMountVolumes(ctx context.Context, k8sCfg *K8SCfg, mounts map[string]string, readOnlyMounts map[string]string, info *pb_k8s.KubernetesTaskInfo) []MountVolume {
	volumes := make([]MountVolume, 0)
	if k8sCfg.MountMod.MountType == pb_k8s.MountType_CephPath {
		volumes = append(volumes, MountVolume{
			name:          "volume-ceph",
			mountType:     pb_k8s.MountType_CephPath,
			hostPath:      k8sCfg.MountMod.CephParams.CephDataPath,
			containerPath: k8sCfg.MountMod.CephParams.CephMountPath,
			readonly:      false,
			cephParam: pb_k8s.MountCephPathParam{
				CephDataPath:   k8sCfg.MountMod.CephParams.CephDataPath,
				CephMountPath:  k8sCfg.MountMod.CephParams.CephMountPath,
				CephSecretName: k8sCfg.MountMod.CephParams.CephSecretName,
				CephUser:       k8sCfg.MountMod.CephParams.CephUser,
				Monitors:       k8sCfg.MountMod.CephParams.Monitors,
			},
		})
		mountAll := ""
		hasMounted := make(map[string]bool, 0)
		for _, env := range info.EnvConfig {
			if env.Name == "JOB_LICENSE" {
				paths := strings.Split(env.Value, ":")
				if len(paths) != 2 {
					common.GetLogger(ctx).Errorf("illegal license path: [%s]", env.Value)
					continue
				}
				hostPath := strings.Replace(paths[0], k8sCfg.MountMod.CephParams.CephMountPath, k8sCfg.MountMod.CephParams.CephDataPath, 1)
				volumes = append(volumes, MountVolume{
					name:          "volume-license",
					mountType:     pb_k8s.MountType_CephPath,
					hostPath:      hostPath,
					containerPath: paths[1],
					readonly:      false,
					cephParam: pb_k8s.MountCephPathParam{
						CephDataPath:   k8sCfg.MountMod.CephParams.CephDataPath,
						CephMountPath:  k8sCfg.MountMod.CephParams.CephMountPath,
						CephSecretName: k8sCfg.MountMod.CephParams.CephSecretName,
						CephUser:       k8sCfg.MountMod.CephParams.CephUser,
						Monitors:       k8sCfg.MountMod.CephParams.Monitors,
					},
				})
				hasMounted[paths[1]] = true
			} else if env.Name == "JOB_DIR" {
				hostPath := strings.Replace(env.Value, k8sCfg.MountMod.CephParams.CephMountPath, k8sCfg.MountMod.CephParams.CephDataPath, 1)
				hostPath = path.Join(hostPath, "algo_pkg")
				volumes = append(volumes, MountVolume{
					name:          "volume-code",
					mountType:     pb_k8s.MountType_CephPath,
					hostPath:      hostPath,
					containerPath: "/app",
					readonly:      false,
					cephParam: pb_k8s.MountCephPathParam{
						CephDataPath:   k8sCfg.MountMod.CephParams.CephDataPath,
						CephMountPath:  k8sCfg.MountMod.CephParams.CephMountPath,
						CephSecretName: k8sCfg.MountMod.CephParams.CephSecretName,
						CephUser:       k8sCfg.MountMod.CephParams.CephUser,
						Monitors:       k8sCfg.MountMod.CephParams.Monitors,
					},
				})
				hasMounted["/app"] = true
			} else if env.Name == "MOUNTS" {
				mountAll = env.Value
			}
		}
		if len(mountAll) > 0 {
			mountAllPairs := strings.Split(mountAll, ",")
			for id, mountPairStrTmp := range mountAllPairs {
				mountPairStr := strings.Trim(mountPairStrTmp, " ")
				if len(mountPairStr) == 0 {
					continue
				}
				mountPair := strings.Split(mountPairStr, ":")
				if len(mountPair) != 2 || len(mountPair[0]) == 0 || len(mountPair[1]) == 0 {
					common.GetLogger(ctx).Errorf("illegal mount[%s] [%s]", mountAll, mountPairStrTmp)
					continue
				}
				if _, ok := hasMounted[mountPair[1]]; ok {
					continue
				}
				hostPath := strings.Replace(mountPair[0], k8sCfg.MountMod.CephParams.CephMountPath, k8sCfg.MountMod.CephParams.CephDataPath, 1)
				volumes = append(volumes, MountVolume{
					name:          fmt.Sprintf("volume-%d", id),
					mountType:     pb_k8s.MountType_CephPath,
					hostPath:      hostPath,
					containerPath: mountPair[1],
					readonly:      false,
					cephParam: pb_k8s.MountCephPathParam{
						CephDataPath:   k8sCfg.MountMod.CephParams.CephDataPath,
						CephMountPath:  k8sCfg.MountMod.CephParams.CephMountPath,
						CephSecretName: k8sCfg.MountMod.CephParams.CephSecretName,
						CephUser:       k8sCfg.MountMod.CephParams.CephUser,
						Monitors:       k8sCfg.MountMod.CephParams.Monitors,
					},
				})
				hasMounted[mountPair[1]] = true
			}
		}
	} else if k8sCfg.MountMod.MountType == pb_k8s.MountType_Nfs {
		volumes = append(volumes, MountVolume{
			name:          "volume-nfs",
			mountType:     pb_k8s.MountType_Nfs,
			hostPath:      k8sCfg.MountMod.NFSParams.NfsDataPath,
			containerPath: k8sCfg.MountMod.NFSParams.NfsMountPath,
			readonly:      false,
			nfsParam: pb_k8s.MountNfsPathParam{
				NfsServer:    k8sCfg.MountMod.NFSParams.NfsServer,
				NfsDataPath:  k8sCfg.MountMod.NFSParams.NfsDataPath,
				NfsMountPath: k8sCfg.MountMod.NFSParams.NfsMountPath,
			},
		})
		mountAll := ""
		hasMounted := make(map[string]bool, 0)
		for _, env := range info.EnvConfig {
			if env.Name == "JOB_LICENSE" {
				paths := strings.Split(env.Value, ":")
				if len(paths) != 2 {
					common.GetLogger(ctx).Errorf("illegal license path: [%s]", env.Value)
					continue
				}
				hostPath := strings.Replace(paths[0], k8sCfg.MountMod.NFSParams.NfsMountPath, k8sCfg.MountMod.NFSParams.NfsDataPath, 1)
				volumes = append(volumes, MountVolume{
					name:          "volume-license",
					mountType:     pb_k8s.MountType_Nfs,
					hostPath:      hostPath,
					containerPath: paths[1],
					readonly:      false,
					nfsParam: pb_k8s.MountNfsPathParam{
						NfsServer:    k8sCfg.MountMod.NFSParams.NfsServer,
						NfsDataPath:  k8sCfg.MountMod.NFSParams.NfsDataPath,
						NfsMountPath: k8sCfg.MountMod.NFSParams.NfsMountPath,
					},
				})
				hasMounted[paths[1]] = true
			} else if env.Name == "JOB_DIR" {
				hostPath := strings.Replace(env.Value, k8sCfg.MountMod.NFSParams.NfsMountPath, k8sCfg.MountMod.NFSParams.NfsDataPath, 1)
				hostPath = path.Join(hostPath, "algo_pkg")
				volumes = append(volumes, MountVolume{
					name:          "volume-code",
					mountType:     pb_k8s.MountType_Nfs,
					hostPath:      hostPath,
					containerPath: "/app",
					readonly:      false,
					nfsParam: pb_k8s.MountNfsPathParam{
						NfsServer:    k8sCfg.MountMod.NFSParams.NfsServer,
						NfsDataPath:  k8sCfg.MountMod.NFSParams.NfsDataPath,
						NfsMountPath: k8sCfg.MountMod.NFSParams.NfsMountPath,
					},
				})
				hasMounted["/app"] = true
			} else if env.Name == "MOUNTS" {
				mountAll = env.Value
			}
		}
		if len(mountAll) > 0 {
			mountAllPairs := strings.Split(mountAll, ",")
			for id, mountPairStrTmp := range mountAllPairs {
				mountPairStr := strings.Trim(mountPairStrTmp, " ")
				if len(mountPairStr) == 0 {
					continue
				}
				mountPair := strings.Split(mountPairStr, ":")
				if len(mountPair) != 2 || len(mountPair[0]) == 0 || len(mountPair[1]) == 0 {
					common.GetLogger(ctx).Errorf("illegal mount[%s] [%s]", mountAll, mountPairStrTmp)
					continue
				}
				if _, ok := hasMounted[mountPair[1]]; ok {
					continue
				}
				hostPath := strings.Replace(mountPair[0], k8sCfg.MountMod.NFSParams.NfsMountPath, k8sCfg.MountMod.NFSParams.NfsDataPath, 1)
				volumes = append(volumes, MountVolume{
					name:          fmt.Sprintf("volume-%d", id),
					mountType:     pb_k8s.MountType_Nfs,
					hostPath:      hostPath,
					containerPath: mountPair[1],
					readonly:      false,
					nfsParam: pb_k8s.MountNfsPathParam{
						NfsServer:    k8sCfg.MountMod.NFSParams.NfsServer,
						NfsDataPath:  k8sCfg.MountMod.NFSParams.NfsDataPath,
						NfsMountPath: k8sCfg.MountMod.NFSParams.NfsMountPath,
					},
				})
				hasMounted[mountPair[1]] = true
			}
		}
	} else {
		id := 0
		for hostPath, containerPath := range mounts {
			id += 1
			volumes = append(volumes, MountVolume{
				name:          fmt.Sprintf("volume-%d", id),
				mountType:     pb_k8s.MountType_HostPath,
				hostPath:      hostPath,
				containerPath: containerPath,
				readonly:      false,
			})
		}
		for hostPath, containerPath := range readOnlyMounts {
			id += 1
			volumes = append(volumes, MountVolume{
				name:          fmt.Sprintf("volume-read-%d", id),
				mountType:     pb_k8s.MountType_HostPath,
				hostPath:      hostPath,
				containerPath: containerPath,
				readonly:      true,
			})
		}
	}
	if info.ShmSize > 0 {
		volumes = append(volumes, MountVolume{
			name:          fmt.Sprintf("shm-mem"),
			mountType:     pb_k8s.MountType_Memory,
			shmSize:       info.ShmSize,
			containerPath: "/dev/shm",
			readonly:      false,
		})
	}

	return volumes
}
func makePodVolumns(ctx context.Context, hostMounts []MountVolume) []corev1.Volume {
	volumes := make([]corev1.Volume, 0)
	for _, hv := range hostMounts {
		if hv.mountType == pb_k8s.MountType_HostPath {
			volumes = append(volumes, corev1.Volume{
				Name: hv.name,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: hv.hostPath,
					},
				},
			})
		} else if hv.mountType == pb_k8s.MountType_CephPath {
			volumes = append(volumes, corev1.Volume{
				Name: hv.name,
				VolumeSource: corev1.VolumeSource{
					CephFS: &corev1.CephFSVolumeSource{
						Monitors: hv.cephParam.Monitors,
						Path:     hv.hostPath,
						User:     hv.cephParam.CephUser,
						SecretRef: &corev1.LocalObjectReference{
							Name: hv.cephParam.CephSecretName,
						},
						ReadOnly: false,
					},
				},
			})
		} else if hv.mountType == pb_k8s.MountType_Nfs {
			volumes = append(volumes, corev1.Volume{
				Name: hv.name,
				VolumeSource: corev1.VolumeSource{
					NFS: &corev1.NFSVolumeSource{
						Server:   hv.nfsParam.NfsServer,
						Path:     hv.hostPath,
						ReadOnly: false,
					},
				},
			})
		} else if hv.mountType == pb_k8s.MountType_Memory {
			shmSize, err := resource.ParseQuantity(fmt.Sprintf("%dMi", hv.shmSize))
			if nil != err {
				common.GetLogger(ctx).Errorf("parse shm size failed: %+v", err)
			}
			volumes = append(volumes, corev1.Volume{
				Name: hv.name,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium:    corev1.StorageMediumMemory,
						SizeLimit: &shmSize,
					},
				},
			})
		}
	}
	return volumes
}

func makeContainerVolumns(hostMounts []MountVolume) []corev1.VolumeMount {
	volumes := make([]corev1.VolumeMount, 0)
	for _, hv := range hostMounts {
		volumes = append(volumes, corev1.VolumeMount{
			Name:      hv.name,
			MountPath: hv.containerPath,
			ReadOnly:  hv.readonly,
		})
	}
	return volumes
}
func makeContainerEnv(envs []*pb_k8s.TaskEnv) []corev1.EnvVar {
	containerEnv := make([]corev1.EnvVar, 0)
	for _, env := range envs {
		if env.Name == "NVIDIA_VISIBLE_DEVICES" {
			continue
		}
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  env.Name,
			Value: env.Value,
		})
	}
	return containerEnv
}

func containerResourceList(task *pb_k8s.KubernetesTaskInfo, gpuResourceKey string, gpuInfo GpuInfo) corev1.ResourceList {
	resourceList := corev1.ResourceList{}
	if task.CpuNum > 0 {
		resourceList[corev1.ResourceCPU] = resource.MustParse(fmt.Sprintf("%d", task.CpuNum))
	}
	if task.Memory > 0 {
		resourceList[corev1.ResourceMemory] = resource.MustParse(fmt.Sprintf("%dMi", task.Memory))
	}
	if task.GpuNum > 0 {
		if gpuInfo.GpuCardMem > 0 {
			resourceList[corev1.ResourceName(gpuResourceKey)] = resource.MustParse(fmt.Sprintf("%d", task.GpuNum*gpuInfo.GpuCardMem))
		} else {
			resourceList[corev1.ResourceName(gpuResourceKey)] = resource.MustParse(fmt.Sprintf("%d", task.GpuNum))
		}
		// resourceList[CudaCoreResourceType] = resource.MustParse(task.GpuType)
	}
	return resourceList
}

func (x *K8SHandler) DescribeNodes(ctx context.Context, labels map[string]string) (*corev1.NodeList, error) {
	logger := common.GetLogger(ctx)
	labelSelector := ""
	for k, v := range labels {
		labelSelector += k + "=" + v + ","
	}
	if strings.HasSuffix(labelSelector, ",") {
		labelSelector = strings.TrimSuffix(labelSelector, ",")
	}
	listOption := metav1.ListOptions{
		LabelSelector: labelSelector,
		Limit:         100,
	}
	nodes, err := x.client.ListNodes(ctx, listOption)
	if nil != err {
		logger.Errorf("describe k8s nodes by [%s] failed: %+v", labelSelector, err)
		return nil, common.Errorf(ctx, code.Code_InternalError__K8SClientFailed, err)
	}

	return nodes, nil
}

// TryUpdateJobResource 当k8s job running 时， 获取分配pod的ip，并更新job 的 resource
func (x *K8SHandler) tryUpdateJobResource(ctx context.Context, job *model.Job) bool {
	if job.JobStatus != types.JobStatus_JOB_STATUS_RUNNING {
		return true
	}
	logger := common.GetLogger(ctx)
	logger.Debugf("[tryUpdateJobResource] try update k8s job resource: [%+v]", job)
	// 根据任务类型获取参数转换器，将 job 转换为 k8s 任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		logger.Errorf("get job parses failed: [%+v]", common.Errorf(ctx, code.Code_InvalidParameter__K8SParserNotExist))
		return false
	}
	jobInfo, err := parser.GetK8STaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to k8s task info fail")
		logger.Errorf("parse k8s job failed: [%+v]", err)
		return false
	}

	pod, err := x.client.GetPod(ctx, x.opt.DefaultK8SCfg.DefaultNameSpace, jobInfo.PodName, metav1.GetOptions{})
	if nil != err {
		logger.Errorf("get k8s job pod [%s] failed: [%+v]", jobInfo.GetPodName(), err)
		return false
	}

	if nil != pod && len(pod.Spec.Containers) > 0 {
		for _, container := range pod.Spec.Containers {
			if container.Name == ContainerDefaultName && len(container.Ports) > 0 && container.Ports[0].ContainerPort > 0 {
				job.Resources[0].Ports = []int32{container.Ports[0].ContainerPort}
			}
		}
		if job.Resources[0].HostIp != job.Resources[0].Ip && len(job.Resources[0].Ip) > 0 {
			job.Resources[0].HostIp = job.Resources[0].Ip
		}
		job.Resources[0].Ip = pod.Status.PodIP
	}

	logger.Debugf("[tryUpdateJobResource] try update k8s job resource: [%+v]", job)
	return true
}
