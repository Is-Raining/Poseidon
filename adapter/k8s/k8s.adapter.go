package k8s

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"git.woa.com/yt-industry-ai/poseidon/adapter/external"
	pb_k8s "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/k8s"
	pb_local "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/local"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/dao"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	"github.com/jmoiron/sqlx"
	v1 "k8s.io/api/core/v1"

	types_ "git.woa.com/yt-industry-ai/poseidon/types"
)

const (
	K8SAdapterType       = "psd.adapter.k8s"
	DefaultK8SGpuTypeKey = "GpuType"
	DefaultK8SGpuNumKey  = "GpuCount"
)

type MountVolumeMod struct {
	MountType  pb_k8s.MountType
	CephParams pb_k8s.MountCephPathParam
	NFSParams  pb_k8s.MountNfsPathParam
}

type K8SCfg struct {

	// k8s 相关
	ClusterType      string             // EXTERNAL: 外部集群，INTERNAL: 本地集群
	ClusterAddr      string             // EXTERNAL 配置参数, k8s api 地址
	ClusterToken     string             // EXTERNAL 配置参数，k8s token
	HostConfigPath   string             // INTERNAL 配置参数, 本地config 根目录, 默认为 /root
	DefaultNameSpace string             // k8s namespace 默认为 tiocr
	NodeSelector     map[string]string  // k8s 资源节点选择器 labels
	NodeGpus         map[string]GpuInfo // k8s 节点GPU类型
	GpuResourceKey   string             // k8s gpu 资源节点调度名称： ”nvidia.com/gpu“
	ResourceNodeKey  string             // k8s node 资源筛选key
	ImagePullSecKey  string             // k8s 镜像拉取 secret name
	MountMod         MountVolumeMod     // 数据盘挂载模式： 支持本地、Cephfs、NFS
	UserGpuTypeKey   string             // k8s 用户自定义 gpuTypeKey
	UserGpuCountKey  string             // k8s 用户自定义 gpuCountKey
	FinalizerKey     string             // 用于任务结束后清理资源的 finalizerKey
}

type GpuInfo struct {
	GpuNum     int32
	GpuType    string
	GpuCardMem int32 // 显卡内存大小, 单位：G
}

// Option ...
type Option struct {
	DB        *sqlx.DB
	DBTimeout time.Duration
	JobTypes  []int32 // 处理哪些任务

	ProjectManager types_.ProjectManager // 必填：项目配额管理器

	JobDir          string                 // job 根目录
	LogDir          string                 // job 日志根目录
	ErrorMarshaler  types_.ErrorMarshaler  // 错误码转换接口
	ContainerEnvOpt *external.ContainerEnv // 容器内环境变量
	DefaultK8SCfg   *K8SCfg                // k8s资源描述参数

	JobParsers map[int32]K8SJobParser // job 参数转换器: JobType -> K8SJobParser

}

// K8sAdapter ...
type K8SAdapter struct {
	*external.ExternalAdapter             // 实现 Adapter 接口
	k8sProcessor              *K8SHandler // 实现 ExternalHandler
	defaultK8SCfg             *K8SCfg     // k8s资源描述参数
	jobDao                    *dao.JobDao // 任务存储层
	opt                       *Option     // 配置信息
}

// k8s 适配器参数
func NewK8sAdapterOption() *Option {
	return &Option{
		ContainerEnvOpt: &external.ContainerEnv{},
		DefaultK8SCfg:   &K8SCfg{},
		JobParsers:      make(map[int32]K8SJobParser),
	}
}

// NewK8sAdapter ...
func NewK8SAdapter(opt *Option) *K8SAdapter {
	k8sProcessor, err := NewK8SHandler(context.Background(), opt)
	if nil != err {
		common.GetLogger(context.Background()).Errorf("get k8s processor failed: %+v", err)
		return nil
	}

	// 该适配器要处理的 job 类型为 JobParsers 的 key 集合
	jobTypes := []int32{}
	for jobType := range opt.JobParsers {
		jobTypes = append(jobTypes, jobType)
	}

	// 传入 TimakerHandler，创建 ExternalAdapter 实例
	externalAdapter := external.NewExternalAdapter(k8sProcessor, &external.ExternalAdapterOption{
		DB:              opt.DB,
		DBTimeout:       opt.DBTimeout,
		JobDir:          opt.JobDir,
		LogDir:          opt.LogDir,
		ErrorMarshaler:  opt.ErrorMarshaler,
		JobTypes:        jobTypes,
		AdapterTypes:    []string{K8SAdapterType},
		CheckPeriod:     time.Minute,
		ContainerEnvOpt: opt.ContainerEnvOpt,
	})
	return &K8SAdapter{
		ExternalAdapter: externalAdapter,
		k8sProcessor:    k8sProcessor,
		defaultK8SCfg:   opt.DefaultK8SCfg,
		jobDao:          dao.NewJobDao(opt.DB, nil),
		opt:             opt,
	}
}

func (x *K8SAdapter) GetAdapterType(ctx context.Context) string {
	return K8SAdapterType
}

// Allocate TODO
// TODO 增加了timaker 任务资源的校验，资源不足时让任务在数据库继续等待，否则会提交失败
func (x *K8SAdapter) Allocate(
	ctx context.Context, job *model.Job, gather *model.Gather,
) ([]*types.Resource, error) {
	logger := common.GetLogger(ctx)
	logger.Debug("start allocate k8s job resource")
	resources := make([]*types.Resource, 0)
	resourceK8S, err := x.DescribeResource(ctx, &pb_k8s.DescribeK8SResourceRequest{RequestId: common.GetRequestId(ctx)})
	if nil != err {
		logger.Errorf("get k8s resource failed: %+v", err)
		return nil, err
	}
	// var jobNode *types.Node
	// 获取可用节点
	candinateNodes := make([]*types.Node, 0)
	if len(resourceK8S.Nodes) > 0 {
		if job.GetResourceRequire().GpuNum > 0 {
			for _, node := range resourceK8S.Nodes {
				if node.FreeResource.ResourceType != job.ResourceRequire.ResourceType {
					continue
				}
				if len(job.ResourceRequire.GpuType) > 0 && node.FreeResource.GpuType != job.ResourceRequire.GpuType {
					logger.Infof("ignore node gpu type: %+v", node)
					continue
				}
				if len(node.FreeResource.Gpus) >= int(job.GetResourceRequire().GpuNum) {
					// jobNode = node
					candinateNodes = append(candinateNodes, node)
				}
			}
		} else {
			// jobNode = resourceK8S.Nodes[0]
			candinateNodes = resourceK8S.Nodes
		}
	}

	jobNode, err := x.SelectJobNodes(ctx, candinateNodes, gather, job)
	if nil != err {
		logger.Errorf("select job nodes failed: [%+v]", err)
		return nil, err
	}
	if jobNode == nil {
		logger.Errorf("job [%+v] container job resource gpu in sufficient: [%+v]", job, resourceK8S)
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__GPUInsufficient)
	}
	fakeGpus := make([]int32, 0)
	for id := int32(0); id < job.ResourceRequire.GpuNum; id++ {
		fakeGpus = append(fakeGpus, id)
	}
	resources = append(resources, &types.Resource{
		Label:        job.ResourceRequire.Label,
		Ip:           jobNode.Ip,
		Gpus:         fakeGpus,
		GpuType:      jobNode.FreeResource.GpuType,
		ResourceType: jobNode.FreeResource.ResourceType,
		HostName:     jobNode.Name,
	})
	job.Resources = resources
	if len(job.GetJobLive().StartTime) < 1 {
		job.GetJobLive().StartTime = utils.FormatTime(time.Now())
		job.GetJobLive().CreateTime = utils.FormatTime(time.Now())
	}
	job.GetJobLive().Error = nil
	err = x.jobDao.UpdateJob(ctx,
		[]string{model.ColJobJobLives, model.ColJobResource},
		[]string{model.ColJobJobId},
		&model.Job{
			JobId:     job.JobId,
			JobLives:  job.JobLives,
			Resources: job.Resources, // 保存所分配的资源
		},
	)
	if err != nil {
		logger.WithError(err).Errorln("update job fail")
		return nil, err
	}
	logger.Debugf("[K8SAdapterAllocate] job use resource [%+v]", resources)
	return resources, nil
}

func (x *K8SAdapter) SelectJobNodes(ctx context.Context, candinateNodes []*types.Node,
	gather *model.Gather, job *model.Job) (*types.Node, error) {
	logger := common.GetLogger(ctx)
	if len(candinateNodes) <= 0 {
		return nil, nil
	}
	if job.GetResourceRequire().GpuNum <= 0 {
		return candinateNodes[0], nil
	}
	// 根据项目配额进行节点过滤
	projectQuotas, err := x.opt.ProjectManager.GetQuotas(ctx)
	if nil != err {
		logger.Errorf("get project quotas failed: %+v", err)
		return nil, err
	}
	if len(job.ResourceRequire.GpuType) <= 0 {
		for gpuType, projectQuota2 := range projectQuotas[job.ProjectId] {
			projectQuotaleft := projectQuota2
			if _, ok := gather.ProjectCounting[job.ProjectId]; !ok {
				gather.ProjectCounting[job.ProjectId] = map[string]int{}
			}
			for _, runningJob := range gather.Jobs {
				if runningJob.ProjectId != job.ProjectId {
					logger.Debugf("[SelectJobNodes] skip job: [%+v] for project or is running", runningJob)
					continue
				}
				if !runningJob.IsResourceConsuming() {
					logger.Debugf("[SelectJobNodes] skip job: [%+v] for is running", runningJob)
					continue
				}
				for _, resource := range runningJob.GetResources() {
					if resource.GpuType != job.ResourceRequire.GpuType {
						logger.Debugf("[SelectJobNodes] skip job: [%+v] for job gpu type", runningJob)
						continue
					}
					projectQuotaleft = projectQuotaleft - len(resource.Gpus)
				}
			}
			if usedNum, ok := gather.ProjectCounting[job.ProjectId][gpuType]; ok {
				projectQuotaleft -= usedNum
			}
			logger.Debugf("[SelectJobNodes] project quota limit: [%d]", projectQuotaleft)
			if projectQuotaleft >= int(job.ResourceRequire.GpuNum) {
				job.ResourceRequire.GpuType = gpuType
				for _, node := range candinateNodes {
					if node.TotalResource.GpuType == gpuType {
						if _, ok := gather.ProjectCounting[job.ProjectId][gpuType]; !ok {
							gather.ProjectCounting[job.ProjectId][gpuType] = 0
						}
						gather.ProjectCounting[job.ProjectId][gpuType] += int(job.ResourceRequire.GpuNum)
						return node, nil
					}
					logger.Debugf("[SelectJobNodes] ignore node [%+v] for gpuType [%s]", node, gpuType)
				}
			}
		}
		logger.Debugf("[SelectJobNodes] project has no quota [%d][%+v]", job.ResourceRequire.GpuNum, projectQuotas[job.ProjectId])
		return nil, nil
	} else {
		projectQuota, ok := projectQuotas[job.ProjectId][job.ResourceRequire.GpuType]
		if !ok {
			logger.Warnf("project gputype has no quota")
			return nil, nil
		}
		if _, ok := gather.ProjectCounting[job.ProjectId]; !ok {
			gather.ProjectCounting[job.ProjectId] = map[string]int{}
		}
		for _, runningJob := range gather.Jobs {
			if runningJob.ProjectId != job.ProjectId {
				logger.Debugf("[SelectJobNodes] skip job: [%+v] for project or is running", runningJob)
				continue
			}
			if !runningJob.IsResourceConsuming() {
				logger.Debugf("[SelectJobNodes] skip job: [%+v] for is running", runningJob)
				continue
			}
			for _, resource := range runningJob.GetResources() {
				if resource.GpuType != job.ResourceRequire.GpuType {
					logger.Debugf("[SelectJobNodes] skip job: [%+v] for job gpu type", runningJob)
					continue
				}
				projectQuota = projectQuota - len(resource.Gpus)
			}
		}
		if usedNum, ok := gather.ProjectCounting[job.ProjectId][job.ResourceRequire.GpuType]; ok {
			projectQuota -= usedNum
		}
		logger.Debugf("[SelectJobNodes] project quota limit: [%d]", projectQuota)
		if projectQuota < int(job.ResourceRequire.GpuNum) {
			logger.Debugf("[SelectJobNodes] project has no quota [%d/%d]", job.ResourceRequire.GpuNum, projectQuota)
			return nil, nil
		}
		if _, ok := gather.ProjectCounting[job.ProjectId][job.ResourceRequire.GpuType]; !ok {
			gather.ProjectCounting[job.ProjectId][job.ResourceRequire.GpuType] = 0
		}
		gather.ProjectCounting[job.ProjectId][job.ResourceRequire.GpuType] += int(job.ResourceRequire.GpuNum)

		return candinateNodes[0], nil
	}
}

// DescribeResource 查询集群的可用资源
func (x *K8SAdapter) DescribeResource(
	ctx context.Context, req *pb_k8s.DescribeK8SResourceRequest,
) (*pb_k8s.DescribeK8SResourceResponse, error) {
	logger := common.GetLogger(ctx)
	rsp := pb_k8s.DescribeK8SResourceResponse{
		Nodes: make([]*types.Node, 0),
	}
	logger.Debugf("start describe k8s resource")
	nodeList, err := x.k8sProcessor.DescribeNodes(ctx, x.defaultK8SCfg.NodeSelector)
	if nil != err {
		logger.Errorf("get k8s nodes by [%+v] failed: %+v", x.defaultK8SCfg.NodeSelector, err)
		return nil, err
	}
	// 从任务存储层读取指定任务和适配器类型的 job
	inConds := []string{model.ColJobJobStatus, model.ColJobAdapterType}
	if len(x.opt.JobTypes) > 0 {
		inConds = append(inConds, model.ColJobJobType)
	}
	processingJobs, err := x.jobDao.GetJobs(ctx, nil, inConds, nil,
		map[string]interface{}{
			model.ColJobJobStatus:   types.GetProcessingJobStatuses().EnumInt32s(),
			model.ColJobJobType:     x.opt.JobTypes,
			model.ColJobAdapterType: K8SAdapterType,
		},
	)
	nodeUsedGpu := make(map[string]int)
	for _, processJob := range processingJobs {
		if processJob.JobStatus == types.JobStatus_JOB_STATUS_WAITING ||
			processJob.JobStatus == types.JobStatus_JOB_STATUS_RESTARTING {
			continue
		}
		for _, resource := range processJob.Resources {
			if _, ok := nodeUsedGpu[resource.HostIp]; !ok {
				nodeUsedGpu[resource.HostIp] = 0
			}
			nodeUsedGpu[resource.HostIp] += len(resource.Gpus)
		}
	}

	for _, k8sNode := range nodeList.Items {
		node := &types.Node{
			Name: k8sNode.Name,
		}
		for _, nodeAddr := range k8sNode.Status.Addresses {
			if nodeAddr.Type == v1.NodeInternalIP {
				node.Ip = nodeAddr.Address
			}
		}
		node.TotalResource = &types.Resource{
			Ip:           node.Ip,
			ResourceType: types.ResourceType_RESOURCE_TYPE_K8S,
		}
		node.FreeResource = &types.Resource{
			Ip:           node.Ip,
			ResourceType: types.ResourceType_RESOURCE_TYPE_K8S,
		}

		if gpuInfo, ok := x.defaultK8SCfg.NodeGpus[node.Name]; ok {
			logger.Debugf("try describe gpu info from config map: [%+v]", x.defaultK8SCfg.NodeGpus)
			node.TotalResource.GpuType = gpuInfo.GpuType
			node.FreeResource.GpuType = gpuInfo.GpuType
			for id := int32(0); id < gpuInfo.GpuNum; id++ {
				node.TotalResource.Gpus = append(node.TotalResource.Gpus, id)
			}
			freeGpuNum := gpuInfo.GpuNum
			if useGpus, ok := nodeUsedGpu[node.Ip]; ok {
				freeGpuNum -= int32(useGpus)
			}
			for id := int32(0); id < freeGpuNum; id++ {
				node.FreeResource.Gpus = append(node.FreeResource.Gpus, id)
			}
		} else {
			nodeLabels := k8sNode.ObjectMeta.Labels
			logger.Debugf("try describe gpu info from node labels: [%+v]", nodeLabels)
			gpuTypeKey := x.defaultK8SCfg.UserGpuTypeKey
			if len(gpuTypeKey) <= 0 {
				gpuTypeKey = DefaultK8SGpuTypeKey
			}
			gpuCntKey := x.defaultK8SCfg.UserGpuCountKey
			if len(gpuCntKey) <= 0 {
				gpuCntKey = DefaultK8SGpuNumKey
			}
			if gpuType, ok := nodeLabels[gpuTypeKey]; ok {
				node.TotalResource.GpuType = gpuType
				node.FreeResource.GpuType = gpuType
			} else {
				logger.Warnf("node [%s] labels [%+v] with no gpu type key [%s]", node.Name, nodeLabels, gpuTypeKey)
			}
			if gpuNumStr, ok := nodeLabels[gpuCntKey]; ok {
				gpuCnt, err := strconv.ParseInt(gpuNumStr, 10, 64)
				if nil == err {
					for id := int32(0); id < int32(gpuCnt); id++ {
						node.TotalResource.Gpus = append(node.TotalResource.Gpus, id)
					}
					freeGpuNum := int32(gpuCnt)
					if useGpus, ok := nodeUsedGpu[node.Ip]; ok {
						freeGpuNum -= int32(useGpus)
					}
					for id := int32(0); id < freeGpuNum; id++ {
						node.FreeResource.Gpus = append(node.FreeResource.Gpus, id)
					}
				} else {
					logger.Errorf("parse node gpu num [%s] failed: %+v", gpuNumStr, err)
				}
			} else {
				logger.Warnf("node [%s] labels [%+v] with no gpu count key [%s]", node.Name, nodeLabels, gpuCntKey)
			}
		}
		rsp.Nodes = append(rsp.Nodes, node)
	}
	rspByte, _ := json.Marshal(&rsp)
	logger.Debugf("describe k8s resource: %s", string(rspByte))
	return &rsp, nil
}

// DescribeUsedResource 查询集群的已用资源
func (x *K8SAdapter) DescribeUsedResource(
	ctx context.Context, req *pb_local.DescribeUsedResourcesRequest,
) (resp *pb_local.DescribeUsedResourcesResponse, err error) {
	resp = &pb_local.DescribeUsedResourcesResponse{
		Projects: make(map[string]*types.Resources),
		Nodes:    make(map[string]*types.Resource),
	}
	logger := common.GetLogger(ctx)
	logger.Infof("start Describe K8S Used Resource")
	// 从任务存储层读取指定任务和适配器类型的 job
	inConds := []string{model.ColJobJobStatus, model.ColJobAdapterType}
	if len(x.opt.JobTypes) > 0 {
		inConds = append(inConds, model.ColJobJobType)
	}
	processingJobs, err := x.jobDao.GetJobs(ctx, nil, inConds, nil,
		map[string]interface{}{
			model.ColJobJobStatus:   types.GetProcessingJobStatuses().EnumInt32s(),
			model.ColJobJobType:     x.opt.JobTypes,
			model.ColJobAdapterType: K8SAdapterType,
		},
	)
	for _, processJob := range processingJobs {
		if processJob.JobStatus == types.JobStatus_JOB_STATUS_WAITING {
			continue
		}
		if !processJob.IsResourceConsuming() {
			continue
		}
		nodes := make(map[string]*types.Resource)
		for _, resource := range processJob.Resources {
			if _, ok := nodes[resource.HostIp]; !ok {
				nodes[resource.HostIp] = &types.Resource{}
			}
			nodes[resource.HostIp].GpuType = resource.GpuType
			nodes[resource.HostIp].ResourceType = resource.ResourceType
			nodes[resource.HostIp].Gpus = append(nodes[resource.HostIp].Gpus, resource.Gpus...)
			nodes[resource.HostIp].Ip = resource.HostIp
		}

		for ip, resource := range nodes {
			if _, ok := resp.Nodes[ip]; ok {
				resp.Nodes[ip].Gpus = append(resp.Nodes[ip].Gpus, resource.Gpus...)
			} else {
				resp.Nodes[ip] = &types.Resource{}
				resByte, _ := json.Marshal(resource)
				json.Unmarshal(resByte, resp.Nodes[ip])
			}
		}
		if projectResource, ok := resp.Projects[processJob.ProjectId]; ok {
			for ip, resource := range nodes {
				if _, ok := projectResource.Resources[ip]; ok {
					projectResource.Resources[ip].Gpus = append(projectResource.Resources[ip].Gpus, resource.Gpus...)
				} else {
					projectResource.Resources[ip] = resource
				}
			}
			resp.Projects[processJob.ProjectId] = projectResource
		} else {
			resp.Projects[processJob.ProjectId] = &types.Resources{
				Resources: nodes,
			}
		}
	}
	logger.Infof("end Describe K8S Used Resource: %+v", resp)
	return resp, nil
}

// TryUpdateJobResource 当k8s job running 时， 获取分配pod的ip，并更新job 的 resource
func (x *K8SAdapter) TryUpdateJobResource(ctx context.Context, job *model.Job) bool {
	if job.JobStatus != types.JobStatus_JOB_STATUS_RUNNING {
		return true
	}
	return x.k8sProcessor.tryUpdateJobResource(ctx, job)
}
