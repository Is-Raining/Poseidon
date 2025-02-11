package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"git.woa.com/yt-industry-ai/grpc-go-ext/log"
	adapter_ "git.woa.com/yt-industry-ai/poseidon/adapter"
	allocators "git.woa.com/yt-industry-ai/poseidon/adapter/local/allocators"
	agent_ "git.woa.com/yt-industry-ai/poseidon/agent"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/agent"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/dao"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/discovery"
	"git.woa.com/yt-industry-ai/poseidon/pkg/distlock"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
)

const (
	DefaultDBTimeout = 5 * time.Second // 默认数据库操作超时时间
)

const (
	LocalAdapterType = "psd.adapter.local" // 调度器类型
)

// 本地集群适配器配置
type Option struct {
	// 基本配置
	JobTypes []int32 // 可选：要处理的任务类型，默认为空，为空代表全部任务类型

	// 数据库相关配置
	DB        *sqlx.DB      // 必填：数据库实例
	DBTimeout time.Duration // 可选：数据库操作超时时间。默认为 DefaultDBTimeout

	// Agent 相关配置
	AgentClient agent_.AgentClient // 必填：Agent 客户端

	// 可选：Agent离线超时时间，超时后正在该节点上运行的任务被置为失败，该节点被屏蔽，<=0 代表一旦 Agent 离线，立即将任务设置为失败
	AgentTimeout time.Duration

	// 设备相关配置
	ProjectManager             types_.ProjectManager    // 必填：项目配额管理器
	Devices                    []*types_.Device         // 选填：设备集合，与 DiskLockOption 二选一
	PortRange                  []int32                  // 选填：可供分配的端口集合（闭区间）
	FunctionJobLimiting        map[int32]int            // 选填：本地函数并发在每个节点的并发限制: <job_type(int32), count>
	ProjectFunctionJobLimiting map[int32]int            // 选填：项目维度本地函数的并发限制: <job_type(int32), count>
	DistLockOption             *distlock.DistLockOption // 选填：Etcd 参数，配置该参数后会从 Etcd 获取设备集合
	Namespace                  string                   // 选填：自动注册的 Etcd 键前缀

	// 硬盘相关配置
	MaxDiskUsedPercent   float32 // 选填：节点硬盘使用率超过该阈值时，屏蔽该节点。默认为 0，0 代表不启用该功能
	MaxInodesUsedPercent float32 // 选填：节点 inode 使用率超过该阈值时，屏蔽该节点。默认为 0，0 代表不启用该功能
}

// 创建适配器参数
func NewLocalAdapterOption() *Option {
	return &Option{}
}

// 本地集群适配器
type LocalAdapter struct {
	adapter_.UnimplementedDispatcherAdapter // 实现接口 DispatcherAdapter

	opt        *Option             // 配置
	discovery  discovery.Discovery // 设备发现组件
	cache      *LocalAdapterCache  // 临时缓存
	nodeStates sync.Map            // 节点状态
	jobDao     *dao.JobDao         // 任务存储层
}

// 节点状态
type NodeState struct {
	DiskUsage *types.DiskUsageStat // 磁盘用量
	Error     error                // 心跳错误
}

// 创建本地集群适配器 ...
func NewLocalAdapter(opt *Option) *LocalAdapter {
	if opt.DBTimeout.Milliseconds() < 1 {
		opt.DBTimeout = DefaultDBTimeout
	}
	c := &LocalAdapter{
		opt:    opt,
		cache:  &LocalAdapterCache{},
		jobDao: dao.NewJobDao(opt.DB, nil),
	}
	if opt.DistLockOption != nil {
		// 初始化设备发现组件
		c.discovery = discovery.NewDiscovery(opt.DistLockOption, opt.Namespace)
	}

	if len(opt.PortRange) > 0 &&
		(len(opt.PortRange) != 2 || opt.PortRange[0] > opt.PortRange[1] ||
			opt.PortRange[0] <= 0 || opt.PortRange[1] <= 0) {
		// 端口范围无效
		log.Fatalf("invalid port range: %v", opt.PortRange)
	}

	// 对通过配置传入的设备，添加默认标签
	for _, device := range c.opt.Devices {
		if device.Label == nil {
			device.Label = types_.DeviceLabel{}
		}
		if _, ok := device.Label[types_.ResourceLabelJobType]; !ok {
			device.Label[types_.ResourceLabelJobType] = []string{}
		}
	}
	log.Infof("local adapter function job limiting: %v", c.opt.FunctionJobLimiting)
	log.Infof("local adapter project job limiting: %v", c.opt.ProjectFunctionJobLimiting)
	log.Infof("local adapter devices: %v", c.opt.Devices)
	log.Infof("local adapter port range: %v", c.opt.PortRange)

	return c
}
func (x *LocalAdapter) GetAdapterType(ctx context.Context) string {
	return LocalAdapterType
}

// 获取设备列表
// totalNodes: CPU 节点列表
// totalGpus: GPU 卡列表
func (x *LocalAdapter) GetDevices(ctx context.Context) (totalNodes types_.DeviceSet, totalGpus types_.DeviceSet, err error) {
	logger := common.GetLogger(ctx)

	var devices []*types_.Device
	if x.discovery == nil {
		// 发现组件为空，则使用通过配置传入的设备列表
		devices = x.opt.Devices
	} else {
		// 发现组件非空，则通过发现组件获取自注册的设备列表
		// TODO(chuangchen): 缓存，避免每次都从 ETCD 获取
		keys, err := x.discovery.GetKeys(ctx)
		if err != nil {
			logger.WithError(err).Errorf("get keys from discovery fail")
			return nil, nil, err
		}
		for key, value := range keys {
			// 将自注册的 key/value 反序列化为设备列表
			logger.Debugf("local adapter discover key: %s, value: %s", key, value)

			args := make([]*types_.Device, 0)
			if err := json.Unmarshal([]byte(value), &args); err != nil {
				logger.Debugf("local adapter unmarhsal discovered device failed. key: %s, value: %s", key, value)
				return nil, nil, common.Errorf(ctx, code.Code_InternalError__JsonUnMarshalError)
			}
			devices = append(devices, args...)
		}
		log.Infof("local adapter discover devices: %v", devices)
	}

	// 去重集合
	totalNodes = types_.NewDeviceSet()
	totalGpus = types_.NewDeviceSet()
	for _, v := range devices {
		if v.GpuId < 0 {
			// totalNodes 仅包含 CPU
			totalNodes.Insert(v)
		} else {
			totalGpus.Insert(v)
		}
	}
	return totalNodes, totalGpus, nil
}

// 实现 DispatcherAdapter.Gather
func (x *LocalAdapter) Gather(ctx context.Context) (gather *model.Gather, err error) {
	logger := common.GetLogger(ctx)

	// 获取设备列表
	totalNodes, totalGpus, err := x.GetDevices(ctx)
	if err != nil {
		logger.WithError(err).Errorf("get devices fail")
		return nil, err
	}
	gather = &model.Gather{
		TotalNodes:                 totalNodes,
		TotalGpus:                  totalGpus,
		FunctionJobLimiting:        x.opt.FunctionJobLimiting,
		ProjectFunctionJobLimiting: x.opt.ProjectFunctionJobLimiting,
	}

	// 从数据库获取待处理任务列表
	eqConds := []string{model.ColJobAdapterType}
	inConds := []string{model.ColJobJobStatus}
	if len(x.opt.JobTypes) > 0 {
		inConds = append(inConds, model.ColJobJobType)
	}
	ctx, cancel := context.WithTimeout(ctx, x.opt.DBTimeout)
	defer cancel()

	gather.Jobs, err = x.jobDao.GetJobs(ctx, eqConds, inConds, nil,
		map[string]interface{}{
			model.ColJobJobStatus:   types.GetProcessingJobStatuses().EnumInt32s(),
			model.ColJobJobType:     x.opt.JobTypes,
			model.ColJobAdapterType: LocalAdapterType,
		},
	)
	if err != nil {
		logger.WithError(err).Errorf("get processing jobs fail")
		return nil, err
	}
	sort.Sort(gather.Jobs) // 按默认方式排序

	// 从 ProjectManager 获取项目配额设置
	gather.ProjectQuota, err = x.opt.ProjectManager.GetQuotas(ctx)
	if err != nil {
		logger.WithError(err).Errorln("get project quotas")
		return nil, err
	}
	logger.Debugf("get quota: %v", gather.ProjectQuota)

	// 根据当前正在运行的任务计算已使用和剩余的资源
	// TODO(chuangchen): 缓存结果，避免重复计算，注意多实例的情况
	gather.FreeGpus = types_.NewDeviceSet(totalGpus.SortedSlice()...)
	gather.UsedGpus = types_.NewDeviceSet()
	gather.ProjectCounting = make(map[string]map[string]int)
	gather.FunctionJobCounting = make(map[string]map[int32]int)
	gather.ProjectFunctionJobCounting = make(map[string]map[int32]int)
	gather.UsedPorts = make(map[string]map[int32]bool)
	for _, job := range gather.Jobs {
		if !job.IsResourceConsuming() {
			// 跳过不占用任何资源的任务（等待中...）
			continue
		}
		if job.GetResourceRequire().GetGpuNum() > 0 {
			// 使用 GPU 的设备
			if _, ok := gather.ProjectCounting[job.ProjectId]; !ok {
				gather.ProjectCounting[job.ProjectId] = make(map[string]int)
			}
			for _, resource := range job.GetResources() {
				// 对使用的每个节点
				for _, gpuId := range resource.GetGpus() {
					// 对使用的每个 GPU 卡
					device := types_.NewDevice(resource.GetIp(), gpuId)
					if !x.checkDeviceInGather(ctx, job, device, gather) {
						// 检查 gpu 是否在设备列表中
						continue
					}
					// 将 GPU 从空闲集合移动到使用中集合
					if gather.FreeGpus.Contains(device) {
						gather.FreeGpus.MoveTo(gather.UsedGpus, device)
					}
					// 修改已使用 GPU 配额
					gather.ProjectCounting[job.ProjectId][resource.GetGpuType()]++
				}
			}
		} else {
			// 使用 CPU 的设备
			for _, resource := range job.GetResources() {
				// 对使用的每个节点
				device := types_.NewDevice(resource.GetIp(), -1)
				if !x.checkDeviceInGather(ctx, job, device, gather) {
					// 检查节点是否在设备列表中
					continue
				}
				// 修改该节点的 Function 任务个数
				if _, ok := gather.FunctionJobCounting[resource.GetIp()]; !ok {
					gather.FunctionJobCounting[resource.GetIp()] = make(map[int32]int)
				}
				gather.FunctionJobCounting[resource.GetIp()][job.JobType]++
			}
		}
		// 端口资源处理
		for _, resource := range job.GetResources() {
			if resource == nil || len(resource.Ports) == 0 {
				continue
			}
			// 修改该节点使用中的端口集合
			if _, ok := gather.UsedPorts[resource.GetIp()]; !ok {
				gather.UsedPorts[resource.GetIp()] = make(map[int32]bool)
			}
			for _, port := range resource.Ports {
				gather.UsedPorts[resource.GetIp()][port] = true
			}
		}
		// 记录每个项目的 Function 任务个数
		if _, ok := gather.ProjectFunctionJobCounting[job.ProjectId]; !ok {
			gather.ProjectFunctionJobCounting[job.ProjectId] = make(map[int32]int)
		}
		gather.ProjectFunctionJobCounting[job.ProjectId][job.JobType]++
	}

	return gather, nil
}

// 返回 gather 是否包含 device
func (x *LocalAdapter) checkDeviceInGather(
	ctx context.Context, job *model.Job, device *types_.Device, gather *model.Gather) bool {
	if device.GpuId >= 0 && gather.TotalGpus.Contains(device) {
		return true
	}
	if device.GpuId < 0 && gather.TotalNodes.Contains(device) {
		return true
	}
	// 设备列表不包含该设备时（集群发生了变更）
	if x.opt.AgentTimeout.Milliseconds() <= 0 ||
		time.Since(x.cache.GetOrSetJobFirstRetryTime(job)) > x.opt.AgentTimeout {
		// 任务距离第一次心跳失败大于 AgentTimeout，将任务设置为失败
		log.Warnf("job %s require device %s, set status to failed", job.JobId, device)

		// 清除心跳信息
		x.cache.ClearFirstRetryTime(job)

		// 设置任务失败
		job.JobStatus = types.JobStatus_JOB_STATUS_STOPPING
		job.GetJobLive().Error = common.ErrorfProto(ctx, code.Code_InternalError__AgentNotInDeviceSet)
	}
	return false
}

// 启动后台线程
func (x *LocalAdapter) StartDaemon(ctx context.Context) {
	updater := func() {
		totalNodes, totalGpus, err := x.GetDevices(ctx)
		if err != nil {
			log.WithError(err).Errorf("get devices fail")
			return
		}

		// 去重 IP
		ips := map[string]struct{}{}
		for _, v := range totalNodes {
			ips[v.Ip] = struct{}{}
		}
		for _, v := range totalGpus {
			ips[v.Ip] = struct{}{}
		}

		for ip := range ips {
			// 依次对轮询每个节点的状态
			// TODO(chuangchen): 并发查询
			node, err := x.DescribeNode(ctx, ip)
			if err != nil {
				log.WithError(err).Errorln("describe node fail")
			}
			nodeState := &NodeState{}
			nodeState.Error = err
			if err == nil {
				nodeState.DiskUsage = node.GetDiskUsageStat()
			}
			// 更新节点信息到缓存
			x.nodeStates.Store(ip, nodeState)
		}
	}

	// 启动后台线程之前，先获取一下
	updater()

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				updater()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// 获取指定节点的状态信息
func (x *LocalAdapter) NodeUsage(ip string) (*types.DiskUsageStat, bool) {
	// 从缓存中读取节点状态
	v, ok := x.nodeStates.Load(ip)
	if v == nil || !ok {
		// 缓存中没有
		return nil, false
	}
	state := v.(*NodeState)
	return state.DiskUsage, true
}

// 检查节点状态，若节点不健康，则返回 error
func (x *LocalAdapter) checkNodeHealth(ctx context.Context, ip string) error {
	logger := common.GetLogger(ctx)
	logger = logger.WithField("node_ip", ip)

	// 从缓存中读取节点状态
	v, ok := x.nodeStates.Load(ip)
	if v == nil || !ok {
		// 缓存中没有，默认节点健康
		logger.Warnf("state not exists, default healthy")
		return nil
	}
	state := v.(*NodeState)
	if state.Error != nil {
		// 节点有错误
		logger.Errorf("node has error: %v", state.Error)
		return state.Error
	}

	usage := state.DiskUsage
	if x.opt.MaxDiskUsedPercent > 0 && usage.GetUsedPercent() > x.opt.MaxDiskUsedPercent {
		// 节点硬盘不足
		logger.Errorf("node disk is full")
		return common.Errorf(ctx, code.Code_ResourceInsufficient__DiskUsageTooHigh)
	}
	if x.opt.MaxInodesUsedPercent > 0 && usage.GetInodesUsedPercent() > x.opt.MaxInodesUsedPercent {
		// 节点 inode 不足
		logger.Errorf("node inode is full")
		return common.Errorf(ctx, code.Code_ResourceInsufficient__InodeUsageTooHigh)
	}
	return nil
}

// 实现 DispatcherAdapter.Allocate
func (x *LocalAdapter) Allocate(
	ctx context.Context, job *model.Job, gather *model.Gather,
) (resources []*types.Resource, err error) {
	logger := common.GetLogger(ctx)

	// 任务至少要有 ResourceRequire 参数
	if job.GetResourceRequire() == nil {
		logger.Debugf("resource require is nil")
		return nil, common.Errorf(ctx, code.Code_InvalidParameterValue__ParameterValueInvalid, "ResourceRequire")
	}

	// 添加默认的 requiredLabel: JobType，资源需要支持该任务的类型
	if job.ResourceRequire.Label == nil {
		job.ResourceRequire.Label = map[string]string{}
	}
	job.ResourceRequire.Label[types_.ResourceLabelJobType] = fmt.Sprintf("%d", job.JobType)

	if job.GetResourceRequire().GetGpuNum() > 0 {
		// 分配 GPU
		resources, err = x.allocateGPU(ctx, job, gather)
	} else {
		// 分配节点
		resources, err = x.allocateNode(ctx, job, gather)
	}
	if common.Match(err, code.Code_ResourceInsufficient__GPUInsufficient) ||
		common.Match(err, code.Code_ResourceInsufficient__GPUQuotaInsufficient) ||
		common.Match(err, code.Code_ResourceInsufficient__CPUInsufficient) {
		logger.Debugf("resource is insufficient")
		return nil, err
	} else if err != nil {
		logger.WithError(err).Errorf("allocate resource failed")
		return nil, err
	}

	if job.GetResourceRequire().GetPortNum() > 0 {
		// 分配端口
		err = x.allocatePort(ctx, job, gather, resources)
		if err != nil {
			logger.WithError(err).Errorf("allocate port failed")
			return nil, err
		}
	}

	return resources, err
}

// allocateGPU 分配 gpu 资源（优先按label分配，未指定label，按项目配额根据数量随机分配），支持以下几种情况
// 1. 单机分配
// 2. 多机分配，同型号的显卡资源
// 3. 多机分配，不同型号的显卡资源
func (x *LocalAdapter) allocateGPU(
	ctx context.Context, job *model.Job, gather *model.Gather,
) ([]*types.Resource, error) {
	logger := common.GetLogger(ctx)

	// 过滤
	candidateSet, err := x.filterGPUs(ctx, job, gather)
	if err != nil {
		return nil, err
	}

	// 选择
	selectedSet, err := x.selectGPUs(ctx, job, candidateSet, gather)
	if err != nil {
		return nil, err
	}

	// 根据亲和性标签获取调度策略
	var policy allocators.Policy
	switch GetJobScheduleLabel(job, types_.ScheduleLabelAffinity) {
	case types_.ScheduleLabelAffinityMultiHost:
		policy = allocators.NewMultiplePolicy()
	case types_.ScheduleLabelAffinityMultiHomoHost:
		policy = allocators.NewMultipleHomoPolicy()
	default:
		policy = allocators.NewSinglePolicy()
	}

	// 分配
	devices := policy.Allocate(selectedSet, int(job.GetResourceRequire().GetGpuNum()))
	if len(devices) < 1 {
		logger.Debugf("no available gpus, may gpu fragmentation? gpu insufficient?")
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__GPUInsufficient)
	}

	// 更新 gather
	for _, device := range devices {
		gpuType := device.GetGpuType()
		if _, ok := gather.ProjectCounting[job.ProjectId]; !ok {
			gather.ProjectCounting[job.ProjectId] = map[string]int{}
		}
		// 更新项目 GPU 配额
		gather.ProjectCounting[job.ProjectId][gpuType]++

		// 更新空闲 GPU 集合
		deviceKey := types_.NewDevice(device.Ip, device.GpuId)
		if gather.FreeGpus.Contains(deviceKey) {
			gather.FreeGpus.MoveTo(gather.UsedGpus, deviceKey)
		}
	}

	return devices.ToResources()
}

// 过滤不符合要求的 GPU
func (x *LocalAdapter) filterGPUs(
	ctx context.Context, job *model.Job, gather *model.Gather,
) (candidateSet types_.DeviceSet, err error) {
	logger := common.GetLogger(ctx)

	requiredGpuType := job.GetResourceRequire().GetGpuType()
	requiredGpuNum := job.GetResourceRequire().GetGpuNum()

	var currentCandidateGpus map[string][]*types_.Device

	// 当前系统中满足如下所有条件的 GPU 集合，按 host 聚合
	// 1. 空闲
	// 2. 所有标签符合 requirement
	// 3. GPU 类型符合 requirement
	candidateGpusByLabel := map[string][]*types_.Device{} // ip -> gpus
	candidateNum := 0
	for _, device := range gather.FreeGpus.SortedSlice() {
		if requiredGpuType != "" && device.GetGpuType() != requiredGpuType {
			// requiredGpuType 为空代表可使用任意类型的 GPU
			continue
		}
		if !device.Match(job.GetResourceRequire().GetLabel()) {
			continue
		}
		candidateNum += 1
		candidateGpusByLabel[device.Ip] = append(candidateGpusByLabel[device.Ip], device)
	}
	if candidateNum < int(requiredGpuNum) {
		logger.Debugf("gpu insufficent")
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__GPUInsufficient)
	}
	currentCandidateGpus = candidateGpusByLabel

	// 过滤掉硬盘容量不足或者心跳超时的 host
	candidateGpusByHealth := map[string][]*types_.Device{} // ip -> gpus
	candidateNum = 0
	err = nil
	for ip, gpus := range currentCandidateGpus {
		if err = x.checkNodeHealth(ctx, ip); err != nil {
			continue
		}
		candidateNum += len(gpus)
		candidateGpusByHealth[ip] = gpus
	}
	if candidateNum < int(requiredGpuNum) {
		logger.Debugf("heathy node insufficient")
		// EXPECT: err != nil
		return nil, err
	}
	currentCandidateGpus = candidateGpusByHealth

	// 过滤掉 gpu 个数不足的 host
	affinity := GetJobScheduleLabel(job, types_.ScheduleLabelAffinity)
	if affinity == "" || affinity == types_.ScheduleLabelAffinitySingleHost {
		// TODO(chuangchen): 移动到 singlePolicy
		candidateGpusByAffinity := map[string][]*types_.Device{} // ip -> gpus
		candidateNum = 0
		for ip, gpus := range currentCandidateGpus {
			if len(gpus) < int(requiredGpuNum) {
				continue
			}
			candidateNum += len(gpus)
			candidateGpusByAffinity[ip] = gpus
		}
		if candidateNum < int(requiredGpuNum) {
			logger.Debugf("no host with >=%d %s gpus", requiredGpuNum, requiredGpuType)
			return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__GPUInsufficient)
		}
		currentCandidateGpus = candidateGpusByAffinity
	}

	// 当前候选的所有 GPU 类型
	candidateSet = types_.NewDeviceSet()
	for _, gpus := range currentCandidateGpus {
		candidateSet.Insert(gpus...)
	}

	return candidateSet, nil
}

// 移除部分 GPU 以适配配额要求
func (x *LocalAdapter) selectGPUs(
	ctx context.Context, job *model.Job, candidateSet types_.DeviceSet, gather *model.Gather,
) (selectedSet types_.DeviceSet, err error) {
	logger := common.GetLogger(ctx)

	selectedSet = types_.NewDeviceSet()

	logger.Debugf("quota: %v", gather.ProjectQuota)
	logger.Debugf("candidateSet: %v", candidateSet)

	// 过滤掉部分 GPU，使得剩余候选 GPU 满足配额要求
	currentCount := make(map[string]int)             // 临时计数
	for _, gpu := range candidateSet.SortedSlice() { // 将设备排序，使得相同节点的设备聚合
		// 获取该 gpuTyp 的配额
		gpuType := gpu.GetGpuType()
		limitCount := gather.ProjectQuota[job.ProjectId][gpuType]

		// 获取该 gpuTyp 的当前用量
		if _, ok := currentCount[gpu.GetGpuType()]; !ok {
			currentCount[gpu.GetGpuType()] = gather.ProjectCounting[job.ProjectId][gpuType]
		}
		requireCount := currentCount[gpu.GetGpuType()] + 1
		if requireCount <= limitCount { // 避免超出配额
			currentCount[gpu.GetGpuType()] = requireCount
			selectedSet.Insert(gpu)
		}
	}

	requiredGpuNum := job.GetResourceRequire().GetGpuNum()
	if len(selectedSet) < int(requiredGpuNum) {
		// 可用设备数少于需要的 GPU 数量
		logger.Debugf("gpu quota insufficent")
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__GPUQuotaInsufficient)
	}
	return selectedSet, nil
}

// 分配节点（针对非 GPU 类型任务）
func (x *LocalAdapter) allocateNode(
	ctx context.Context, job *model.Job, gather *model.Gather,
) (allocatedResources []*types.Resource, err error) {
	logger := common.GetLogger(ctx)

	// 选择节点
	availableSet := types_.NewDeviceSet()
	for _, node := range gather.TotalNodes {
		if node.Match(job.GetResourceRequire().GetLabel()) {
			// 检查节点资源标签
			availableSet.Insert(node)
		}
	}
	if len(availableSet) < 1 {
		logger.Debugf("no matched nodes")
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__CPUInsufficient)
	}

	// 机器负载
	load := map[string]int{} // ip -> 负载（该节点上正在运行的该类型任务的个数）
	nodes := make(types_.Devices, 0)
	_, isJobTypeLimit := gather.FunctionJobLimiting[job.JobType]
	for _, node := range availableSet {
		if _, ok := gather.FunctionJobCounting[node.Ip]; !ok {
			gather.FunctionJobCounting[node.Ip] = make(map[int32]int)
		}
		// 没有配置节点运行限制则节点直接放入候选集合 || 配置了，但未超过运行任务个数的节点放入候选集合
		if !isJobTypeLimit || gather.FunctionJobCounting[node.Ip][job.JobType] < gather.FunctionJobLimiting[job.JobType] {
			nodes = append(nodes, node)
			for _, count := range gather.FunctionJobCounting[node.Ip] {
				load[node.Ip] += count
			}
		}
	}
	// 将节点按负载小到大排序
	sort.Slice(nodes, func(i, j int) bool {
		return load[nodes[i].Ip] < load[nodes[j].Ip]
	})

	// 候选集合为空
	if len(nodes) == 0 {
		logger.Debugf("node is not enough, function job counting: %v, limiting: %v",
			gather.FunctionJobCounting, gather.FunctionJobLimiting)
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__CPUInsufficient)
	}

	for _, node := range nodes {
		if err = x.checkNodeHealth(ctx, node.Ip); err != nil {
			// 过滤非健康的节点
			continue
		}
		logger.Debugf("function job counting map: %v", gather.FunctionJobCounting)
		// 更新 gather, 分配完成
		gather.FunctionJobCounting[node.Ip][job.JobType]++
		return []*types.Resource{{Ip: node.Ip}}, nil
	}
	return nil, err
}

// 为 resources 中的每个 host 分配两个端口，对于多机，分配的端口一致
func (x *LocalAdapter) allocatePort(
	ctx context.Context, job *model.Job, gather *model.Gather, resources []*types.Resource) error {
	logger := common.GetLogger(ctx)

	// 需要的端口个数
	portNum := job.GetResourceRequire().GetPortNum()
	if portNum <= 0 {
		return nil
	}

	// 端口范围空
	if len(x.opt.PortRange) == 0 {
		logger.Errorf("port range is empty. cannot allocate ports")
		return common.Errorf(ctx, code.Code_InvalidParameter__PortRangeInvalid)
	}

	// 已使用端口（不区分 IP）
	usedPorts := map[int32]bool{}
	for _, resource := range resources {
		for port := range gather.UsedPorts[resource.GetIp()] {
			usedPorts[port] = true
		}
	}

	// 分配端口
	ports := []int32{}
	for port := int32(x.opt.PortRange[0]); port <= int32(x.opt.PortRange[1]); port += 1 {
		if len(ports) >= int(portNum) {
			break
		}
		if _, ok := usedPorts[port]; !ok {
			// allocate port
			ports = append(ports, port)
			usedPorts[port] = true
		}
	}

	// 端口不足
	if len(ports) < int(portNum) {
		logger.Errorf("insufficient port: allocated: %v, need: %d", ports, portNum)
		return common.Errorf(ctx, code.Code_ResourceInsufficient__PortInsufficient)
	}

	// 从小到大排序
	sort.Slice(ports, func(i, j int) bool {
		return ports[i] < ports[j]
	})

	// 更新 gather
	for _, resource := range resources {
		resource.Ports = ports
		for _, port := range ports {
			if _, ok := gather.UsedPorts[resource.GetIp()]; !ok {
				gather.UsedPorts[resource.GetIp()] = make(map[int32]bool)
			}
			gather.UsedPorts[resource.GetIp()][port] = true
		}
	}
	return nil
}

// 实现 Dispatcher.StartJob
func (x *LocalAdapter) StartJob(ctx context.Context, job *model.Job) (jobErr *types.Error, retry bool, err error) {
	logger := common.GetLogger(ctx)
	logger.Debugf("local adapter start job %+v", job)
	defer func(start time.Time) {
		logger.Debugf("start job cost: %d ms", time.Since(start).Milliseconds())
	}(time.Now())

	// (并发)调用分配给该任务的节点的 Agent 的 StartJob 接口
	hosts := job.GetHosts()
	rsps := make([]*agent.StartJobResponse, len(hosts))
	errs := make([]error, len(hosts))

	wg := sync.WaitGroup{}
	for idx, host := range hosts {
		wg.Add(1)
		go func(i int, host string) {
			defer func() {
				wg.Done()
			}()
			rsp, err := x.opt.AgentClient.StartJob(ctx, host, &agent.StartJobRequest{
				RequestId: common.GetRequestId(ctx),
				JobId:     job.JobId,
				JobType:   job.JobType,
				Resources: job.Resources,
				Args:      job.Args,
				Hosts:     hosts,
				HostIndex: int32(i),
			})
			if err != nil {
				logger.WithError(err).Errorf("start job for %s fail", host)
			}
			errs[i] = err
			rsps[i] = rsp
		}(idx, host)
	}
	wg.Wait()
	retry = true
	var finalErr error = nil
	for _, err := range errs {
		if err != nil {
			finalErr = err
			if x.shouldRetryJob(job, err) {
				continue
			}
			// 不可重试的错误
			return nil, false, err
		}
	}
	for _, rsp := range rsps {
		if rsp.GetJobErr() != nil {
			return rsp.GetJobErr(), false, nil
		}
	}

	return nil, retry, finalErr
}

// 实现 Dispatcher.StopJob
func (x *LocalAdapter) StopJob(ctx context.Context, job *model.Job) (jobErr *types.Error, retry bool, err error) {
	logger := common.GetLogger(ctx)

	defer func(start time.Time) {
		logger.Debugf("stop job cost: %d ms", time.Since(start).Milliseconds())
	}(time.Now())

	// (并发)调用分配给该任务的节点的 Agent 的 StopJob 接口
	hosts := job.GetHosts()
	if len(hosts) == 0 {
		return nil, false, nil
	}
	rsps := make([]*agent.StopJobResponse, len(hosts))
	errs := make([]error, len(hosts))

	wg := sync.WaitGroup{}
	for i, host := range hosts {
		wg.Add(1)
		go func(i int, host string) {
			defer func() {
				wg.Done()
			}()
			rsp, err := x.opt.AgentClient.StopJob(ctx, host, &agent.StopJobRequest{
				RequestId: common.GetRequestId(ctx),
				JobId:     job.JobId,
				JobType:   job.JobType,
			})
			if err != nil {
				logger.WithError(err).Errorf("stop job for %s fail", host)
			}
			errs[i] = err
			rsps[i] = rsp
		}(i, host)
	}
	wg.Wait()
	retry = true
	var finalErr error = nil
	for _, err := range errs {
		if err != nil {
			finalErr = err
			if x.shouldRetryJob(job, err) {
				continue
			}
			// 不可重试的错误
			return nil, false, err
		}
	}
	for _, rsp := range rsps {
		if rsp.GetJobErr() != nil {
			return rsp.GetJobErr(), false, nil
		}
	}

	return nil, retry, finalErr
}

// 实现 Dispatcher.DescribeJob
func (x *LocalAdapter) DescribeJob(ctx context.Context, job *model.Job) (respJob *model.Job, retry bool, err error) {
	logger := common.GetLogger(ctx)

	defer func(start time.Time) {
		logger.Debugf("describe job cost: %d ms", time.Since(start).Milliseconds())
	}(time.Now())

	// (并发)调用分配给该任务的节点的 Agent 的 DescribeJob 接口
	hosts := job.GetHosts()
	rsps := make([]*agent.DescribeJobResponse, len(hosts))
	errs := make([]error, len(hosts))

	wg := sync.WaitGroup{}
	for i, host := range hosts {
		wg.Add(1)
		go func(i int, host string) {
			defer func() {
				wg.Done()
			}()
			rsp, err := x.opt.AgentClient.DescribeJob(ctx, host, &agent.DescribeJobRequest{
				RequestId: common.GetRequestId(ctx),
				JobId:     job.JobId,
				JobType:   job.JobType,
			})
			if err != nil {
				logger.WithError(err).Errorf("describe job for %s fail", host)
			}

			// bf, _ := json.Marshal(rsp)
			// logger.Debugf("describe job response: %v", string(bf))

			errs[i] = err
			rsps[i] = rsp
		}(i, host)
	}
	wg.Wait()
	retry = true
	var finalErr error = nil
	for _, err := range errs {
		if err != nil {
			finalErr = err
			if x.shouldRetryJob(job, err) {
				continue
			}
			// 不可重试的错误
			return nil, false, err
		}
	}
	if finalErr != nil {
		return nil, retry, finalErr
	}

	respJob = &model.Job{}
	respJob.GetJobLive().EndTime = rsps[0].GetDockerContainerState().GetFinishedAt() // 第一个节点作为代表

	// 依序检查:
	// 1. 若任意一个 host failed：则整体为 failed
	// 2. 若任意一个 host 未 ended：
	//    1）任意一个 host 处于如下状态，则整体处于该状态。优先级从高到低
	//       canceling, submiting, stopping, waiting, restarting, starting, preparing，running
	// 3. 若全部 host 都已经 ended：
	//    1）若任意一个 host 处于 canceled，则整体为 canceled
	//    2）其他情况，即全部 host 状态都处于 success, failed, canceled，则整体处于该状态
	processingStatuses := []types.JobStatus{
		types.JobStatus_JOB_STATUS_CANCELLING,
		types.JobStatus_JOB_STATUS_SUBMITTING,
		types.JobStatus_JOB_STATUS_STOPPING,
		types.JobStatus_JOB_STATUS_WAITING,
		types.JobStatus_JOB_STATUS_RESTARTING,
		types.JobStatus_JOB_STATUS_STARTING,
		types.JobStatus_JOB_STATUS_PREPARING,
		types.JobStatus_JOB_STATUS_RUNNING,
	}
	endedStatuses := []types.JobStatus{
		types.JobStatus_JOB_STATUS_FAILED,
		types.JobStatus_JOB_STATUS_CANCELED,
		types.JobStatus_JOB_STATUS_SUCCESS,
	}

	anyFailed := false
	processingJobStatus := types.JobStatus_JOB_STATUS_UNKNOWN
	endedJobStatus := types.JobStatus_JOB_STATUS_UNKNOWN
	var jobErr *types.Error = nil
	for _, rsp := range rsps {
		status := rsp.GetJobStatus()
		if !types.IsEnded(status) {
			for _, processingStatus := range processingStatuses {
				if status == processingStatus {
					processingJobStatus = status
				}
			}
		} else {
			for _, endedStatus := range endedStatuses {
				if status == endedStatus {
					endedJobStatus = status
				}
			}
		}
		if status == types.JobStatus_JOB_STATUS_FAILED {
			anyFailed = true
		}
		if jobErr == nil {
			if rsp.GetJobErr() != nil {
				// 优先使用业务错误
				jobErr = rsp.GetJobErr()
			} else {
				jobErr = rsp.GetError()
			}
		}
		respJob.Resources = append(respJob.Resources, rsp.GetResources()...)
	}

	if anyFailed {
		respJob.JobStatus = types.JobStatus_JOB_STATUS_FAILED
	} else if processingJobStatus != types.JobStatus_JOB_STATUS_UNKNOWN {
		respJob.JobStatus = processingJobStatus
	} else {
		respJob.JobStatus = endedJobStatus
	}
	respJob.GetJobLive().Error = jobErr

	return respJob, false, nil
}

// 节点心跳，获取节点状态
func (x *LocalAdapter) DescribeNode(ctx context.Context, ip string) (*types.Node, error) {
	logger := common.GetLogger(ctx)

	defer func(start time.Time) {
		logger.Debugf("describe node cost: %d ms", time.Since(start).Milliseconds())
	}(time.Now())

	resp, err := x.opt.AgentClient.DescribeNode(ctx, ip, &agent.DescribeNodeRequest{
		RequestId:  common.GetRequestId(ctx),
		Path:       "", // Agent 的数据目录可能与 Master 不一致
		RelDataDir: true,
	})
	if err != nil {
		logger.WithError(err).Errorln("describe node")
		return nil, err
	}

	return resp.GetNode(), nil
}

// 获取任务的调度标签值
func GetJobScheduleLabel(job *model.Job, key string) (value string) {
	label := job.GetResourceRequire().GetLabel()
	if label == nil {
		return ""
	}
	return label[key]
}

// 是否应该重试该任务
func (x *LocalAdapter) shouldRetryJob(job *model.Job, err error) bool {
	if !isNetworkError(err) {
		// 非网络错误不重试
		return false
	}
	if x.opt.AgentTimeout.Milliseconds() < 1 {
		// AgentTimeout 无效，不重试
		return false
	}
	if time.Since(x.cache.GetOrSetJobFirstRetryTime(job)) > x.opt.AgentTimeout {
		// 距离任务心跳失败超过 AgentTimeout，不重试
		return false
	}
	// Agent离线，且离线时间未超时，待重试
	return true
}

// 是否为网络错误
func isNetworkError(err error) bool {
	return common.Match(err, code.Code_InternalError__AgentUnavailable) ||
		common.Match(err, code.Code_InternalError__RPCTimeout) ||
		common.Match(err, code.Code_InternalError__RPC)
}

// local场景当前用来判断启动类型的状态是否达到限频
func (x *LocalAdapter) IsReadyByStatus(
	ctx context.Context, status types.JobStatus, job *model.Job, gather *model.Gather) bool {
	logger := common.GetLogger(ctx)

	if status == types.JobStatus_JOB_STATUS_WAITING || status == types.JobStatus_JOB_STATUS_RESTARTING {
		if gather.ProjectFunctionJobLimiting == nil || len(gather.ProjectFunctionJobLimiting) == 0 {
			return true
		}
		// 判断项目下某一个任务类型是否达到该类型的限制
		if gather.ProjectFunctionJobCounting[job.ProjectId][job.JobType] >= gather.ProjectFunctionJobLimiting[job.JobType] {
			logger.Debugf("reach project limit, job: %s, status: %s", job.JobId, job.JobStatus.String())
			return false
		}
		if _, ok := gather.ProjectFunctionJobCounting[job.ProjectId]; !ok {
			gather.ProjectFunctionJobCounting[job.ProjectId] = make(map[int32]int)
		}
		gather.ProjectFunctionJobCounting[job.ProjectId][job.JobType]++
	}

	return true
}

func (x *LocalAdapter) TryUpdateJobResource(ctx context.Context, job *model.Job) bool {
	return true
}
