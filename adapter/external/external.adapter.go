package external

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"git.woa.com/yt-industry-ai/grpc-go-ext/log"
	"git.woa.com/yt-industry-ai/poseidon/adapter"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	agent_model "git.woa.com/yt-industry-ai/poseidon/internal/agent/model"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/dao"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/docker"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
	"github.com/jmoiron/sqlx"
)

const (
	// 任务输出到任务临时目录下该文件的内容，将会被自动收集
	// 前提条件：调度器能够访问任务的临时目录（例如通过共享存储）
	JobReporterLogFileName = "local_reporter.txt"
)

// 外部集群适配器实现该接口
// 注意所有接口方法均必须为无状态，也不要把依赖 containerJob 的状态，因为调度器可能重启，会导致状态丢失
type ExternalHandler interface {
	// 将 job 任务投递到外部集群
	// Args:
	//  job: 任务元信息
	//  containerJob: 容器任务的运行实例
	// Return:
	//  externalJobId: 任务在外部集群中的 id
	//  retry: 如果投递失败，是否重试，若为 false，则直接将任务设置为失败，失败原因是 err
	//  err: 投递失败原因
	StartJob(ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob) (
		externalJobId string, retry bool, err error)

	// 向外部集群查询 job 任务状态
	// Args:
	//  job: 任务元信息
	//  containerJob: 容器任务的运行实例
	// Return:
	//  describeJob: 外部集群返回的 job 元信息
	//  retry: 如果查询失败，是否重试，若为 false，则直接将任务设置为失败，失败原因是 err
	//  err: 查询失败原因
	DescribeJob(ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob) (
		describeJob *model.Job, retry bool, err error)

	// 调用外部集群接口停止 job 任务
	// Args:
	//  job: 任务元信息
	//  containerJob: 容器任务的运行实例
	// Return:
	//  retry: 如果停止失败，是否重试，若为 false，则直接将任务设置为失败，失败原因是 err
	//  err: 停止失败原因
	StopJob(ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob) (retry bool, err error)
}

// 外部集群适配器基础配置，通常由适配器实现封装后再给使用者
type ExternalAdapterOption struct {
	DB              *sqlx.DB              // 数据库实例
	DBTimeout       time.Duration         // 数据库操作超时时间
	JobDir          string                // job 根目录
	LogDir          string                // job 日志根目录
	JobTypes        []int32               // 负责处理的任务类型
	AdapterTypes    []string              // 负责处理指定适配器类型的任务
	ErrorMarshaler  types_.ErrorMarshaler // 错误码转换接口
	CheckPeriod     time.Duration         // 检查周期
	ContainerEnvOpt *ContainerEnv         // 容器镜像的依赖环境变量
}

// 容器环境变量参数
type ContainerEnv struct {
	ReporterIpPort       string // 进度上报地址
	EnableLicense        bool   // 是否启用
	LicensePlatformPath  string // license 本地路径
	LicenseContainerPath string // license 容器内路径
	JobPlatform          string // 任务调度平台 : LOCAL / TAIJI / TIMAKER
}

// 实现外部集群的基本流程
type ExternalAdapter struct {
	adapter.UnimplementedDispatcherAdapter // 实现调度适配器接口

	opt             *ExternalAdapterOption                                // 配置
	masterCtl       types_.MasterController                               // Master接口
	containers      *agent_model.Cache[string, *agent_model.ContainerJob] // 容器任务运行实例缓存
	externalHandler ExternalHandler                                       // 外部适配器接口实现
	jobDao          *dao.JobDao                                           // job 数据存储
}

// 创建 ExternalAdapter
func NewExternalAdapter(externalHandler ExternalHandler, opt *ExternalAdapterOption) *ExternalAdapter {
	return &ExternalAdapter{
		opt:             opt,
		jobDao:          dao.NewJobDao(opt.DB, nil),
		containers:      &agent_model.Cache[string, *agent_model.ContainerJob]{},
		externalHandler: externalHandler,
	}
}

// 注入 Master 控制逻辑
func (x *ExternalAdapter) SetMasterController(masterCtl types_.MasterController) {
	x.masterCtl = masterCtl
}

// 创建容器任务运行实例
func (x *ExternalAdapter) newContainerJob(
	ctx context.Context, job *model.Job, handler types_.Container,
) *agent_model.ContainerJob {
	containerJob := agent_model.NewContainerJob()

	containerJob.Ctx, containerJob.Cancel = context.WithCancel(common.NewContext(ctx))
	containerJob.WaitGroup = utils.NewWaitGroup()
	containerJob.Handler = handler

	containerJob.JobCtx.JobId = job.JobId
	containerJob.JobCtx.JobType = job.JobType
	containerJob.JobCtx.Args = job.Args
	containerJob.JobCtx.AgentAddr = "" // no agent
	containerJob.JobCtx.Resources = job.Resources
	containerJob.JobCtx.SetJobDir(x.opt.JobDir)
	containerJob.JobCtx.SetJobLogDir(x.opt.LogDir)

	containerJob.SetStatus(types.JobStatus_JOB_STATUS_PREPARING)

	// 自动收集日志
	containerJob.JobCtx.LogReporter = types_.NewJobLogReporter(&types_.JobLogReporterOption{
		LogPath:     path.Join(containerJob.JobDir(), JobReporterLogFileName),
		JobReporter: x.masterCtl,
	})

	return containerJob
}

// 周期性调用 Container.Check 进行（业务层面的）检查
func (x *ExternalAdapter) checkJob(ctx context.Context, containerJob *agent_model.ContainerJob) {
	logger := common.GetLogger(ctx)

	logger.Debugf("check container job")
	if jobErr := containerJob.HandlerCaller().Check(ctx, containerJob.JobCtx); jobErr != nil {
		logger.WithError(jobErr).Errorf("check fail")
		// 检查失败，将任务设置为失败
		containerJob.SetFailed(jobErr, nil)
	}

	logger.Debugf("check local reporter")
	if jobErr := containerJob.JobCtx.LogReporter.ScanReport(ctx); jobErr != nil {
		// 上报错误，仅输出日志，不将任务设置为失败
		logger.WithError(jobErr).Errorf("report fail")
	}
}

// 收集应由该适配器处理的 Jobs
func (x *ExternalAdapter) Gather(ctx context.Context) (gather *model.Gather, err error) {
	ctx, cancel := context.WithTimeout(ctx, x.opt.DBTimeout)
	defer cancel()

	logger := common.GetLogger(ctx)
	gather = &model.Gather{
		ProjectQuota:               map[string]map[string]int{},
		ProjectCounting:            map[string]map[string]int{},
		FunctionJobLimiting:        map[int32]int{},
		FunctionJobCounting:        map[string]map[int32]int{},
		ProjectFunctionJobLimiting: map[int32]int{},
		ProjectFunctionJobCounting: map[string]map[int32]int{},
	}

	// 从任务存储层读取指定任务和适配器类型的 job
	inConds := []string{model.ColJobJobStatus, model.ColJobAdapterType}
	if len(x.opt.JobTypes) > 0 {
		inConds = append(inConds, model.ColJobJobType)
	}
	gather.Jobs, err = x.jobDao.GetJobs(ctx, nil, inConds, nil,
		map[string]interface{}{
			model.ColJobJobStatus:   types.GetProcessingJobStatuses().EnumInt32s(),
			model.ColJobJobType:     x.opt.JobTypes,
			model.ColJobAdapterType: x.opt.AdapterTypes,
		},
	)
	if err != nil {
		logger.WithError(err).Errorf("get processing jobs fail. adapterTypes: %v, jobTypes: %v",
			x.opt.AdapterTypes, x.opt.JobTypes)
		return nil, err
	}
	sort.Sort(gather.Jobs) // 按照默认方法排序

	return gather, nil
}

// 调用钩子：Container.Initialize/Prepare，最终调用 ExternalHandler.StartJob 创建并启动外部任务
func (x *ExternalAdapter) StartJob(ctx context.Context, job *model.Job) (
	jobErr *types.Error, retry bool, err error) {
	logger := common.GetLogger(ctx)
	logger.Debugf("ExternalAdapter start job: %+v", job)

	// 创建 Container 接口实例
	factory, ok := types_.GetContainer(job.JobType)
	if !ok {
		logger.WithError(err).Errorf("setup handler fail")
		return nil, false, common.Errorf(ctx, code.Code_InvalidParameter__JobTypeNotRegistered)
	}

	// 创建容器任务运行实例
	containerJob := x.newContainerJob(ctx, job, factory())
	if _, ok := x.containers.LoadOrStore(job.JobId, containerJob); ok {
		// 原子操作：将容器任务实例放入缓存
		// 若缓存中已经有该任务运行实例了，直接返回
		return nil, false, nil
	}

	// 创建任务日志目录、任务临时目录，将日志目录软链到临时目录的 log 目录
	if err := utils.MkdirAll(containerJob.LogDir()); err != nil {
		logger.WithError(err).Errorln("create job log dir")
		return nil, false, common.Errorf(ctx, code.Code_InternalError__MakeDirFailed)
	}
	if err := utils.MkdirAll(containerJob.JobDir()); err != nil {
		logger.WithError(err).Errorln("create job dir")
		return nil, false, common.Errorf(ctx, code.Code_InternalError__MakeDirFailed)
	}
	logDirPath := filepath.Join(containerJob.JobDir(), "log")
	if !utils.PathExist(logDirPath) {
		if err := utils.MkSymlink(containerJob.LogDir(), logDirPath); err != nil {
			logger.WithError(err).Errorln("create job log symlink fail")
			return nil, false, common.Errorf(ctx, code.Code_InternalError__MakeSymlinkFailed)
		}
	}

	// 钩子：Container.Initialize，出错不重试
	if jobErr := containerJob.HandlerCaller().Initialize(ctx, containerJob.JobCtx); jobErr != nil {
		logger.WithError(jobErr).Errorf("initialize fail")
		pbErr := types_.ToErrorProto(ctx, x.opt.ErrorMarshaler, jobErr, code.Code_InternalError__InitializeJob)
		return pbErr, false, nil
	}

	// 钩子：Container.Prepare，出错不重试
	if jobErr := containerJob.HandlerCaller().Prepare(ctx, containerJob.JobCtx); jobErr != nil {
		logger.WithError(jobErr).Errorf("prepare fail")
		pbErr := types_.ToErrorProto(ctx, x.opt.ErrorMarshaler, jobErr, code.Code_InternalError__PrepareJob)
		return pbErr, false, nil
	}

	// 容器公共环境变量准备
	logger.Infof("prepare container job")
	if jobErr, err := x.prepareContainerJob(ctx, containerJob, job); jobErr != nil || err != nil {
		logger.WithError(err).Errorln("prepare fail. job_err: %v", jobErr)
		containerJob.SetFailed(jobErr, err)
		return nil, false, common.Errorf(ctx, code.Code_InternalError__PrepareJob)
	}

	// 投递任务到外部
	externalJobId, retry, err := x.externalHandler.StartJob(ctx, job, containerJob)
	if err != nil {
		if retry {
			// 开启任务失败且需要重试时, 从缓存中移除该 job
			x.containers.Delete(job.JobId)
		}
		logger.WithError(err).Errorf("start job fail")
		return nil, retry, err
	}
	job.ExternalJobId = externalJobId
	logger.Debugf("get external job %s", externalJobId)

	// 持久化创建外部集群任务后得到的 job id
	if err := x.jobDao.UpdateJob(ctx,
		[]string{model.ColJobExternalJobId, model.ColJobExternalJobDir},
		[]string{model.ColJobJobId},
		&model.Job{
			JobId:          job.JobId,
			ExternalJobId:  externalJobId,
			ExternalJobDir: containerJob.JobCtx.JobDir,
		},
	); err != nil {
		logger.WithError(err).Errorln("update job external_job_id fail")
		return nil, false, err
	}

	// 启动检查线程
	containerJob.WaitGroup.AddFunc(x.opt.CheckPeriod, func() {
		x.checkJob(ctx, containerJob)
	})
	return nil, false, nil
}

func (x *ExternalAdapter) prepareContainerJob(ctx context.Context, containerJob *agent_model.ContainerJob, job *model.Job) (jobErr error, err error) {
	if nil == x.opt.ContainerEnvOpt {
		return nil, nil
	}
	logger := log.WithField("JobId", containerJob.JobId())
	cfg := containerJob.JobCtx.GetConfig()
	// 容器名称
	cfg.ContainerName = containerJob.JobId()
	// 公共环境变量
	if cfg.Env == nil {
		cfg.Env = map[string]string{}
	}
	cfg.Env[types_.EnvPlatform] = x.opt.ContainerEnvOpt.JobPlatform
	cfg.Env[types_.EnvProjId] = job.ProjectId
	cfg.Env[types_.EnvUin] = job.Creator
	cfg.Env[types_.EnvLanguage] = job.Language
	cfg.Env[types_.EnvJobId] = containerJob.JobId()
	cfg.Env[types_.EnvOCRJobId] = containerJob.JobId()
	cfg.Env[types_.EnvJobDir] = containerJob.JobDir()
	cfg.Env[types_.EnvLogDir] = containerJob.LogDir()
	cfg.Env[types_.EnvReporterIpPort] = x.opt.ContainerEnvOpt.ReporterIpPort
	// NVIDIA 相关环境变量
	gpus, err := containerJob.JobCtx.GetGpus()
	if err != nil {
		logger.WithError(err).Errorf("get gpus failed")
		return nil, common.Errorf(ctx, code.Code_InternalError__Unknown)
	}
	if len(gpus) > 0 { // 在入口处已经做了下标保护
		if x.opt.ContainerEnvOpt.JobPlatform != "TIMAKER" {
			cfg.Env[docker.EnvNvidiaVisibleDevices] = utils.Join(gpus, ",")
		}
		cfg.Env[docker.EnvNvidiaDriverCapabilities] = "video,compute,utility"
	}
	// 端口相关环境变量
	ports, err := containerJob.JobCtx.GetPorts()
	if err != nil {
		logger.WithError(err).Errorf("get ports failed")
		return nil, common.Errorf(ctx, code.Code_InternalError__Unknown)
	}
	if len(ports) > 0 { // 在入口处已经做了下标保护
		cfg.Env[types_.EnvPorts] = utils.Join(ports, ",")
	}

	// 多机相关环境变量
	hostGpus := []string{}
	for _, resource := range containerJob.JobCtx.Resources {
		if resource == nil {
			continue
		}
		if len(resource.Gpus) > 0 {
			for _, gpuId := range resource.Gpus {
				hostGpus = append(hostGpus, fmt.Sprintf("%s:%d", resource.Ip, gpuId))
			}
		} else {
			hostGpus = append(hostGpus, resource.Ip)
		}
	}
	cfg.Env[types_.EnvHostGpus] = strings.Join(hostGpus, ";")
	cfg.Env[types_.EnvHostIndex] = fmt.Sprintf("%d", containerJob.JobCtx.HostIndex)
	cfg.Env[types_.EnvHosts] = strings.Join(containerJob.JobCtx.Hosts, ";")
	// 容器标签
	if cfg.Labels == nil {
		cfg.Labels = map[string]string{}
	}
	cfg.Labels[types_.PoseidonLabel] = "Copyright © Tencent youtu poseidon. All Rights Reserved."

	// 公共挂载点
	if x.opt.ContainerEnvOpt.EnableLicense {
		// License 文件挂载
		licenseSourcePath := x.opt.ContainerEnvOpt.LicensePlatformPath
		if len(licenseSourcePath) == 0 {
			licenseSourcePath = types_.DefaultLicenseSourcePath
		}
		// if !utils.IsFile(licenseSourcePath) {
		// 	logger.Errorf("license %s not exist", licenseSourcePath)
		// 	return nil, common.Errorf(ctx, code.Code_InternalError__LicenseNotExist)
		// }
		licenseTargetPath := x.opt.ContainerEnvOpt.LicenseContainerPath
		if len(licenseTargetPath) == 0 {
			licenseTargetPath = types_.DefaultLicenseTargetPath
		}
		if cfg.ReadonlyMounts == nil {
			cfg.ReadonlyMounts = map[string]string{}
		}
		cfg.ReadonlyMounts[licenseSourcePath] = licenseTargetPath
		cfg.Env[types_.EnvLicense] = fmt.Sprintf("%s:%s", licenseSourcePath, licenseTargetPath)
	}
	// 挂载日志目录
	// if len(containerJob.LogDir()) > 0 {
	// 	if cfg.Mounts == nil {
	// 		cfg.Mounts = map[string]string{}
	// 	}
	// 	cfg.Mounts[containerJob.LogDir()] = containerJob.LogDir()
	// }

	// // 挂载临时目录
	// cfg.Mounts[containerJob.JobDir()] = containerJob.JobDir()
	mounts := make([]string, 0)
	for k, v := range cfg.Mounts {
		mounts = append(mounts, fmt.Sprintf("%s:%s", k, v))
	}
	cfg.Env["MOUNTS"] = strings.Join(mounts, ",")

	// 打印容器参数
	bf, _ := json.Marshal(cfg)
	logger.Debugf("container config: %s", string(bf))

	return nil, nil
}

// 调用钩子：Container.Initialize/Submit/Clean, 最终调用 ExternalHandler.StopJob 停止外部任务
func (x *ExternalAdapter) StopJob(ctx context.Context, job *model.Job) (
	jobErr *types.Error, retry bool, err error) {
	logger := common.GetLogger(ctx)

	defer func() {
		// TODO(chuangchen): 非重试情况下才删除缓存，避免重复创建运行实例
		x.containers.Delete(job.JobId)
	}()

	// 创建 Container 接口实例
	factory, ok := types_.GetContainer(job.JobType)
	if !ok {
		logger.WithError(err).Errorf("setup handler fail")
		return nil, false, common.Errorf(ctx, code.Code_InvalidParameter__JobTypeNotRegistered)
	}

	// 创建容器任务运行实例
	containerJob := x.newContainerJob(ctx, job, factory())
	containerJob, ok = x.containers.LoadOrStore(job.JobId, containerJob) // 不存在则使用新new出来的

	// 取消检查线程
	containerJob.WaitGroup.TerminatedAndWait()

	// 调用外部集群停止任务
	retry, err = x.externalHandler.StopJob(ctx, job, containerJob)
	if err != nil {
		logger.WithError(err).Errorf("stop fail")
		return nil, retry, err
	}

	if !ok {
		// 新创建的容器任务运行实例，需要调用钩子：Container.Initialize，出错不重试
		if jobErr := containerJob.HandlerCaller().Initialize(ctx, containerJob.JobCtx); jobErr != nil {
			logger.WithError(jobErr).Errorf("initialize fail")
			pbErr := types_.ToErrorProto(ctx, x.opt.ErrorMarshaler, jobErr, code.Code_InternalError__InitializeJob)
			return pbErr, false, nil
		}
	}

	// 从外部集群获取任务状态
	rspJob, retry, err := x.externalHandler.DescribeJob(ctx, job, containerJob)
	// 任务不存在
	if common.Match(err, code.Code_ResourceNotFound__JobNotExist) {
		logger.WithError(err).Warnf("external task not exist")
		return nil, false, nil
	}
	if err != nil {
		logger.WithError(err).Errorf("get task fail")
		return nil, retry, err
	}
	// 任务成功，调用钩子：Container.Submit
	if types.IsSuccess(rspJob.JobStatus) {
		if jobErr := containerJob.HandlerCaller().Submit(ctx, containerJob.JobCtx); jobErr != nil {
			logger.WithError(jobErr).Errorf("submit fail")
			pbErr := types_.ToErrorProto(ctx, x.opt.ErrorMarshaler, jobErr, code.Code_InternalError__SubmitJob)
			return pbErr, false, nil
		}
	}

	// 钩子：Container.Clean
	if jobErr := containerJob.HandlerCaller().Clean(ctx, containerJob.JobCtx); jobErr != nil {
		logger.WithError(jobErr).Errorf("clean fail")
		pbErr := types_.ToErrorProto(ctx, x.opt.ErrorMarshaler, jobErr, code.Code_InternalError__CleanJob)
		return pbErr, false, nil
	}

	return nil, false, nil
}

// Dispatcher 周期性调用，转发至：ExternalHandler.DescribeJob，收集 Job 运行情况
func (x *ExternalAdapter) DescribeJob(ctx context.Context, job *model.Job) (respJob *model.Job, retry bool, err error) {
	logger := common.GetLogger(ctx)

	// 创建 Container 接口实例
	factory, ok := types_.GetContainer(job.JobType)
	if !ok {
		logger.WithError(err).Errorf("setup handler fail")
		return nil, false, common.Errorf(ctx, code.Code_InvalidParameter__JobTypeNotRegistered)
	}

	// 创建容器任务运行实例
	containerJob := x.newContainerJob(ctx, job, factory())
	if containerJob, ok := x.containers.LoadOrStore(job.JobId, containerJob); ok {
		// 从缓存中读取检查结果（由检查线程注入）
		jobStatus := containerJob.GetStatus()
		logger.Debugf("job status is %s", jobStatus.String())

		if jobStatus == types.JobStatus_JOB_STATUS_FAILED {
			// Container.Check 返回错误，任务被设置为 failed
			respJob := &model.Job{}
			respJob.GetJobLive().Error =
				types_.ToErrorProto(ctx, x.opt.ErrorMarshaler, containerJob.JobErr, code.Code_InternalError__CheckJob)
			respJob.JobStatus = jobStatus
			return respJob, false, nil
		}
	} else {
		// Scheduler 重启，重新注入缓存，并启动检查线程
		if jobErr := containerJob.HandlerCaller().Initialize(ctx, containerJob.JobCtx); jobErr != nil {
			logger.WithError(err).Errorf("initialize fail")
			job.GetJobLive().Error =
				types_.ToErrorProto(ctx, x.opt.ErrorMarshaler, jobErr, code.Code_InternalError__InitializeJob)
			return job, false, nil
		}

		containerJob.WaitGroup.AddFunc(x.opt.CheckPeriod, func() {
			x.checkJob(ctx, containerJob)
		})
	}
	// 转发至 ExternalHandler.DescribeJob
	respJob, retry, err = x.externalHandler.DescribeJob(ctx, job, containerJob)
	return respJob, retry, err
}

// TryUpdateJobResource
func (x *ExternalAdapter) TryUpdateJobResource(ctx context.Context, job *model.Job) bool {

	return true
}
