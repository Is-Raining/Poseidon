package taiji

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	pb_taiji "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/taiji"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	agent_model "git.woa.com/yt-industry-ai/poseidon/internal/agent/model"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/http"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
)

// 太极 API 文档：https://doc.weixin.qq.com/doc/w3_m_csFraPCgGnBs
// 太极错误码文档：https://doc.weixin.qq.com/doc/w3_m_cVHPOyfKkkEE

const (
	// TODO(chuangchen): 太极文档错误码似乎有变动
	TaijiApiDomain = "taijiapi.woa.com" // 太极 API 域名
)

const (
	TaijiApiErrorTaskAlreadyExist = 100003
	TaijiApiErrorTaskNotExist     = 100004
	TaijiApiErrorResourceNotExist = 200002
)

// 实现 ExternalHandler
type TaijiHandler struct {
	opt    *Option      // 太极适配器参数
	client *http.Client // 用于调用太极 HTTP 接口
}

// StartJob
// 对应太极接口 [创建任务接口 + 启动任务接口]
func (x *TaijiHandler) StartJob(ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob) (
	externalJobId string, retry bool, err error) {
	logger := common.GetLogger(ctx)

	logger.Debugf("start create taiji task")

	// 根据任务类型获取参数转换器，将 job 转换为太极任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return "", false, common.Errorf(ctx, code.Code_InvalidParameter__TaijiParserNotExist)
	}
	info, err := parser.GetTaijiTaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to taiji task info fail")
		return "", false, err
	}

	// 检查参数
	if len(info.StartCmd) == 0 {
		info.StartCmd = strings.Join(containerJob.JobCtx.Config.Cmd, " ")
	}
	if len(info.ImageName) == 0 {
		info.ImageName = containerJob.JobCtx.Config.ImageName
	}
	if info.EnvVarsDict == nil {
		info.EnvVarsDict = map[string]string{}
	}

	// 添加 Container.Prepare 注入的环境变量
	for k, v := range containerJob.JobCtx.Config.Env {
		info.EnvVarsDict[k] = v
	}

	// 公共环境变量设置
	info.EnvVarsDict[types_.EnvProjId] = common.GetProjectId(ctx)
	info.EnvVarsDict[types_.EnvUin] = common.GetUin(ctx)
	info.EnvVarsDict[types_.EnvLanguage] = common.GetLanguage(ctx)
	info.EnvVarsDict[types_.EnvJobId] = containerJob.JobId()
	info.EnvVarsDict[types_.EnvJobDir] = containerJob.JobDir()
	info.EnvVarsDict[types_.EnvLogDir] = containerJob.LogDir()
	info.EnvVarsDict[types_.EnvReporterIpPort] = ""

	headers := map[string]string{"content-type": "application/json", "IPLUS-TASK-SERVER-API-TOKEN": info.ApiToken}

	// 固定参数配置
	datasetId := info.DatasetId
	if datasetId == "" {
		datasetId = x.opt.FixedDatasetId
	}

	modelId := info.CodePkgId
	if modelId == "" {
		modelId = x.opt.FixedCodePkgId
	}

	// 任务名称
	taskName := info.TaskName + "-" + fmt.Sprint(time.Now().Unix())

	// 创建任务
	url := getTaijiTaskApiUrl("training_task/create/")
	createReq := &pb_taiji.CreateTaijiTaskRequest{
		Id:      utils.NewUID(),
		Jsonrpc: "2.0",
		Method:  "TASK_CREATE",
		Params: &pb_taiji.CreateTaijiTaskRequestParam{
			ReqModuleId: "2",
			Event:       "TASK_CREATE",
			Task: &pb_taiji.CreateTaijiTaskRequestParamTask{
				ProjectId: "",                 // 当传空时，默认为用户创建 `<rtx>的API工程`
				TaskType:  "general_gpu_type", // 对于 TI 工具，固定为 general_gpu_type
				Common: &pb_taiji.CreateTaijiTaskRequestParamTaskCommon{
					ReadableName: taskName,
					TemplateFlag: "kubeflow_job_learning", // 对于 TI 工具，固定为 kubeflow_job_learning
					TaskFlag:     info.TaskId,             // 给将要创建的任务指定一个 task id
					DatasetId:    datasetId,
					ModelId:      modelId,
					BusinessFlag: info.AppGroupName,
					RunningType:  "manual", // 对于 TI 工具，固定为 manual
					Permission: &pb_taiji.CreateTaijiTaskRequestParamTaskPermission{
						AlertGroup: info.AlertUser,
					},
				},
				TaskConfig: &pb_taiji.CreateTaijiTaskRequestParamTaskConfig{
					EnvVarsDict: info.EnvVarsDict,
					JobType:     "",
					JobConfig: &pb_taiji.CreateTaijiTaskRequestParamTaskConfigJobConfig{
						StartCmd:              info.StartCmd,
						ExecStartInAllMpiPods: false,
					},
					DesignatedResource: &pb_taiji.CreateTaijiTaskRequestParamTaskConfigResource{
						HostNum:                       info.GetHostNum(),
						HostGpuNum:                    info.GetHostGpuNum(),
						ImageFullName:                 info.GetImageName(),
						GPUName:                       info.GetGpuType(),
						CudaVersion:                   info.GetCudaVersion(),
						IsElasticity:                  info.IsElasticity,
						IsEnableRdma:                  info.GetEnableRdma(),
						IsResourceWaiting:             true,
						IsEnableSshWithoutPassword:    false,
						KeepRunningAfterTrainerFinish: true,
					},
				},
			},
		},
	}

	if info.GetEnableMpi() {
		createReq.Params.Task.TaskConfig.JobType = "mpijob"
		createReq.Params.Task.TaskConfig.JobConfig.ExecStartInAllMpiPods = true
		createReq.Params.Task.TaskConfig.DesignatedResource.IsEnableSshWithoutPassword = true
	}
	if info.GetEnableDebug() {
		createReq.Params.Task.TaskConfig.DesignatedResource.KeepRunningAfterTrainerFinish = true
	}

	// 调用太极创建任务 API
	createRsp := &pb_taiji.CreateTaijiTaskResponse{}
	body, httpCode, err := x.client.JSONPost(ctx, url, headers, createReq, createRsp)
	logger.Debugf("taiji response of create job: %s", string(body))

	// 解析错误
	err = parseCallTaijiError(
		ctx, url, err, httpCode, createRsp.GetResult().GetCode(), createRsp.GetResult().GetMessage(),
		TaijiApiErrorTaskAlreadyExist)
	if err != nil {
		return "", true, err
	}

	// 启动任务
	url = getTaijiTaskApiUrl("training_task/enable/")
	startReq := &pb_taiji.StartTaijiTaskRequest{
		Id:      utils.NewUID(),
		Jsonrpc: "2.0",
		Method:  "TASK_ENABLE",
		Params: &pb_taiji.StartTaijiTaskRequestParam{
			ReqModuleId: "YG_00000000000000000000000000000000_00", // 固定值
			OneOff:      true,                                     // 固定为 true
			Event:       "TASK_ENABLE",
			TaskFlag:    info.TaskId,
		},
	}

	// 调用太极启动任务 API
	startRsp := &pb_taiji.StartTaijiTaskResponse{}
	body, httpCode, err = x.client.JSONPost(ctx, url, headers, startReq, startRsp)
	logger.Debugf("taiji response of start job: %s", string(body))

	// parse error
	err = parseCallTaijiError(
		ctx, url, err, httpCode, startRsp.GetResult().GetCode(), startRsp.GetResult().GetMessage())
	if err != nil {
		return "", true, err
	}

	logger.Debugf("end create taiji task")
	return info.TaskId, false, nil
}

// DescribeJob
// 对应太极接口 [任务详情查询接口]
// READY 就绪
// TRAINING_RESOURCE_QUERYING  申请训练资源
// TRAINING_TASK_SUBMITTING 训练任务启动
// TRAINING_RUNNING 训练任务运行中
// TRAINING_RESOURCE_WAITING 进入资源等待队列
// RESOURCE_WAIT_TRANSITION 移出资源等待队列
// TRAINING_DEBUG 可调试状态
// START_FAILED 启动失败
// RUNNING_FAILED 运行失败
// RUNNING_SUCCESS 训练完成
// JOB_DELETE 资源回收中（新架构模板才有的状态）
// TRAINING_INIT （ 新架构模板才有的状态）
// PENDING （新架构模板才有的状态）
// END 结束
func (x *TaijiHandler) DescribeJob(ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob) (
	describeJob *model.Job, retry bool, err error) {
	logger := common.GetLogger(ctx)

	// 根据任务类型获取参数转换器，将 job 转换为太极任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return nil, false, common.Errorf(ctx, code.Code_InvalidParameter__TaijiParserNotExist)
	}
	info, err := parser.GetTaijiTaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to taiji task info fail")
		return nil, false, err
	}

	headers := map[string]string{"content-type": "application/json", "IPLUS-TASK-SERVER-API-TOKEN": info.ApiToken}

	// 获取任务实例列表
	url := getTaijiTaskApiUrl("task_instance/list/")
	listReq := &pb_taiji.DescribeTaijiTaskRequest{
		Id:      utils.NewUID(),
		Jsonrpc: "2.0",
		Method:  "INSTANCE_LIST",
		Params: &pb_taiji.DescribeTaijiTaskRequestParam{
			TaskFlag:  info.TaskId, // 创建任务时可以指定 taskid, taskflag 和 job 中的 taskid 完全一致
			IsSuccess: false,       // 是否只查看成功的实例，固定为 false
		},
	}

	// 调用太极查询任务实例 API
	listRsp := &pb_taiji.DescribeTaijiTaskResponse{}
	body, httpCode, err := x.client.JSONPost(ctx, url, headers, listReq, listRsp)
	logger.Debugf("taiji response of describe job: %s", string(body))

	// 解析错误
	err = parseCallTaijiError(ctx, url, err, httpCode, listRsp.GetResult().GetCode(), listRsp.GetResult().GetMessage(),
		TaijiApiErrorTaskNotExist)
	if err != nil {
		return nil, true, err
	}

	// 任务不存在
	listRspCode := listRsp.GetResult().GetCode()
	if listRspCode == TaijiApiErrorTaskNotExist {
		logger.Warnf("taiji task not exist")
		return nil, false, common.Errorf(ctx, code.Code_ResourceNotFound__JobNotExist)
	}

	// 取最新的任务实例作为当前实例
	instanceInfos := listRsp.GetResult().GetData().GetInstanceInfo()
	if len(instanceInfos) < 1 {
		logger.WithError(err).Errorf("instance number of task < 1")
		return nil, false, common.Errorf(ctx, code.Code_InternalError__TaijiResponseMalformed)
	}
	taskInfo := instanceInfos[0]
	createTime := time.Now()
	timeZone, _ := time.LoadLocation("Asia/Shanghai")
	for i, tmpTaskInfo := range instanceInfos {
		tmpCreateTime, err := time.ParseInLocation("2006-01-02 15:04:05", tmpTaskInfo.GetCreateTime(), timeZone)
		if err != nil {
			logger.WithError(err).Errorf("parse taiji time string fail")
			return nil, false, common.Errorf(ctx, code.Code_InternalError__TaijiResponseMalformed)
		}
		if i == 0 || tmpCreateTime.Unix() > createTime.Unix() {
			taskInfo = tmpTaskInfo
			createTime = tmpCreateTime
		}
	}
	instanceId := taskInfo.GetInstanceId()

	// 查询任务实例详细信息
	url = getTaijiTaskApiUrl("task_instance/detail/")
	detailReq := &pb_taiji.DescribeTaijiTaskDetailRequest{
		Id:      utils.NewUID(),
		Jsonrpc: "2.0",
		Method:  "INSTANCE_DETAIL",
		Params: &pb_taiji.DescribeTaijiTaskDetailRequestParam{
			TaskType:   "general_gpu_type",
			InstanceId: instanceId,
		},
	}
	detailRsp := &pb_taiji.DescribeTaijiTaskDetailResponse{}
	body, httpCode, err = x.client.JSONPost(ctx, url, headers, detailReq, detailRsp)

	logger.Debugf("taiji response of describe job instance: %s", string(body))

	// 解析错误
	err = parseCallTaijiError(
		ctx, url, err, httpCode, detailRsp.GetResult().GetCode(), detailRsp.GetResult().GetMessage())
	if err != nil {
		return nil, true, err
	}

	// 转换元数据
	endTime := time.Unix(createTime.Unix()+int64(taskInfo.GetTimeDelay()), 0)
	info.State = taskInfo.GetState()
	info.Msg = fmt.Sprintf("Task: %s Instance: %s", taskInfo.GetMsg(),
		detailRsp.GetResult().GetData().GetCommon().GetMsg())
	info.IsSuccess = taskInfo.GetIsSuccess()
	info.CreateTime = utils.FormatTime(createTime)
	info.EndTime = utils.FormatTime(endTime)

	respJob := &model.Job{}
	respJob.GetJobLive().StartTime = info.CreateTime
	respJob.GetJobLive().EndTime = info.EndTime

	// 转换状态
	switch info.State {
	case "END":
		if info.IsSuccess {
			respJob.JobStatus = types.JobStatus_JOB_STATUS_SUCCESS
		} else {
			// 目前太极提供的接口无法判断是 CANCELED or FAILED，所以只能统一为 FAILED
			respJob.JobStatus = types.JobStatus_JOB_STATUS_FAILED
			respJob.GetJobLive().Error = common.ErrorfProto(ctx, code.Code_InternalError__TaijiTaskFailed, info.Msg)
		}
	case "READY",
		"TRAINING_RESOURCE_QUERYING",
		"TRAINING_INIT",
		"PENDING",
		"TRAINING_RESOURCE_WAITING",
		"RESOURCE_WAIT_TRANSITION":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_PREPARING
	case "TRAINING_TASK_SUBMITTING",
		"TRAINING_RUNNING",
		"TRAINING_DEBUG",
		"START_FAILED",
		"RUNNING_FAILED",
		"RUNNING_SUCCESS":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_RUNNING
	case "JOB_DELETE":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_RUNNING
	default:
		logger.Debugf("ignore taiji task state %s", info.State)
	}

	logger.Debugf("taiji %s state %s, job status %s", job.JobId, info.State, respJob.JobStatus.String())

	return respJob, false, nil
}

// StopJob
// 对应太极接口 [停止任务接口]
func (x *TaijiHandler) StopJob(ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob) (retry bool, err error) {
	logger := common.GetLogger(ctx)

	logger.Debugf("start kill taiji task")

	// 根据任务类型获取参数转换器，将 job 转换为太极任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return false, common.Errorf(ctx, code.Code_InvalidParameter__TaijiParserNotExist)
	}
	info, err := parser.GetTaijiTaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to taiji task info fail")
		return false, err
	}

	headers := map[string]string{"content-type": "application/json", "IPLUS-TASK-SERVER-API-TOKEN": info.ApiToken}

	// 停止任务
	url := getTaijiTaskApiUrl("training_task/kill/")
	stopReq := &pb_taiji.StopTaijiTaskRequest{
		Id:      utils.NewUID(),
		Jsonrpc: "2.0",
		Method:  "INSTANCE_KILL",
		Params: &pb_taiji.StopTaijiTaskRequestParam{
			TaskFlag: info.TaskId, // 创建任务时可以指定 task id, task info 和 job 中的 task id 完全一致``
		},
	}
	stopRsp := &pb_taiji.StopTaijiTaskResponse{}
	body, httpCode, err := x.client.JSONPost(ctx, url, headers, stopReq, stopRsp)
	logger.Debugf("taiji response of stop job detail: %s", string(body))

	// 解析错误
	err = parseCallTaijiError(
		ctx, url, err, httpCode, stopRsp.GetResult().GetCode(), stopRsp.GetResult().GetMessage(),
		TaijiApiErrorTaskNotExist, TaijiApiErrorResourceNotExist)
	if err != nil {
		return false, err
	}

	// 任务不存在，则不需要再查询确认
	stopRspCode := stopRsp.GetResult().GetCode()
	if stopRspCode == TaijiApiErrorTaskNotExist || stopRspCode == TaijiApiErrorResourceNotExist {
		logger.Warnf("taiji task not exist, stop succeeds")
		return false, nil
	}

	// 查询状态
	respJob, retry, err := x.DescribeJob(ctx, job, containerJob)
	if err != nil {
		logger.WithError(err).Errorf("get taiji task fail")
		return retry, err
	}
	if !types.IsEnded(respJob.JobStatus) {
		// 任务仍未停止，返回失败并重试
		logger.Debugf("taiji task is ended")
		return true, common.Errorf(ctx, code.Code_InternalError__TaijiTaskNotEnded)
	}
	// 任务已经停止，返回成功
	return false, nil
}

// 解析太极错误，错误码为 acceptRspCodes 其一的将被忽略
func parseCallTaijiError(ctx context.Context, url string, err error, httpCode int, rspCode int32, rspMsg string,
	acceptRspCodes ...int32) error {
	logger := common.GetLogger(ctx)
	if err != nil {
		logger.WithError(err).Errorf("call taiji service failed. http_code: %d", httpCode)
		if errors.Is(err, context.DeadlineExceeded) {
			// 超时
			return common.Errorf(ctx, code.Code_InternalError__TaijiTimeout)
		}
		// 调用 HTTP 错误
		return common.Errorf(ctx, code.Code_InternalError__TaijiHttpFailed, httpCode)
	}
	if rspCode != 0 {
		// 业务错误
		for _, acceptRspCode := range acceptRspCodes {
			if acceptRspCode == rspCode {
				return nil
			}
		}
		msg := fmt.Sprintf("taiji API return errorcode: %d, errormsg: %s", rspCode, rspMsg)
		logger.WithError(err).Errorf(msg)
		return common.Errorf(ctx, code.Code_InternalError__TaijiFailed, msg)
	}
	return nil
}

func getTaijiTaskApiUrl(sub string) string {
	return fmt.Sprintf("http://%s/taskmanagement/task_server/task_management/api/%s", TaijiApiDomain, sub)
}
