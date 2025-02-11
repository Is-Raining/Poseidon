package timaker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	pb_timaker "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/timaker"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	agent_model "git.woa.com/yt-industry-ai/poseidon/internal/agent/model"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/http"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	"github.com/Tencent-Ti/ti-sign-go/tisign"
)

// TimakerHandler ExternalHandler的Timaker实现
type TimakerHandler struct {
	opt    *Option      // timaker 适配器参数
	client *http.Client // 用于调用 timaker HTTP 接口
}

// StartJob 启动Job，对应 timaker 接口 [创建训练任务接口 + 启动训练任务接口]
func (x *TimakerHandler) StartJob(
	ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob,
) (externalJobId string, retry bool, err error) {
	logger := common.GetLogger(ctx)

	logger.Debugf("start create timaker task")

	// 根据任务类型获取参数转换器，将 job 转换为 timaker 任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return "", false, common.Errorf(ctx, code.Code_InvalidParameter__TimakerParserNotExist)
	}
	info, err := parser.GetTimakerTaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to timaker task info fail")
		return "", false, err
	}

	// 检查参数
	if len(info.StartCmd) == 0 {
		info.StartCmd = strings.Join(containerJob.JobCtx.Config.Cmd, " ")
	}
	if len(info.ImageName) == 0 {
		info.ImageName = containerJob.JobCtx.Config.ImageName
	}
	if info.EnvConfig == nil {
		info.EnvConfig = []*pb_timaker.TimakerEnv{}
	}
	// 添加 Container.Prepare 注入的环境变量
	for k, v := range containerJob.JobCtx.Config.Env {
		info.EnvConfig = append(info.EnvConfig, &pb_timaker.TimakerEnv{
			Name:  k,
			Value: v,
		})
	}

	// inputDataConfig := []*pb_timaker.TimakerInputDataConfig{}
	// for source, target := range containerJob.JobCtx.Config.Mounts {
	// 	channel_name := strings.Join(filepath.SplitList(target), "_")
	// 	inputDataConfig = append(inputDataConfig, &pb_timaker.TimakerInputDataConfig{
	// 		ChannelName: channel_name,
	// 		DataSource: &pb_timaker.TimakerDataSource{
	// 			FileSystemDataSource: &pb_timaker.TimakerFileSystemDataSource{
	// 				FileSystemType: "hostpath",
	// 				DirectoryPath:  source,
	// 			},
	// 		},
	// 	})
	// }

	// 创建TiSign对象
	ts := tisign.NewTiSign(tisign.HttpHeaderContent{
		XTCAction:   "v1/CreateTrainingJob", // 调用接口名
		XTCService:  "timaker",              // 接口所属服务名
		XTCVersion:  "2020-10-12",           // 接口版本
		ContentType: "application/json",     // http请求的content-type
		HttpMethod:  "POST",                 // http请求方法
		Host:        info.Host,              // 访问网关的host
	}, info.SecretId, info.SecretKey)
	// 生成请求headers
	headers, _ := ts.CreateSignatureInfo()

	// 创建任务
	url := fmt.Sprintf("%s/gateway", info.Addr)
	req := &pb_timaker.CreateTimakerTaskRequest{
		Uin:               info.GetUin(),
		RequestId:         utils.NewUUID(),
		TiProjectId:       info.GetProjectId(),
		TrainingJobName:   info.GetTaskName(),
		TrainingJobSource: info.GetTrainingJobSource(),
		ResourceConfig: &pb_timaker.TimakerResourceConfig{
			Memory:        info.GetMemory(),
			CpuNum:        info.GetCpuNum(),
			GpuNum:        info.GetGpuNum(),
			GpuType:       info.GetGpuType(),
			InstanceCount: info.GetInstanceCount(),
			RDMAConfig: &pb_timaker.TimakerRDMAConfig{
				Enable: info.GetEnableRdma(),
			},
		},
		EnvConfig: info.EnvConfig,
		// InputDataConfig: inputDataConfig,
		AlgorithmSpecification: &pb_timaker.TimakerAlgorithmSpecification{
			EntryPoint:        info.GetStartCmd(),
			TrainingImageName: info.GetImageName(),
			AlgorithmSource: &pb_timaker.TimakerDataSource{
				FileSystemDataSource: info.FileSystemOption,
			},
		},
		Elastic: info.GetIsElasticity(),
		RestartPolicy: &pb_timaker.TimakerRestartPolicy{
			PolicyName: "AutoRestart",
		},
	}
	reqByte, _ := json.Marshal(req)

	// 调用 timaker 创建任务 API
	rsp := &pb_timaker.CreateTimakerTaskResponse{}
	_, httpCode, err := x.client.JSONPbPost(ctx, url, headers, req, rsp)
	logger.Debugf("timaker req [%s] response of create job detail: %+v", string(reqByte), rsp)
	if err != nil {
		logger.Errorf("create timaker job failed: %+v", err)
	}

	// 解析错误
	timakerErr := rsp.GetResponse().GetError()
	err = parseCallTimakerError(ctx, url, err, httpCode, timakerErr.GetCode(), timakerErr.GetMessage())
	if err != nil {
		logger.Errorf("create timaker job failed: %+v", err)
		return "", true, err
	}

	trainingJobId := rsp.GetResponse().GetTrainingJobId()
	info.TrainingJobId = trainingJobId
	// create 后会自动启动，再启动会失败 2023.08.30 remove start
	/*
		// 启动任务
		tsStart := tisign.NewTiSign(tisign.HttpHeaderContent{
			XTCAction:   "v1/StartTrainingJob", // 调用接口名
			XTCService:  "timaker",             // 接口所属服务名
			XTCVersion:  "2020-10-12",          // 接口版本
			ContentType: "application/json",    // http请求的content-type
			HttpMethod:  "POST",                // http请求方法
			Host:        info.Host,             // 访问网关的host
		}, info.SecretId, info.SecretKey)
		// 生成请求headers
		headersStart, _ := tsStart.CreateSignatureInfo()
		url = fmt.Sprintf("%s/gateway", info.Addr)
		startReq := &pb_timaker.StartTimakerTaskRequest{
			Uin:           info.GetUin(),
			RequestId:     utils.NewUUID(),
			TiProjectId:   info.ProjectId,
			TrainingJobId: trainingJobId,
		}

		// 调用 timaker 启动任务 API
		startRsp := &pb_timaker.StartTimakerTaskResponse{}
		_, httpCode, err = x.client.JSONPbPost(ctx, url, headersStart, startReq, startRsp)
		logger.Debugf("timaker response of start job detail: %+v", rsp)

		// 解析错误
		timakerErr = rsp.GetResponse().GetError()
		err = parseCallTimakerError(ctx, url, err, httpCode, timakerErr.GetCode(), timakerErr.GetMessage())
		if err != nil {
			logger.Errorf("start timaker task failed: %+v", err)
			return "", true, err
		}
	*/
	logger.Debugf("end create timaker task")
	return trainingJobId, false, nil
}

// DescribeJob 查看Job，对应 timaker 接口 [获取训练任务详情信息接口]
func (x *TimakerHandler) DescribeJob(
	ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob,
) (describeJob *model.Job, retry bool, err error) {
	logger := common.GetLogger(ctx)

	// 根据任务类型获取参数转换器，将 job 转换为 timaker 任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return nil, false, common.Errorf(ctx, code.Code_InvalidParameter__TimakerParserNotExist)
	}
	info, err := parser.GetTimakerTaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to timaker task info fail")
		return nil, false, err
	}

	// 创建TiSign对象
	ts := tisign.NewTiSign(tisign.HttpHeaderContent{
		XTCAction:   "v1/DescribeTrainingJob", // 调用接口名
		XTCService:  "timaker",                // 接口所属服务名
		XTCVersion:  "2020-10-12",             // 接口版本
		ContentType: "application/json",       // http请求的content-type
		HttpMethod:  "POST",                   // http请求方法
		Host:        info.Host,                // 访问网关的host
	}, info.SecretId, info.SecretKey)
	// 生成请求headers
	headers, _ := ts.CreateSignatureInfo()

	// 获取任务实例列表
	url := fmt.Sprintf("%s/gateway", info.Addr)
	req := &pb_timaker.DescribeTimakerTaskRequest{
		Uin:           info.GetUin(),
		RequestId:     utils.NewUUID(),
		TiProjectId:   info.GetProjectId(),
		TrainingJobId: job.ExternalJobId, // task info 中的 TrainingJobId 为空, 使用数据库中保存的 job id
	}

	// 调用 timaker 获取训练任务详情信息 API
	rsp := &pb_timaker.DescribeTimakerTaskResponse{}
	_, httpCode, err := x.client.JSONPbPost(ctx, url, headers, req, rsp)
	response := rsp.GetResponse()
	reqByte, _ := json.Marshal(req)
	rspByte, _ := json.Marshal(rsp)
	logger.Debugf("timaker req/response of get job detail: [%s, %s]", string(reqByte), string(rspByte))

	// 解析错误
	err = parseCallTimakerError(ctx, url, err, httpCode, response.GetError().GetCode(), response.GetError().GetMessage())
	if err != nil {
		logger.Errorf("get timaker job status failed: %+v", err)
		return nil, true, err
	}

	// 解析任务的创建时间和结束时间
	timeZone, _ := time.LoadLocation("Asia/Shanghai")
	createTime, err := time.ParseInLocation("2006-01-02 15:04:05", response.GetTrainingCreateTime(), timeZone)
	if err != nil {
		logger.WithError(err).Errorf("parse timaker time string fail")
		return nil, false, common.Errorf(ctx, code.Code_InternalError__Unknown)
	}
	if len(response.GetTrainingEndTime()) > 0 {
		endTime, err := time.ParseInLocation("2006-01-02 15:04:05", response.GetTrainingEndTime(), timeZone)
		if err != nil {
			logger.WithError(err).Errorf("parse timaker time string fail")
			return nil, false, common.Errorf(ctx, code.Code_InternalError__Unknown)
		}
		info.EndTime = utils.FormatTime(endTime)
	}

	info.Msg = fmt.Sprintf(
		"errorcode: %s, errormsg: %s", response.GetFailureCode(), response.GetFailureReason())
	info.State = rsp.GetResponse().GetTrainingJobStatus()
	info.CreateTime = utils.FormatTime(createTime)
	info.InstanceId = rsp.GetResponse().GetInstanceId()

	respJob := &model.Job{}
	respJob.GetJobLive().StartTime = info.CreateTime
	respJob.GetJobLive().EndTime = info.EndTime

	// 转换状态
	switch info.State {
	case "Completed":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_SUCCESS
	case "Failed",
		"Evicted":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_FAILED
		respJob.GetJobLive().Error = common.ErrorfProto(ctx, code.Code_InternalError__TimakerTaskFailed, info.Msg)
	case "JobReceived",
		"PrepareInstance",
		"InstanceReady",
		"PreResume",
		"Resuming":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_PREPARING
	case "Training",
		"Suspending":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_RUNNING
	case "PreStopped",
		"Stopping",
		"Suspended",
		"PreSuspend":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_STOPPING
	case "Stopped":
		respJob.JobStatus = types.JobStatus_JOB_STATUS_CANCELED
	default:
		logger.Debugf("timaker task state %s", info.State)
	}

	logger.Debugf("timaker %s state %s, job status %s", job.JobId, info.State, respJob.JobStatus.String())

	return respJob, false, nil
}

// StopJob 停止Job，对应 timaker 接口 [停止训练任务接口]
func (x *TimakerHandler) StopJob(
	ctx context.Context, job *model.Job, containerJob *agent_model.ContainerJob,
) (retry bool, err error) {
	logger := common.GetLogger(ctx)

	// 根据任务类型获取参数转换器，将 job 转换为 timaker 任务参数
	parser, ok := x.opt.JobParsers[job.JobType]
	if !ok {
		return false, common.Errorf(ctx, code.Code_InvalidParameter__TimakerParserNotExist)
	}
	info, err := parser.GetTimakerTaskInfo(ctx, (*types.Job)(job.ToPb()))
	if err != nil {
		logger.WithError(err).Errorf("parse job to timaker task info fail")
		return false, err
	}

	logger.Debugf("start kill timaker task")

	// 创建TiSign对象
	ts := tisign.NewTiSign(tisign.HttpHeaderContent{
		XTCAction:   "v1/StopTrainingJob", // 调用接口名
		XTCService:  "timaker",            // 接口所属服务名
		XTCVersion:  "2020-10-12",         // 接口版本
		ContentType: "application/json",   // http请求的content-type
		HttpMethod:  "POST",               // http请求方法
		Host:        info.Host,            // 访问网关的host
	}, info.SecretId, info.SecretKey)
	// 生成请求headers
	headers, _ := ts.CreateSignatureInfo()

	// 停止任务
	url := fmt.Sprintf("%s/gateway", info.Addr)
	req := &pb_timaker.StopTimakerTaskRequest{
		Uin:           info.GetUin(),
		RequestId:     utils.NewUUID(),
		TiProjectId:   info.GetProjectId(),
		TrainingJobId: job.ExternalJobId, // task info 中的 TrainingJobId 为空, 使用数据库中保存的 job id
	}

	rsp := &pb_timaker.StopTimakerTaskResponse{}
	_, httpCode, err := x.client.JSONPbPost(ctx, url, headers, req, rsp)
	logger.Debugf("timaker response of stop job detail: %+v", rsp)

	// 解析错误
	timakerErr := rsp.GetResponse().GetError()
	err = parseCallTimakerError(ctx, url, err, httpCode, timakerErr.GetCode(), timakerErr.GetMessage())
	if err != nil {
		logger.Errorf("stop timaker job failed: %+v", err)
		return false, err
	}

	// 查询状态
	_, retry, err = x.DescribeJob(ctx, job, containerJob)
	if err != nil {
		logger.WithError(err).Errorf("get timaker task fail")
		return retry, err
	}
	// 不能对 stopping 状态报错
	// if !types.IsEnded(respJob.JobStatus) {
	// 	logger.Debugf("timaker task is ended")
	// 	return true, common.Errorf(ctx, code.Code_InternalError__TimakerTaskNotEnded)
	// }

	logger.Debugf("end kill timaker task")
	return false, nil
}

// 解析 timaker 错误
func parseCallTimakerError(ctx context.Context, url string, err error, httpCode int, rspCode string,
	rspMsg string) error {
	logger := common.GetLogger(ctx)
	if err != nil {
		logger.WithError(err).Errorf("call timaker service failed. http_code: %d", httpCode)
		if errors.Is(err, context.DeadlineExceeded) {
			// 超时
			logger.WithError(err).Errorf("timaker timeout: %+v", httpCode)
			return common.Errorf(ctx, code.Code_InternalError__TimakerTimeout)
		}
		// 调用 HTTP 错误
		return common.Errorf(ctx, code.Code_InternalError__TimakerHttpFailed, httpCode)
	}
	if len(rspCode) != 0 {
		// 业务错误
		msg := fmt.Sprintf("Timaker Service return errorcode: %s, errormsg: %s", rspCode, rspMsg)
		logger.WithError(err).Errorf(msg)
		return common.Errorf(ctx, code.Code_InternalError__TimakerFailed, msg)
	}
	return nil
}
