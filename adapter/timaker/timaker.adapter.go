package timaker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.woa.com/yt-industry-ai/poseidon/adapter/external"
	pb_local "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/local"
	pb_timaker "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/timaker"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/dao"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/http"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
	"github.com/Tencent-Ti/ti-sign-go/tisign"
	"github.com/jmoiron/sqlx"
)

const (
	// TimakerAdapterType Timaker适配器类型
	TimakerAdapterType = "psd.adapter.timaker"
)

const (
	timeout = 5 * time.Second // 调用 timaker 超时时间
)

// TimakerCfg TODO
type TimakerCfg struct {
	AppGroupId int32
	Host       string
	Addr       string
	Uin        string
	SecretId   string
	SecretKey  string
}

// Option 适配器参数结构体
type Option struct {
	DB                *sqlx.DB               // 数据库实例
	DBTimeout         time.Duration          // 数据库查询超时时间
	JobDir            string                 // job 根目录
	LogDir            string                 // job 日志根目录
	ErrorMarshaler    types_.ErrorMarshaler  // 错误码转换接口
	ContainerEnvOpt   *external.ContainerEnv // 容器内环境变量
	DefaultTimakerCfg *TimakerCfg            // timaker资源描述参数

	JobParsers map[int32]TimakerJobParser // job 参数转换器: JobType -> TimakerJobParser

}

// TimakerAdapter 适配器结构体
type TimakerAdapter struct {
	*external.ExternalAdapter // 实现 Adapter 接口

	timakerProcessor *TimakerHandler // 实现 ExternalHandler

	DefaultTimakerCfg *TimakerCfg // timaker资源描述参数

	jobDao *dao.JobDao // 任务存储层
}

// NewTimakerAdapterOption 创建 timaker 适配器参数
func NewTimakerAdapterOption() *Option {
	return &Option{}
}

// NewTimakerAdapter 创建 timaker 适配器
func NewTimakerAdapter(opt *Option) *TimakerAdapter {
	// 创建 HTTP 客户端
	client, _ := http.New(http.WithTimeout(timeout))
	timakerProcessor := &TimakerHandler{
		opt:    opt,
		client: client,
	}

	// 该适配器要处理的 job 类型为 JobParsers 的 key 集合
	jobTypes := []int32{}
	for jobType := range opt.JobParsers {
		jobTypes = append(jobTypes, jobType)
	}

	// 传入 TimakerHandler，创建 ExternalAdapter 实例
	externalAdapter := external.NewExternalAdapter(timakerProcessor, &external.ExternalAdapterOption{
		DB:              opt.DB,
		DBTimeout:       opt.DBTimeout,
		JobDir:          opt.JobDir,
		LogDir:          opt.LogDir,
		ErrorMarshaler:  opt.ErrorMarshaler,
		JobTypes:        jobTypes,
		AdapterTypes:    []string{TimakerAdapterType},
		CheckPeriod:     time.Minute,
		ContainerEnvOpt: opt.ContainerEnvOpt,
	})
	return &TimakerAdapter{
		ExternalAdapter:   externalAdapter,
		timakerProcessor:  timakerProcessor,
		DefaultTimakerCfg: opt.DefaultTimakerCfg,
		jobDao:            dao.NewJobDao(opt.DB, nil),
	}
}
func (x *TimakerAdapter) GetAdapterType(ctx context.Context) string {
	return TimakerAdapterType
}

// Allocate TODO
// TODO 增加了timaker 任务资源的校验，资源不足时让任务在数据库继续等待，否则会提交失败
func (x *TimakerAdapter) Allocate(
	ctx context.Context, job *model.Job, gather *model.Gather,
) ([]*types.Resource, error) {
	logger := common.GetLogger(ctx)
	// defer func() {
	// 	if err := recover(); err != nil {
	// 		logger.Errorf("recover from panic: %v", err)
	// 		logger.Debugf("stacktrace from panic: %s\n", string(debug.Stack()))
	// 		time.Sleep(time.Second * 5)
	// 	}
	// }()
	logger.Debugf("[TimakerAdapterAllocate]start check timaker resource")
	// 任务至少要有 ResourceRequire 参数
	if job.GetResourceRequire() == nil {
		logger.Errorf("[TimakerAdapterAllocate]resource require is nil")
		return nil, common.Errorf(ctx, code.Code_InvalidParameterValue__ParameterValueInvalid, "ResourceRequire")
	}
	if x.DefaultTimakerCfg == nil {
		logger.Warnf("[TimakerAdapterAllocate]timaker check resource default cfg is nil")
		return nil, nil
	}
	// 等待上一个任务申请到对应的资源后，资源被扣掉，再申请当前任务的资源
	time.Sleep(10 * time.Second)
	resourceRsp, err := x.DescribeResource(ctx, x.DefaultTimakerCfg.AppGroupId, x.DefaultTimakerCfg.Host,
		x.DefaultTimakerCfg.Addr, x.DefaultTimakerCfg.Uin, x.DefaultTimakerCfg.SecretId, x.DefaultTimakerCfg.SecretKey)

	if nil != err {
		logger.Errorf("[TimakerAdapterAllocate]GetTimakerResource Failed: %+v", err)
		return nil, err
	}
	availableGpus := resourceRsp.AvailableResource.Train.VGpu + 100*resourceRsp.AvailableResource.Elastic
	if strings.HasPrefix(job.ResourceRequire.GpuType, "V100") {
		availableGpus += 100 * resourceRsp.AvailableResource.Train.V100
	} else if strings.HasPrefix(job.ResourceRequire.GpuType, "A100") {
		availableGpus += 100 * resourceRsp.AvailableResource.Train.A100
	} else if strings.HasPrefix(job.ResourceRequire.GpuType, "T4") {
		availableGpus += 100 * resourceRsp.AvailableResource.Train.T4
	} else if strings.HasPrefix(job.ResourceRequire.GpuType, "P40") {
		availableGpus += 100 * resourceRsp.AvailableResource.Train.P40
	}
	if job.ResourceRequire.GpuNum > 0 && 100*job.ResourceRequire.GpuNum > availableGpus {
		logger.Errorf("[TimakerAdapterAllocate]timaker train resource gpu in sufficient: [%+v][%+v]", job.ResourceRequire,
			resourceRsp)
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__GPUInsufficient)
	} else {
		logger.Debugf("[TimakerAdapterAllocate] check gpu ok")
	}
	if job.ResourceRequire.CpuNum > 0 && 1000*job.ResourceRequire.CpuNum > resourceRsp.AvailableResource.Train.Cpu {
		logger.Errorf("[TimakerAdapterAllocate]timaker train resource cpu in sufficient: [%+v][%+v]", job.ResourceRequire,
			resourceRsp)
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__CPUInsufficient)
	} else {
		logger.Debugf("[TimakerAdapterAllocate] check cpu ok")
	}
	if job.ResourceRequire.Memory > 0 && job.ResourceRequire.Memory > resourceRsp.AvailableResource.Train.Memory {
		logger.Errorf("[TimakerAdapterAllocate]timaker train resource mem in sufficient: [%+v][%+v]", job.ResourceRequire,
			resourceRsp)
		return nil, common.Errorf(ctx, code.Code_ResourceInsufficient__MEMInsufficient)
	} else {
		logger.Debugf("[TimakerAdapterAllocate] check memory ok")
	}
	logger.Infof("[TimakerAdapterAllocate]check timaker job resource ok: [%+v][%+v]", job.ResourceRequire, resourceRsp)
	if len(job.GetJobLive().StartTime) < 1 {
		job.GetJobLive().StartTime = utils.FormatTime(time.Now())
	}
	fakeGpus := make([]int32, 0)
	for id := int32(0); id < job.ResourceRequire.GpuNum; id++ {
		fakeGpus = append(fakeGpus, id)
	}
	resources := make([]*types.Resource, 0)
	resources = append(resources, &types.Resource{
		Label:   job.ResourceRequire.Label,
		Ip:      x.DefaultTimakerCfg.Addr,
		Gpus:    fakeGpus,
		GpuType: job.ResourceRequire.GpuType,
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
	}
	logger.Debugf("[TimakerAdapterAllocate] job use resource [%+v]", resources)
	return resources, nil
}

// DescribeResource 查询集群的可用资源
func (x *TimakerAdapter) DescribeResource(
	ctx context.Context, appGroupId int32,
	host, addr, uin, secretId, secretKey string,
) (rspData *pb_timaker.DescribeTimakerResourceRsp, err error) {
	logger := common.GetLogger(ctx)
	url := fmt.Sprintf("%s/gateway", addr)

	// 创建TiSign对象
	tsTotal := tisign.NewTiSign(tisign.HttpHeaderContent{
		XTCAction:   "ResourceServer/Resources/GetTotalAppGroupResources", // 调用接口名
		XTCService:  "ti-resource-server",                                 // 接口所属服务名
		XTCVersion:  "2020-10-10",                                         // 接口版本
		ContentType: "application/json",                                   // http请求的content-type
		HttpMethod:  "POST",                                               // http请求方法
		Host:        host,                                                 // 访问网关的host
	}, secretId, secretKey)
	// 生成请求headers
	headersTotal, _ := tsTotal.CreateSignatureInfo()

	reqTotal := &pb_timaker.DescribeTimakerResourceRequest{
		Uin:        uin,
		RequestId:  utils.NewUUID(),
		AppGroupId: appGroupId,
	}

	rspTotal := &pb_timaker.DescribeTimakerResourceResponse{}
	_, httpCode, err := x.timakerProcessor.client.JSONPbPost(ctx, url, headersTotal, reqTotal, rspTotal)
	logger.Debugf("[GetTotalAppGroupResources] timaker response of get resource detail: %+v", rspTotal)

	timakerErr := rspTotal.GetResponse().GetError()
	// 解析 timaker 回包
	err = parseCallTimakerError(ctx, url, err, httpCode, timakerErr.GetCode(), timakerErr.GetMessage())
	if err != nil {
		logger.Errorf("describe timaker resource failed: %+v", err)
		return nil, err
	}

	// 创建TiSign对象
	ts := tisign.NewTiSign(tisign.HttpHeaderContent{
		XTCAction:   "ResourceServer/Resources/GetAvailableAppGroupResources", // 调用接口名
		XTCService:  "ti-resource-server",                                     // 接口所属服务名
		XTCVersion:  "2020-10-10",                                             // 接口版本
		ContentType: "application/json",                                       // http请求的content-type
		HttpMethod:  "POST",                                                   // http请求方法
		Host:        host,                                                     // 访问网关的host
	}, secretId, secretKey)
	// 生成请求headers
	headers, _ := ts.CreateSignatureInfo()

	req := &pb_timaker.DescribeTimakerResourceRequest{
		Uin:        uin,
		RequestId:  utils.NewUUID(),
		AppGroupId: appGroupId,
	}

	rsp := &pb_timaker.DescribeTimakerResourceResponse{}
	_, httpCode, err = x.timakerProcessor.client.JSONPbPost(ctx, url, headers, req, rsp)
	logger.Debugf("[GetAvailableAppGroupResources] timaker response of get resource detail: %+v", rsp)

	timakerErr = rsp.GetResponse().GetError()
	// 解析 timaker 回包
	err = parseCallTimakerError(ctx, url, err, httpCode, timakerErr.GetCode(), timakerErr.GetMessage())
	if err != nil {
		logger.Errorf("describe timaker resource failed: %+v", err)
		return nil, err
	}

	return &pb_timaker.DescribeTimakerResourceRsp{
		TotalResource:     rspTotal.GetResponse().GetResources(),
		AvailableResource: rsp.GetResponse().GetResources(),
	}, nil
}

// DescribeUsedResource 查询集群的已用资源
func (x *TimakerAdapter) DescribeUsedResource(
	ctx context.Context, req *pb_local.DescribeUsedResourcesRequest,
) (resp *pb_local.DescribeUsedResourcesResponse, err error) {
	resp = &pb_local.DescribeUsedResourcesResponse{}
	jobs, err := x.jobDao.GetJobs(ctx, nil, []string{model.ColJobJobStatus}, nil,
		map[string]interface{}{
			model.ColJobJobStatus: types.GetResourceConsumingJobStatuses().EnumInt32s(),
		},
	)
	if err != nil {
		common.GetLogger(ctx).Errorf("get jobs failed: %v", err)
		return nil, err
	}
	projects := make(map[string]*types.Resources)
	nodes := make(map[string]*types.Resource)
	for _, job := range jobs {
		for _, resource := range job.GetResources() {
			if len(resource.GetGpus()) < 1 {
				continue
			}

			if _, ok := projects[job.ProjectId]; !ok {
				projects[job.ProjectId] = &types.Resources{
					Resources: map[string]*types.Resource{},
				}
			}
			project := projects[job.ProjectId].GetResources()
			if _, ok := project[resource.GetGpuType()]; !ok {
				project[resource.GetGpuType()] = &types.Resource{}
			}
			ip := resource.GetIp()
			if _, ok := nodes[ip]; !ok {
				nodes[ip] = &types.Resource{}
			}

			p := project[resource.GetGpuType()]
			p.GpuType = resource.GetGpuType()
			p.Gpus = append(p.Gpus, resource.GetGpus()...)

			n := nodes[ip]
			n.Ip = resource.GetIp()
			n.GpuType = resource.GetGpuType()
			n.Gpus = append(n.Gpus, resource.GetGpus()...)

		}
	}
	resp.Projects = projects
	resp.Nodes = nodes

	return resp, nil
}
