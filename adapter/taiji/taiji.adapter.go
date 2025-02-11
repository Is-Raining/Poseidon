package taiji

import (
	"context"
	"time"

	"git.woa.com/yt-industry-ai/poseidon/adapter/external"
	pb_taiji "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/taiji"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/http"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
	"github.com/jmoiron/sqlx"
)

const (
	TaijiAdapterType        = "psd.adapter.taiji"
	taijiSidecarAdapterType = "psd.adapter.taiji_sidecar" // deprecated
)

const (
	taijiApiTimeout = 5 * time.Second // 调用太极超时时间
)

type Option struct {
	DB             *sqlx.DB              // 数据库实例
	DBTimeout      time.Duration         // 数据库查询超时时间
	JobDir         string                // job 根目录
	LogDir         string                // job 日志根目录
	ErrorMarshaler types_.ErrorMarshaler // 错误码转换接口

	// 太极
	JobParsers     map[int32]TaijiJobParser // job 参数转换器: JobType -> TaijiJobParser
	FixedDatasetId string                   // 默认数据集 ID（太极接口要求）
	FixedCodePkgId string                   // 默认代码包 ID（太极接口要求）
}

// TaijiAdapter 公司内部太极协议适配器
type TaijiAdapter struct {
	*external.ExternalAdapter // 实现 Adapter 接口

	taijiHandler *TaijiHandler // 实现 ExternalHandler
}

// 创建太极适配器参数
func NewTaijiAdapterOption() *Option {
	return &Option{}
}

// 创建太极适配器
func NewTaijiAdapter(opt *Option) *TaijiAdapter {
	// 创建 HTTP 客户端
	client, _ := http.New(http.WithTimeout(taijiApiTimeout))
	taijiHandler := &TaijiHandler{
		opt:    opt,
		client: client,
	}

	// 该适配器要处理的 job 类型为 JobParsers 的 key 集合
	jobTypes := []int32{}
	for jobType := range opt.JobParsers {
		jobTypes = append(jobTypes, jobType)
	}

	// 传入 TaijiHandler，创建 ExternalAdapter 实例
	externalAdapter := external.NewExternalAdapter(taijiHandler, &external.ExternalAdapterOption{
		DB:             opt.DB,
		DBTimeout:      opt.DBTimeout,
		JobDir:         opt.JobDir,
		LogDir:         opt.LogDir,
		ErrorMarshaler: opt.ErrorMarshaler,
		JobTypes:       jobTypes,
		AdapterTypes:   []string{TaijiAdapterType, taijiSidecarAdapterType},
		CheckPeriod:    time.Minute, // 每分钟检查一次任务的输出目录
	})

	return &TaijiAdapter{
		ExternalAdapter: externalAdapter,
		taijiHandler:    taijiHandler,
	}
}
func (x *TaijiAdapter) GetAdapterType(ctx context.Context) string {
	return TaijiAdapterType
}

// 太极专用方法：DescribeResource 查询资源使用情况
func (x *TaijiAdapter) DescribeResource(ctx context.Context, apiToken, business string) (
	rspData *pb_taiji.DescribeTaijiResourceResponseData, err error) {
	logger := common.GetLogger(ctx)

	// 调用太极查询资源情况
	headers := map[string]string{"content-type": "application/json", "IPLUS-TASK-SERVER-API-TOKEN": apiToken}

	url := getTaijiTaskApiUrl("business/resource_query/")
	req := &pb_taiji.DescribeTaijiResourceRequest{
		Id:      utils.NewUID(),
		Jsonrpc: "2.0",
		Method:  "BUSINESS_RESOURCE_QUERY",
		Params: &pb_taiji.DescribeTaijiResourceRequestParam{
			BusinessFlag: business,
		},
	}
	rsp := &pb_taiji.DescribeTaijiResourceResponse{}
	body, httpCode, err := x.taijiHandler.client.JSONPbPost(ctx, url, headers, req, rsp)
	logger.Debugf("taiji response of query resource: %s", string(body))

	// 解析太极回包
	err = parseCallTaijiError(ctx, url, err, httpCode, rsp.GetResult().GetCode(), rsp.GetResult().GetMessage())
	if err != nil {
		return nil, err
	}

	return rsp.GetResult().GetData(), nil
}
