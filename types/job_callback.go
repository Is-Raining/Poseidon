package types

import (
	"context"

	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
)

// 任务回调接口
// TODO(chuangchen): 整理接口参数
type JobCallback interface {
	// 当类型为 jobType，ID 为 jobId 的任务通过接口 ReportJob 上报时，回调该方法，jobInfo 为上报的内容；
	// 返回的 error 将作为接口 ReportJob 的回参透传给任务。
	OnJobReported(ctx context.Context, jobId string, jobType int32, jobInfo *types.ReportJobInfo) error

	// 当 ID 为 jobId 的任务状态变更为 jobStatus 时，回调该方法，pbErr 为任务错误。
	// 当 jobStatus 为 FAILED，并且返回值 restart 为 true 时，任务状态会被重置为 RESTARTING，放入等待队列中，
	// 业务可在该回调中实现自定义的失败重试逻辑；
	// 返回的 error 将作为接口 ReportJob 的回参透传给任务。
	OnJobStatusChanged(
		ctx context.Context, jobId string, jobStatus types.JobStatus, pbErr *types.Error) (restart bool, err error)

	// 当 ID 为 jobId 的容器任务被 Docker 自动分配/移除了端口时，回调该方法，target 为 "<容器所在宿主机 IP>:<宿主机端口>"，
	// jobStatus 为目标状态，可能为 RUNNING/STARTING/SUCCESS/FAILED/CANCELED
	OnJobExposeChanged(ctx context.Context, jobId, target string, jobStatus types.JobStatus) error
}

// 默认任务回调接口实现
type DefaultJobCallback struct{}

// OnJobReported ...
func (DefaultJobCallback) OnJobReported(
	ctx context.Context, jobId string, jobType int32, info *types.ReportJobInfo,
) error {
	return nil
}

// OnJobStatusChanged ...
func (DefaultJobCallback) OnJobStatusChanged(
	ctx context.Context, jobId string, status types.JobStatus, pbErr *types.Error) (restart bool, err error) {
	return false, nil
}

// OnJobExposeChanged ...
func (DefaultJobCallback) OnJobExposeChanged(
	ctx context.Context, jobId, target string, status types.JobStatus,
) error {
	return nil
}
