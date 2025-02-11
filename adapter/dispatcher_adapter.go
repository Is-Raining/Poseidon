package adapter

import (
	"context"

	"git.woa.com/yt-industry-ai/grpc-go-ext/log"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DispatcherAdapter TODO
// 调度适配器接口，适配不同的集群模式和调度方法 ...
type DispatcherAdapter interface {
	// 启动后台线程，若初始化失败，则直接退出
	StartDaemon(ctx context.Context)

	// 启动任务，应尽快返回，具体的启动逻辑异步执行，结果由 DescribeJob 查询，如果任务不存在，应自动创建之
	// Args:
	//  job: 任务元信息
	// Return:
	//  jobErr: 任务执行逻辑失败原因（任务 Hook 输出）
	//  retry: 若 jobErr != nil || err != nil，是否重试
	//  err: 任务调度失败原因（Adapter 输出）
	StartJob(ctx context.Context, job *model.Job) (jobErr *types.Error, retry bool, err error)

	// 停止任务，应尽快返回，具体的停止逻辑异步执行，结果由 DescribeJob 查询
	// Args: 参考 StartJob
	// Return: 参考 StartJob，若任务不存在，则返回 ResourceNotFound__JobNotExist
	StopJob(ctx context.Context, job *model.Job) (jobErr *types.Error, retry bool, err error)

	// 查询任务状态
	// Args:
	//  job: 任务元信息
	// Return:
	//  describeJob: 查询结果
	//  retry: 若任务查询失败是否重试
	//  err: 任务查询失败原因（Adapter 输出），若任务不存在，则返回 ResourceNotFound__JobNotExist，
	//       任务执行逻辑失败原因通过 describeJob.GetJobLive().Error 输出
	DescribeJob(ctx context.Context, job *model.Job) (describedJob *model.Job, retry bool, err error)

	// 获取当前要调度的任务列表和资源情况
	Gather(ctx context.Context) (gather *model.Gather, err error)

	// 为指定任务 job 分配资源，分配后的资源情况需反映到入参 gather，返回错误则被视为资源不足
	Allocate(ctx context.Context, job *model.Job, gather *model.Gather) ([]*types.Resource, error)

	// 判断当前状态是否就绪
	IsReadyByStatus(ctx context.Context, status types.JobStatus, job *model.Job, gather *model.Gather) bool

	GetAdapterType(ctx context.Context) string

	TryUpdateJobResource(ctx context.Context, job *model.Job) bool
}

// UnimplementedDispatcherAdapter TODO
type UnimplementedDispatcherAdapter struct {
}

// StartDaemon TODO
func (a UnimplementedDispatcherAdapter) StartDaemon(ctx context.Context) {
}

// StartJob TODO
func (a UnimplementedDispatcherAdapter) StartJob(ctx context.Context, job *model.Job) (
	jobErr *types.Error, retry bool, err error) {
	return nil, false, status.Errorf(codes.Unimplemented, "method StartJob not implemented")
}

// StopJob TODO
func (a UnimplementedDispatcherAdapter) StopJob(ctx context.Context, job *model.Job) (
	jobErr *types.Error, retry bool, err error) {
	return nil, false, status.Errorf(codes.Unimplemented, "method StopJob not implemented")
}

// DescribeJob TODO
func (a UnimplementedDispatcherAdapter) DescribeJob(ctx context.Context, job *model.Job) (
	respJob *model.Job, retry bool, err error) {
	return nil, false, status.Errorf(codes.Unimplemented, "method DescribeJob not implemented")
}

// Gather TODO
func (a UnimplementedDispatcherAdapter) Gather(ctx context.Context) (gather *model.Gather, err error) {
	return nil, status.Errorf(codes.Unimplemented, "method Gather not implemented")
}

// Allocate TODO
func (a UnimplementedDispatcherAdapter) Allocate(
	ctx context.Context, job *model.Job, gather *model.Gather,
) ([]*types.Resource, error) {
	log.Warnf("")
	return nil, nil
}

// IsReadyByStatus TODO
func (a UnimplementedDispatcherAdapter) IsReadyByStatus(
	ctx context.Context, status types.JobStatus, job *model.Job, gather *model.Gather) bool {
	return true
}
