package model

import (
	"context"
	"runtime/debug"

	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
	"go.uber.org/atomic"
)

// 容器任务的运行实例
type ContainerJob struct {
	BaseJob

	Ctx                      context.Context    // 上下文
	Cancel                   context.CancelFunc // 取消函数
	DockerInspectFailedTimes atomic.Int32       // docker inspect 失败次数
	WaitGroup                *utils.WaitGroup   // 后台协程组

	JobCtx  *types_.ContainerJobContext // 任务上下文（Function 接口入参）
	Handler types_.Container            // Container 接口实例

	ContainerState Atomic[*types.DockerContainerState] // 容器状态
}

// 创建容器任务运行实例 ...
func NewContainerJob() *ContainerJob {
	return &ContainerJob{
		ContainerState: Atomic[*types.DockerContainerState]{},
		JobCtx: &types_.ContainerJobContext{
			Config: types_.NewContainerConfig(),
		},
	}
}

// 获取 JobId
func (job *ContainerJob) JobId() string {
	return job.JobCtx.JobId
}

// 获取 JobDir
func (job *ContainerJob) JobDir() string {
	return job.JobCtx.JobDir
}

// 获取 LogDir
func (job *ContainerJob) LogDir() string {
	return job.JobCtx.LogDir
}

// 创建 Handler 的代理对象
func (job *ContainerJob) HandlerCaller() ContainerJobHandlerCaller {
	return NewContainerJobHandlerCaller(job.Handler)
}

// types.Container 的代理对象，用于安全地调用用户实现的方法
type ContainerJobHandlerCaller struct {
	handler types_.Container
}

// 创建 handler 的代理对象
func NewContainerJobHandlerCaller(handler types_.Container) ContainerJobHandlerCaller {
	return ContainerJobHandlerCaller{handler: handler}
}

// 代理 Container.Initialize
func (x ContainerJobHandlerCaller) Initialize(ctx context.Context, jobCtx *types_.ContainerJobContext) error {
	logger := common.GetLogger(ctx)

	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Errorf("run job initialize panic: %v, stacktrace: %s", err, string(debug.Stack()))
			err = common.Errorf(ctx, code.Code_InternalError__InitializeJob)
		}
	}()
	return x.handler.Initialize(ctx, jobCtx)
}

// 代理 Container.Prepare
func (x ContainerJobHandlerCaller) Prepare(ctx context.Context, jobCtx *types_.ContainerJobContext) error {
	logger := common.GetLogger(ctx)

	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Errorf("run job prepare panic: %v, stacktrace: %s", err, string(debug.Stack()))
			err = common.Errorf(ctx, code.Code_InternalError__PrepareJob)
		}
	}()
	return x.handler.Prepare(ctx, jobCtx)
}

// 代理 Container.Check
func (x ContainerJobHandlerCaller) Check(ctx context.Context, jobCtx *types_.ContainerJobContext) error {
	logger := common.GetLogger(ctx)

	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Errorf("run job check panic: %v, stacktrace: %s", err, string(debug.Stack()))
			err = common.Errorf(ctx, code.Code_InternalError__CheckJob)
		}
	}()
	return x.handler.Check(ctx, jobCtx)
}

// 代理 Container.Submit
func (x ContainerJobHandlerCaller) Submit(ctx context.Context, jobCtx *types_.ContainerJobContext) error {
	logger := common.GetLogger(ctx)

	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Errorf("run job submit panic: %v, stacktrace: %s", err, string(debug.Stack()))
			err = common.Errorf(ctx, code.Code_InternalError__SubmitJob)
		}
	}()
	return x.handler.Submit(ctx, jobCtx)
}

// 代理 Container.Clean
func (x ContainerJobHandlerCaller) Clean(ctx context.Context, jobCtx *types_.ContainerJobContext) error {
	logger := common.GetLogger(ctx)

	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Errorf("run job clean panic: %v, stacktrace: %s", err, string(debug.Stack()))
			err = common.Errorf(ctx, code.Code_InternalError__CleanJob)
		}
	}()
	return x.handler.Clean(ctx, jobCtx)
}
