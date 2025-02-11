package types

import (
	"context"

	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/master"
)

type MasterController interface {
	// 启动调度
	StartSchedule() error
	// 上报原子任务进度和结果
	ReportJob(context.Context, *master.ReportJobRequest) (*master.ReportJobResponse, error)
	// 创建原子任务
	CreateJob(context.Context, *master.CreateJobRequest) (*master.CreateJobResponse, error)
	// 取消原子任务
	CancelJob(context.Context, *master.CancelJobRequest) (*master.CancelJobResponse, error)
	// 重启原子任务
	RestartJob(context.Context, *master.RestartJobRequest) (*master.RestartJobResponse, error)
	// 查询原子任务列表
	DescribeJobs(context.Context, *master.DescribeJobsRequest) (*master.DescribeJobsResponse, error)
	// 查询原子任务详情
	DescribeJob(context.Context, *master.DescribeJobRequest) (*master.DescribeJobResponse, error)
	// 删除原子任务
	DeleteJobs(context.Context, *master.DeleteJobsRequest) (*master.DeleteJobsResponse, error)
	// 取消原子任务
	CancelJobs(context.Context, *master.CancelJobsRequest) (*master.CancelJobsResponse, error)
	// 更新原子任务
	UpdateJob(context.Context, *master.UpdateJobRequest) (*master.UpdateJobResponse, error)
	// 通知任务操作
	NotifyJobs(context.Context, *master.NotifyJobsRequest) (*master.NotifyJobsResponse, error)
}
