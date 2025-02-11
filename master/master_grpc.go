package master

import (
	"context"

	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/master"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/controller"
	"git.woa.com/yt-industry-ai/poseidon/types"
)

// GrpcMasterController ...
type GrpcMasterController struct {
	master.UnimplementedPoseidonMasterServiceServer
	types.MasterController
}

// NewGrpcController ...
func NewGrpcController(opt *controller.Option) *GrpcMasterController {
	return &GrpcMasterController{
		MasterController: NewController(opt),
	}
}

func (c GrpcMasterController) StartSchedule() (err error) {
	return c.MasterController.StartSchedule()
}

func (c GrpcMasterController) ReportJob(
	ctx context.Context, req *master.ReportJobRequest,
) (resp *master.ReportJobResponse, err error) {
	return c.MasterController.ReportJob(ctx, req)
}

func (c GrpcMasterController) CreateJob(
	ctx context.Context, req *master.CreateJobRequest,
) (resp *master.CreateJobResponse, err error) {
	return c.MasterController.CreateJob(ctx, req)
}

func (c GrpcMasterController) CancelJob(
	ctx context.Context, req *master.CancelJobRequest,
) (resp *master.CancelJobResponse, err error) {
	return c.MasterController.CancelJob(ctx, req)
}

func (c GrpcMasterController) RestartJob(
	ctx context.Context, req *master.RestartJobRequest,
) (resp *master.RestartJobResponse, err error) {
	return c.MasterController.RestartJob(ctx, req)
}

func (c GrpcMasterController) DescribeJobs(
	ctx context.Context, req *master.DescribeJobsRequest,
) (resp *master.DescribeJobsResponse, err error) {
	return c.MasterController.DescribeJobs(ctx, req)
}

func (c GrpcMasterController) DescribeJob(
	ctx context.Context, req *master.DescribeJobRequest,
) (resp *master.DescribeJobResponse, err error) {
	return c.MasterController.DescribeJob(ctx, req)
}

func (c GrpcMasterController) DeleteJobs(
	ctx context.Context, req *master.DeleteJobsRequest,
) (resp *master.DeleteJobsResponse, err error) {
	return c.MasterController.DeleteJobs(ctx, req)
}

func (c GrpcMasterController) CancelJobs(
	ctx context.Context, req *master.CancelJobsRequest,
) (resp *master.CancelJobsResponse, err error) {
	return c.MasterController.CancelJobs(ctx, req)
}

func (c GrpcMasterController) UpdateJob(
	ctx context.Context, req *master.UpdateJobRequest,
) (resp *master.UpdateJobResponse, err error) {
	return c.MasterController.UpdateJob(ctx, req)
}

func (c GrpcMasterController) NotifyJobs(
	ctx context.Context, req *master.NotifyJobsRequest,
) (resp *master.NotifyJobsResponse, err error) {
	return c.MasterController.NotifyJobs(ctx, req)
}
