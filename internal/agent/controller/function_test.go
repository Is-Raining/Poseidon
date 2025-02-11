package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"git.woa.com/yt-industry-ai/grpc-go-ext/log/writer"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/agent"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/internal/agent/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
	"github.com/stretchr/testify/suite"
)

func init() {
	writer.InstallLogOrDieForTest()
}

type mockFunction struct {
	ctx    context.Context
	cancel context.CancelFunc

	count int
}

func (f *mockFunction) Initialize(ctx context.Context, jobCtx *types_.JobContext) error {
	return nil
}

func (f *mockFunction) Start(ctx context.Context, jobCtx *types_.JobContext) error {
	for {
		select {
		case <-time.After(time.Second):
			fmt.Printf("mock: function job running\n")
			if f.count > 2 {
				switch jobCtx.JobId {
				case "fail":
					return common.Errorf(ctx, code.Code_InvalidParameterValue__TooLarge)
				case "success":
					return nil
				case "panic":
					panic("an error occurred during rule execution")
				}
			}
			f.count++
		case <-f.ctx.Done():
			return nil
		}
	}
}

func (f *mockFunction) Stop(ctx context.Context) error {
	f.cancel()
	return nil
}

func (f *mockFunction) Clean(ctx context.Context) error {
	return nil
}

type FunctionTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	srv *Controller
}

func (s *FunctionTestSuite) SetupSuite() {
	s.ctx, s.cancel = common.NewTestContext("test-proj", "test-uin")

	types_.MustRegisterFunction(1, func() types_.Function {
		ctx, cancel := context.WithCancel(context.Background())
		return &mockFunction{
			ctx:    ctx,
			cancel: cancel,
		}
	})
	s.srv = &Controller{
		opt: Option{
			JobDir: "/tmp/jobs",
			LogDir: "/tmp/logs",
		},
		functions: &model.Cache[string, *model.FunctionJob]{},
	}
}

func (s *FunctionTestSuite) TearDownSuite() {
	s.cancel()
}

func TestFunctionTestSuite(t *testing.T) {
	suite.Run(t, new(FunctionTestSuite))
}

func (s *FunctionTestSuite) TestDescribeNode() {
	resp, err := s.srv.DescribeNode(s.ctx, &agent.DescribeNodeRequest{
		RequestId: utils.NewUUID(),
		Path:      "/tmp",
	})
	s.T().Log(resp.GetNode())
	s.Suite.Nil(err)
}

func (s *FunctionTestSuite) TestFail() {
	_, err := s.srv.StartJob(s.ctx, &agent.StartJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "fail",
		JobType:   1,
		Resources: []*types.Resource{{Ip: "127.0.0.1"}},
		Args:      "{}",
	})
	s.Suite.Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		_, err = s.srv.StopJob(s.ctx, &agent.StopJobRequest{
			RequestId: utils.NewUUID(),
			JobId:     "fail",
			JobType:   1,
		})
		s.Suite.Nil(err)

		defer cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			s.T().Log(ctx.Err())
			s.T().Error("expect fail, but context deadline exceeded")
			return
		case <-time.After(time.Second):
			resp, err := s.srv.DescribeJob(s.ctx, &agent.DescribeJobRequest{
				RequestId: utils.NewUUID(),
				JobId:     "fail",
				JobType:   1,
			})
			s.T().Log(resp)
			s.Suite.Nil(err)
			if resp.GetJobStatus() == types.JobStatus_JOB_STATUS_FAILED {
				s.Suite.Equal("InvalidParameterValue.TooLarge", resp.GetJobErr().GetCode())
				s.Suite.Nil(resp.GetError())
				return
			}
		}
	}
}

func (s *FunctionTestSuite) TestCancel() {
	_, err := s.srv.StartJob(s.ctx, &agent.StartJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
		Resources: []*types.Resource{{Ip: "127.0.0.1"}},
		Args:      "{}",
	})
	s.Suite.Nil(err)

	time.Sleep(time.Second)

	resp, err := s.srv.DescribeJob(s.ctx, &agent.DescribeJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
	})
	s.T().Log(resp)
	s.Suite.Nil(err)

	_, err = s.srv.StopJob(s.ctx, &agent.StopJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
	})
	s.Suite.Nil(err)

	resp, err = s.srv.DescribeJob(s.ctx, &agent.DescribeJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
	})
	s.T().Log(resp)
	s.Suite.Nil(err)
	s.Suite.Equal("ResourceNotFound.JobNotExist", resp.GetError().Code)
}

func (s *FunctionTestSuite) TestSuccess() {
	_, err := s.srv.StartJob(s.ctx, &agent.StartJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
		Resources: []*types.Resource{{Ip: "127.0.0.1"}},
		Args:      "{}",
	})
	s.Suite.Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		_, err = s.srv.StopJob(s.ctx, &agent.StopJobRequest{
			RequestId: utils.NewUUID(),
			JobId:     "success",
			JobType:   1,
		})
		s.Suite.Nil(err)

		defer cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			s.T().Log(ctx.Err())
			s.T().Error("expect success, but context deadline exceeded")
			return
		case <-time.After(time.Second):
			resp, err := s.srv.DescribeJob(s.ctx, &agent.DescribeJobRequest{
				RequestId: utils.NewUUID(),
				JobId:     "success",
				JobType:   1,
			})
			s.T().Log(resp)
			s.Suite.Nil(err)
			if resp.GetJobStatus() == types.JobStatus_JOB_STATUS_SUCCESS {
				s.Suite.Nil(resp.GetError())
				s.Suite.Nil(resp.GetError())
				return
			}
		}
	}
}

func (s *FunctionTestSuite) TestPanic() {
	_, err := s.srv.StartJob(s.ctx, &agent.StartJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "panic",
		JobType:   1,
		Resources: []*types.Resource{{Ip: "127.0.0.1"}},
		Args:      "{}",
	})
	s.Suite.Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		_, err = s.srv.StopJob(s.ctx, &agent.StopJobRequest{
			RequestId: utils.NewUUID(),
			JobId:     "panic",
			JobType:   1,
		})
		s.Suite.Nil(err)

		defer cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			s.T().Log(ctx.Err())
			s.T().Error("expect panic, but context deadline exceeded")
			return
		case <-time.After(time.Second):
			resp, err := s.srv.DescribeJob(s.ctx, &agent.DescribeJobRequest{
				RequestId: utils.NewUUID(),
				JobId:     "panic",
				JobType:   1,
			})
			s.T().Log(resp)
			s.Suite.Nil(err)
			if resp.GetJobStatus() == types.JobStatus_JOB_STATUS_FAILED {
				s.Suite.Equal("InternalError.RunJobPanic", resp.GetError().Code)
				return
			}
		}
	}
}

func (s *FunctionTestSuite) TestAbnormal() {
	_, err := s.srv.StartJob(s.ctx, &agent.StartJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
		Resources: []*types.Resource{{Ip: "127.0.0.1"}},
		Args:      "{}",
	})
	s.Suite.Nil(err)

	_, err = s.srv.StartJob(s.ctx, &agent.StartJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
		Resources: []*types.Resource{{Ip: "127.0.0.1"}},
		Args:      "{}",
	})
	s.Suite.Nil(err)

	time.Sleep(time.Second)

	_, err = s.srv.StopJob(s.ctx, &agent.StopJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
	})
	s.Suite.Nil(err)

	_, err = s.srv.StopJob(s.ctx, &agent.StopJobRequest{
		RequestId: utils.NewUUID(),
		JobId:     "success",
		JobType:   1,
	})
	s.Suite.Nil(err)
}
