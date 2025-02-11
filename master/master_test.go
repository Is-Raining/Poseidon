package master_test

import (
	"context"
	"testing"
	"time"

	"git.woa.com/yt-industry-ai/grpc-go-ext/log/writer"
	taiji "git.woa.com/yt-industry-ai/poseidon/adapter/taiji"
	pb_taiji "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/taiji"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/master"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	poseidon "git.woa.com/yt-industry-ai/poseidon/master"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	psd_types "git.woa.com/yt-industry-ai/poseidon/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"
)

func init() {
	writer.InstallLogOrDieForTest()
}

var targetJobInfo *types.ReportJobInfo

type mockJobCallback struct {
}

func (m mockJobCallback) OnJobReported(
	ctx context.Context, jobId string, jobType int32, jobInfo *types.ReportJobInfo,
) error {
	if jobId == "running" {
		targetJobInfo = jobInfo
	}
	return nil
}

func (m mockJobCallback) OnJobStatusChanged(
	ctx context.Context, jobId string, jobStatus types.JobStatus, pbErr *types.Error,
) (restart bool, err error) {
	return false, nil
}

func (m mockJobCallback) OnJobExposeChanged(
	ctx context.Context, jobId, target string, jobStatus types.JobStatus,
) error {
	return nil
}

type PoseidonMasterServiceTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc
	db     *sqlx.DB
	srv    master.PoseidonMasterServiceServer
}

func (s *PoseidonMasterServiceTestSuite) SetupSuite() {
	s.db = utils.NewDBFromEnv()
	s.ctx, s.cancel = common.NewTestContext("test-proj", "test-uin")

	if _, err := s.db.Exec("truncate table t_schedule_job"); err != nil {
		s.Suite.T().Error(err)
	}

	psd_types.MustRegisterCallback(1, &mockJobCallback{})

	adapterOption := taiji.NewTaijiAdapterOption()
	adapterOption.DB = s.db
	adapterOption.DBTimeout = 5 * time.Second
	adapterOption.FixedCodePkgId = "899688f37f437129017ffdc96fc63038"
	adapterOption.FixedDatasetId = "f06a5cc1127144de9cf3ff9bdbe77a1a"

	option := poseidon.NewOption()
	option.DB = s.db
	option.DBTimeout = 5 * time.Second
	option.Adapters = append(option.Adapters, taiji.NewTaijiAdapter(adapterOption))

	s.srv = poseidon.NewGrpcController(option)
}

func (s *PoseidonMasterServiceTestSuite) TearDownSuite() {
	time.Sleep(10 * time.Minute)
	s.cancel()
}

func TestPoseidonMasterServiceTestSuite(t *testing.T) {
	suite.Run(t, new(PoseidonMasterServiceTestSuite))
}

func (s *PoseidonMasterServiceTestSuite) TestReportJob() {

}

func (s *PoseidonMasterServiceTestSuite) TestCreateJob() {
	taskId := "youtu_lowsource_chongqing95BD98401F694FA"
	//taskId := "34682094-b588-11ec-8c33-525400aaf075"

	info := &pb_taiji.TaijiTaskInfo{
		ApiToken:     "Sr1WUwAAZOeuujNC6an9dA",
		TaskName:     "test",
		TaskId:       utils.NewUID(),
		AppGroupName: "youtu_lowsource_chongqing",
		ImageName:    "mirrors.tencent.com/xlab/g-xlab-project-mmdet:mmcv-1.3.8",
		HostNum:      2,
		HostGpuNum:   2,
		GpuType:      "V100",
		CudaVersion:  "10.2",
		IsElasticity: true,
		//AlgoConfigPath: "/apdcephfs/private_jonyao/algorithm/example_config/train_config.json",
		//EntryCodeDir:   "/apdcephfs/private_jonyao/algorithm",
		//EntryScript:    "train.sh",
	}
	content, _ := protojson.Marshal(info)

	resp, err := s.srv.CreateJob(s.ctx, &master.CreateJobRequest{
		RequestId:       utils.NewUUID(),
		JobId:           taskId,
		JobType:         1,
		ResourceRequire: nil,
		Args:            string(content),
	})
	s.Suite.Nil(err)
	s.T().Log(resp)
}

func (s *PoseidonMasterServiceTestSuite) TestCancelJob() {

}

func (s *PoseidonMasterServiceTestSuite) TestRestartJob() {

}

func (s *PoseidonMasterServiceTestSuite) TestDescribeJobs() {

}

func (s *PoseidonMasterServiceTestSuite) TestDescribeJob() {

}

func (s *PoseidonMasterServiceTestSuite) TestDeleteJobs() {

}

func (s *PoseidonMasterServiceTestSuite) TestCancelJobs() {

}
