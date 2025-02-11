package timaker

import (
	"context"
	"fmt"
	"testing"

	"git.woa.com/yt-industry-ai/grpc-go-ext/log/writer"
	timaker_pb "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/timaker"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	agent_model "git.woa.com/yt-industry-ai/poseidon/internal/agent/model"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/http"
	"github.com/stretchr/testify/assert"
)

var defaultInfo = &timaker_pb.TimakerTaskInfo{
	SecretId:  "16111e9bb6ca4708abb0b4db2f",
	SecretKey: "fd46f3cb84c141ffa52dd9c8d6",
	Addr:      "http://1.116.68.51:31425",
	Host:      "1.116.68.51",
	Uin:       "superadmin",
	ProjectId: 1,
	TaskName:  "dlgghaotest6",
	ImageName: "mirrors.tencent.com/ddys/dlgghao:latest",
}

type JobParser struct{}

func (x JobParser) GetTimakerTaskInfo(ctx context.Context, job *types.Job) (
	info *timaker_pb.TimakerTaskInfo, err error) {
	return defaultInfo, nil
}

func init() {
	writer.InstallLogOrDieForTest()
}

func TestStartTimakerTask(t *testing.T) {
	client, _ := http.New(http.WithTimeout(timeout))
	a := &TimakerHandler{
		opt: &Option{
			JobParsers: map[int32]TimakerJobParser{1: JobParser{}},
		},
		client: client,
	}

	job := &model.Job{
		JobType: 1,
	}

	ctx, _ := common.NewTestContext("project", "uin")

	trainingJobId, _, err := a.StartJob(ctx, job, agent_model.NewContainerJob())
	assert.Nil(t, err, "err: %v", err)

	t.Logf("task_id: %s", trainingJobId)
	fmt.Printf("%v\n", defaultInfo)
}

func TestDescribeTimakerTask(t *testing.T) {
	client, _ := http.New(http.WithTimeout(timeout))
	a := &TimakerHandler{
		opt: &Option{
			JobParsers: map[int32]TimakerJobParser{1: JobParser{}},
		},
		client: client,
	}

	job := &model.Job{
		JobType:       1,
		ExternalJobId: "timaker-kkpnombzuu",
	}

	ctx, _ := common.NewTestContext("project", "uin")

	rspJob, _, err := a.DescribeJob(ctx, job, agent_model.NewContainerJob())

	assert.Nil(t, err, "err: %v", err)
	fmt.Printf("State: %s\n", defaultInfo.GetState())
	fmt.Printf("Msg: %s\n", defaultInfo.GetMsg())
	if rspJob != nil {
		fmt.Printf("JobStatus: %s\n", rspJob.JobStatus)
	}
}

func TestStopTimakerTask(t *testing.T) {
	client, _ := http.New(http.WithTimeout(timeout))
	a := &TimakerHandler{
		opt: &Option{
			JobParsers: map[int32]TimakerJobParser{1: JobParser{}},
		},
		client: client,
	}

	job := &model.Job{
		JobType:       1,
		ExternalJobId: "timaker-kkpnombzuu",
	}

	ctx, _ := common.NewTestContext("project", "uin")

	_, err := a.StopJob(ctx, job, agent_model.NewContainerJob())
	assert.Nil(t, err, "err: %v", err)
}

func TestDescribeResource(t *testing.T) {
	a := NewTimakerAdapter(&Option{
		JobParsers: map[int32]TimakerJobParser{1: JobParser{}},
	})

	ctx, _ := common.NewTestContext("project", "uin")

	detail, err := a.DescribeResource(
		ctx, 1, defaultInfo.Host, defaultInfo.Addr, defaultInfo.Uin,
		defaultInfo.SecretId, defaultInfo.SecretKey,
	)
	assert.Nil(t, err, "err: %v", err)
	t.Log(detail)
}
