package taiji

import (
	"context"
	"fmt"
	"testing"
	"time"

	"git.woa.com/yt-industry-ai/grpc-go-ext/log/writer"
	taiji_pb "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/taiji"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	agent_model "git.woa.com/yt-industry-ai/poseidon/internal/agent/model"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/http"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	"github.com/stretchr/testify/assert"
)

var defaultInfo = &taiji_pb.TaijiTaskInfo{
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
	EnvVarsDict: map[string]string{
		"ENTRY_CODE_DIR": "/apdcephfs/private_jonyao/algorithm",
	},
	StartCmd: "sh /apdcephfs/private_jonyao/algorithm/train.sh " +
		"/apdcephfs/private_jonyao/algorithm/example_config/train_config.json",
}

type JobParser struct{}

func (x JobParser) GetTaijiTaskInfo(ctx context.Context, job *types.Job) (
	info *taiji_pb.TaijiTaskInfo, err error) {
	return defaultInfo, nil
}

func init() {
	writer.InstallLogOrDieForTest()
}

func TestStartTaijiTask(t *testing.T) {
	client, _ := http.New(http.WithTimeout(time.Second * 5))
	a := &TaijiHandler{
		opt: &Option{
			JobParsers:     map[int32]TaijiJobParser{1: JobParser{}},
			FixedDatasetId: "f06a5cc1127144de9cf3ff9bdbe77a1a", // 使用空数据集
			FixedCodePkgId: "899688f37f437129017ffdc96fc63038", // 使用空代码包
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
	fmt.Printf("instance url: http://taiji.oa.com/#/project-list/jizhi/task-inst-detail/%s\n", defaultInfo.GetInstanceId())
}

func TestDescribeTaijiTask(t *testing.T) {
	defaultInfo.TaskId = "34682094-b588-11ec-8c33-525400aaf075"

	client, _ := http.New(http.WithTimeout(time.Second * 5))
	a := &TaijiHandler{
		opt: &Option{
			JobParsers: map[int32]TaijiJobParser{1: JobParser{}},
		},
		client: client,
	}

	job := &model.Job{
		JobType: 1,
	}

	ctx, _ := common.NewTestContext("project", "uin")

	rspJob, _, err := a.DescribeJob(ctx, job, agent_model.NewContainerJob())
	if err != nil {
		assert.Nil(t, err, "err: %v", err)
	}
	t.Logf("%+v", rspJob)

	fmt.Printf("IsSuccess: %v\n", defaultInfo.IsSuccess)
	fmt.Printf("State: %s\n", defaultInfo.State)
	fmt.Printf("Msg: %s\n", defaultInfo.Msg)
	fmt.Printf("JobStatus: %s\n", rspJob.JobStatus)
}

func TestStopTaijiTask(t *testing.T) {
	defaultInfo.TaskId = "424c132d5ae5ba5c91c6ce39d25d6fb0"

	client, _ := http.New(http.WithTimeout(time.Second * 5))
	a := &TaijiHandler{
		opt: &Option{
			JobParsers: map[int32]TaijiJobParser{1: JobParser{}},
		},
		client: client,
	}

	job := &model.Job{
		JobType: 1,
	}

	ctx, _ := common.NewTestContext("project", "uin")

	retry, err := a.StopJob(ctx, job, agent_model.NewContainerJob())
	assert.Nil(t, err, "err: %v", err)
	assert.True(t, retry)
}

func TestDescribeResource(t *testing.T) {
	adapter := NewTaijiAdapter(&Option{
		DB:             nil,
		DBTimeout:      0,
		FixedDatasetId: "",
		FixedCodePkgId: "",
	})
	ctx, _ := common.NewTestContext("project", "uin")
	rspData, err := adapter.DescribeResource(ctx, "2uFZxwDR6QxnWAgvz8MLUA", "youtu_dev_test")
	assert.Nil(t, err, "err: %v", err)
	fmt.Printf("rsp: %v", rspData)
}
