package adapter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"git.woa.com/yt-industry-ai/grpc-go-ext/log/writer"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
)

func init() {
	writer.InstallLogOrDieForTest()
}

type projectManager struct {
}

func (p projectManager) GetQuotas(_ context.Context) (map[string]map[string]int, error) {
	return map[string]map[string]int{
		"test-proj": {"t4": 0, "v100": 8},
	}, nil
}

type LocalAdapterTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc
	adp    *LocalAdapter
	db     *sqlx.DB
}

func (s *LocalAdapterTestSuite) SetupSuite() {
	s.db = utils.NewDBFromEnv()
	s.ctx, s.cancel = common.NewTestContext("test-proj", "test-uin")

	// if _, err := s.db.Exec("truncate table t_schedule_job"); err != nil {
	// s.Suite.T().Error(err)
	// }

	devices := []*types_.Device{}
	// devices = append(devices,
	// &types_.Device{Ip: "127.0.0.1", GpuId: 0, Label: types_.Label{types_.ResourceLabelGpuType: []string{"v100"}}},
	// &types_.Device{Ip: "127.0.0.1", GpuId: 1, Label: types_.Label{types_.ResourceLabelGpuType: []string{"v100"}}},
	// &types_.Device{Ip: "127.0.0.1", GpuId: 2, Label: types_.Label{types_.ResourceLabelGpuType: []string{"v100"}}},
	// &types_.Device{Ip: "127.0.0.1", GpuId: 3, Label: types_.Label{types_.ResourceLabelGpuType: []string{"v100"}}},
	// &types_.Device{Ip: "127.0.0.1", GpuId: 4, Label: types_.Label{types_.ResourceLabelGpuType: []string{"v100"}}},
	// &types_.Device{Ip: "127.0.0.1", GpuId: 5, Label: types_.Label{types_.ResourceLabelGpuType: []string{"v100"}}},
	// &types_.Device{Ip: "127.0.0.1", GpuId: 6, Label: types_.Label{types_.ResourceLabelGpuType: []string{"v100"}}},
	// &types_.Device{Ip: "127.0.0.1", GpuId: 7, Label: types_.Label{types_.ResourceLabelGpuType: []string{"v100"}}},
	// &types_.Device{Ip: "127.0.0.1", GpuId: -1},
	// )

	// jobDao := dao.NewJobDao(s.db, nil)
	// for _, job := range dao.MockJobs {
	// job.ProjectId = "test-proj"
	// job.Creator = "test-uin"
	// job.Language = "zh-CN"
	// err := jobDao.AddJob(s.ctx, job)
	// s.Suite.Nil(err)
	// }
	s.adp = NewLocalAdapter(
		&Option{
			DB:                  s.db,
			DBTimeout:           2 * time.Second,
			ProjectManager:      &projectManager{},
			Devices:             devices,
			FunctionJobLimiting: map[int32]int{1: 2},
		},
	)
}

func (s *LocalAdapterTestSuite) TearDownSuite() {
	s.cancel()
}

func TestLocalAdapterTestSuite(t *testing.T) {
	suite.Run(t, new(LocalAdapterTestSuite))
}

// func (s *LocalAdapterTestSuite) TestGather() {
// gather, err := s.adp.Gather(s.ctx)
// s.Suite.Nil(err)
// s.Suite.Equal(10, len(gather.Jobs))
// s.Suite.Len(gather.FreeGpus, 1)
// s.Suite.Len(gather.UsedGpus, 7)
// s.Suite.Len(gather.TotalGpus, 8)
// s.T().Log(gather.ProjectCounting)
// s.T().Log(gather.ProjectQuota)

// s.T().Log(gather.TotalNodes)
// s.T().Log(gather.FunctionJobCounting)

// for _, job := range gather.Jobs {
// if job.JobId == "running-other" {
// s.Suite.Equal(types.JobStatus_JOB_STATUS_STOPPING, job.JobStatus)
// }
// }
// }

// func (s *LocalAdapterTestSuite) TestGetDevices() {
// nodes, gpus, err := s.adp.GetDevices(s.ctx)
// s.Suite.Nil(err)
// s.Suite.Equal(1, len(nodes))
// s.Suite.Equal(8, len(gpus))
// s.T().Log(nodes)
// s.T().Log(gpus)
// }

func (s *LocalAdapterTestSuite) TestAllocateGPUSingle() {
	freeDevices := []*types_.Device{}
	freeDevices = append(freeDevices, genGpus("127.0.0.1", "v100", []int32{2, 3}, []int32{1})...)
	freeDevices = append(freeDevices, genGpus("127.0.0.2", "t4", []int32{1, 2, 3}, []int32{1})...)

	usedDevices := []*types_.Device{}
	usedDevices = append(usedDevices, genGpus("127.0.0.1", "v100", []int32{0, 1}, []int32{1})...)
	usedDevices = append(usedDevices, genGpus("127.0.0.2", "t4", []int32{0}, []int32{1})...)

	totalDevices := append(freeDevices, usedDevices...)

	gather := &model.Gather{
		TotalGpus:       types_.NewDeviceSet(totalDevices...),
		FreeGpus:        types_.NewDeviceSet(freeDevices...),
		UsedGpus:        types_.NewDeviceSet(usedDevices...),
		ProjectQuota:    map[string]map[string]int{"test-proj": {"t4": 2, "v100": 3}},
		ProjectCounting: map[string]map[string]int{"test-proj": {"t4": 1, "v100": 1}},
	}

	job := &model.Job{
		ProjectId: "test-proj",
		JobId:     utils.NewUUID(),
		JobType:   1,
		ResourceRequire: &types.ResourceRequire{
			Label: map[string]string{
				types_.ScheduleLabelContainer: "",
				types_.ScheduleLabelAffinity:  types_.ScheduleLabelAffinitySingleHost,
				types_.ResourceLabelGpuType:   "v100",
			},
			GpuNum: 2,
		},
	}
	resources, err := s.adp.Allocate(s.ctx, job, gather)
	s.Suite.Nil(err)
	s.T().Log(resources)
	s.Suite.Equal(1, len(resources))
	s.Suite.Equal(2, len(resources[0].GetGpus()))
	s.Suite.Equal("v100", resources[0].GetGpuType())

	job.GetResourceRequire().GpuNum = 3
	_, err = s.adp.Allocate(s.ctx, job, gather)
	s.Suite.True(common.Match(err, code.Code_ResourceInsufficient__GPUInsufficient))

	job.GetResourceRequire().GpuNum = 1
	job.GetResourceRequire().Label = map[string]string{types_.ScheduleLabelContainer: ""} // 取消显卡类型限制
	resources, err = s.adp.Allocate(s.ctx, job, gather)
	s.Suite.Nil(err)
	s.T().Log(resources)
	s.Suite.Equal(1, len(resources))
	s.Suite.Equal("t4", resources[0].GetGpuType())

	job.GetResourceRequire().GpuNum = 1
	job.GetResourceRequire().Label = map[string]string{types_.ScheduleLabelContainer: ""} // 取消显卡类型限制
	_, err = s.adp.Allocate(s.ctx, job, gather)
	s.Suite.True(common.Match(err, code.Code_ResourceInsufficient__GPUQuotaInsufficient))
}

func (s *LocalAdapterTestSuite) TestAllocateGPUSingleHostMultiGpusWithQuota() {
	freeDevices := []*types_.Device{}
	freeDevices = append(freeDevices, genGpus("127.0.0.1", "v100", []int32{1}, []int32{1})...)
	freeDevices = append(freeDevices, genGpus("127.0.0.2", "v100", []int32{1, 2}, []int32{1})...)

	usedDevices := []*types_.Device{}

	totalDevices := append(freeDevices, usedDevices...)

	gather := &model.Gather{
		TotalGpus:       types_.NewDeviceSet(totalDevices...),
		FreeGpus:        types_.NewDeviceSet(freeDevices...),
		UsedGpus:        types_.NewDeviceSet(usedDevices...),
		ProjectQuota:    map[string]map[string]int{"test-proj": {"v100": 2}},
		ProjectCounting: map[string]map[string]int{"test-proj": {"v100": 0}},
	}

	job := &model.Job{
		ProjectId: "test-proj",
		JobId:     utils.NewUUID(),
		JobType:   1,
		ResourceRequire: &types.ResourceRequire{
			Label: map[string]string{
				types_.ScheduleLabelContainer: "",
				types_.ScheduleLabelAffinity:  types_.ScheduleLabelAffinitySingleHost,
				types_.ResourceLabelGpuType:   "v100",
			},
			GpuNum: 2,
		},
	}
	adp := &LocalAdapter{}
	resources, err := adp.Allocate(s.ctx, job, gather)
	s.Suite.Nil(err)
	s.T().Log(resources)
	s.Suite.Equal(1, len(resources))
	s.Suite.Equal(2, len(resources[0].GetGpus()))
	s.Suite.Equal("v100", resources[0].GetGpuType())
}

func (s *LocalAdapterTestSuite) TestAllocateGPUMulti() {
	freeDevices := []*types_.Device{}
	freeDevices = append(freeDevices, genGpus("127.0.0.1", "v100", []int32{2, 3}, []int32{1})...)
	freeDevices = append(freeDevices, genGpus("127.0.0.2", "t4", []int32{1, 2, 3}, []int32{1})...)

	usedDevices := []*types_.Device{}
	usedDevices = append(usedDevices, genGpus("127.0.0.1", "v100", []int32{0, 1}, []int32{1})...)
	usedDevices = append(usedDevices, genGpus("127.0.0.2", "t4", []int32{0}, []int32{1})...)

	totalDevices := append(freeDevices, usedDevices...)

	gather := &model.Gather{
		TotalGpus:       types_.NewDeviceSet(totalDevices...),
		FreeGpus:        types_.NewDeviceSet(freeDevices...),
		UsedGpus:        types_.NewDeviceSet(usedDevices...),
		ProjectQuota:    map[string]map[string]int{"test-proj": {"t4": 3, "v100": 2}},
		ProjectCounting: map[string]map[string]int{"test-proj": {"t4": 1, "v100": 1}},
	}

	job := &model.Job{
		ProjectId: "test-proj",
		JobId:     utils.NewUUID(),
		JobType:   1,
		ResourceRequire: &types.ResourceRequire{
			Label: map[string]string{
				types_.ScheduleLabelContainer: "",
				types_.ScheduleLabelAffinity:  types_.ScheduleLabelAffinityMultiHost,
			},
			GpuNum: 3,
		},
	}
	resources, err := s.adp.Allocate(s.ctx, job, gather)
	s.Suite.Nil(err)
	s.T().Log(resources)
	s.Suite.Equal(2, len(resources))
	for _, resource := range resources {
		if len(resource.Gpus) == 1 {
			s.Suite.Equal("v100", resource.GetGpuType())
		} else {
			s.Suite.Equal(2, len(resource.GetGpus()))
			s.Suite.Equal("t4", resource.GetGpuType())
		}
	}

	job.GetResourceRequire().GpuNum = 1
	_, err = s.adp.Allocate(s.ctx, job, gather)
	s.Suite.True(common.Match(err, code.Code_ResourceInsufficient__GPUQuotaInsufficient))
}

func (s *LocalAdapterTestSuite) TestAllocateCPU() {
	devices := []*types_.Device{}
	devices = append(devices, genCpus("127.0.0.1", map[string]string{"Master": ""}, []int32{1, 2}))
	devices = append(devices, genCpus("127.0.0.1", map[string]string{"Agent": ""}, []int32{1, 2}))

	gather := &model.Gather{
		TotalNodes:          types_.NewDeviceSet(devices...),
		FunctionJobLimiting: map[int32]int{1: 2, 2: 2},
		FunctionJobCounting: map[string]map[int32]int{},
	}

	job := &model.Job{
		JobId:   utils.NewUUID(),
		JobType: 1,
		ResourceRequire: &types.ResourceRequire{
			Label: map[string]string{
				types_.ScheduleLabelFastFail: "",
				"NOT-MATCHED":                "",
			},
		},
	}

	resource, err := s.adp.Allocate(s.ctx, job, gather)
	s.T().Log(resource)
	s.Suite.True(common.Match(err, code.Code_ResourceInsufficient__CPUInsufficient))

	job.GetResourceRequire().Label = map[string]string{types_.ScheduleLabelFastFail: ""}
	resource, err = s.adp.Allocate(s.ctx, job, gather)
	s.Suite.Nil(err)
	s.T().Log(resource)
	s.Suite.Equal(1, len(resource))

	resource, err = s.adp.Allocate(s.ctx, job, gather)
	s.Suite.Nil(err)
	s.T().Log(resource)
	s.Suite.Equal(1, len(resource))

	resource, err = s.adp.Allocate(s.ctx, job, gather)
	s.T().Log(resource)
	s.Suite.True(common.Match(err, code.Code_ResourceInsufficient__CPUInsufficient))

	job.JobType = 2
	resource, err = s.adp.Allocate(s.ctx, job, gather)
	s.Suite.Nil(err)
	s.T().Log(resource)
	s.Suite.Equal(1, len(resource))
}

func (s *LocalAdapterTestSuite) TestAllocatePort() {
	gather := &model.Gather{
		UsedPorts: map[string]map[int32]bool{
			"127.0.0.1": map[int32]bool{
				9091: true,
			},
			"127.0.0.2": map[int32]bool{
				9092: true,
				9093: true,
			},
		},
	}

	job := &model.Job{
		JobId:   utils.NewUUID(),
		JobType: 1,
		ResourceRequire: &types.ResourceRequire{
			PortNum: 2,
		},
	}

	resources := []*types.Resource{
		&types.Resource{
			Ip: "127.0.0.1",
		},
		&types.Resource{
			Ip: "127.0.0.2",
		},
	}

	s.adp.opt.PortRange = []int32{9090, 9095}
	err := s.adp.allocatePort(s.ctx, job, gather, resources)
	s.Suite.Nil(err)
	s.Suite.Equal(9090, int(resources[0].Ports[0]))
	s.Suite.Equal(9094, int(resources[0].Ports[1]))
	s.Suite.Equal(9090, int(resources[1].Ports[0]))
	s.Suite.Equal(9094, int(resources[1].Ports[1]))
	s.Suite.True(gather.UsedPorts["127.0.0.1"][9090])
	s.Suite.True(gather.UsedPorts["127.0.0.2"][9090])
	s.Suite.True(gather.UsedPorts["127.0.0.1"][9094])
	s.Suite.True(gather.UsedPorts["127.0.0.2"][9094])
	s.Suite.False(gather.UsedPorts["127.0.0.2"][9095])

	err = s.adp.allocatePort(s.ctx, job, gather, resources)
	s.Suite.NotNil(err)
}

func genGpus(ip string, gpuType string, gpus []int32, jobTypes []int32) []*types_.Device {
	devices := []*types_.Device{}
	jobTypeValues := []string{}
	for _, jobType := range jobTypes {
		jobTypeValues = append(jobTypeValues, fmt.Sprintf("%d", jobType))
	}
	for _, gpu := range gpus {
		devices = append(devices, &types_.Device{
			Ip:    ip,
			GpuId: gpu,
			Label: types_.DeviceLabel{
				types_.ResourceLabelGpuType: []string{gpuType},
				types_.ResourceLabelJobType: jobTypeValues,
			}},
		)
	}
	return devices
}

func genCpus(ip string, labels map[string]string, jobTypes []int32) *types_.Device {
	jobTypeValues := []string{}
	for _, jobType := range jobTypes {
		jobTypeValues = append(jobTypeValues, fmt.Sprintf("%d", jobType))
	}
	label := types_.DeviceLabel{
		types_.ResourceLabelJobType: jobTypeValues,
	}
	for k, v := range labels {
		label[k] = []string{v}
	}
	return &types_.Device{
		Ip:    ip,
		Label: label,
	}
}
