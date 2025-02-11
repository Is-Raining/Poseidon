package taiji

import (
	"context"

	pb_taiji "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/taiji"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
)

// 太极任务解析器
type TaijiJobParser interface {
	// 将 Poseidon 任务转换为太极任务的参数
	// job: 任务元数据
	// info: 太极任务参数
	GetTaijiTaskInfo(ctx context.Context, job *types.Job) (info *pb_taiji.TaijiTaskInfo, err error)
}
