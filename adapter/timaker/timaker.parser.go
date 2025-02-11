package timaker

import (
	"context"

	pb_timaker "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/timaker"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
)

// timaker 任务解析器
type TimakerJobParser interface {
	// 将 Poseidon 任务转换为 timaker 任务的参数
	// job: 任务元数据
	// info: timaker 任务参数
	GetTimakerTaskInfo(ctx context.Context, job *types.Job) (info *pb_timaker.TimakerTaskInfo, err error)
}
