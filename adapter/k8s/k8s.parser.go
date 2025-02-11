package k8s

import (
	"context"

	pb_k8s "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/k8s"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
)

// k8s 任务解析器
type K8SJobParser interface {
	// 将 Poseidon 任务转换为 k8s 任务的参数
	// job: 任务元数据
	// info: k8s 任务参数
	GetK8STaskInfo(ctx context.Context, job *types.Job) (info *pb_k8s.KubernetesTaskInfo, err error)
}
