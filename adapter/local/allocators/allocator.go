package adapter

import "git.woa.com/yt-industry-ai/poseidon/types"

// Policy 资源分配策略
type Policy interface {
	Allocate(available types.DeviceSet, size int) types.Devices
}
