package adapter

import (
	"git.woa.com/yt-industry-ai/poseidon/types"
)

type singlePolicy struct{}

// NewSinglePolicy creates a new SimplePolicy.
func NewSinglePolicy() Policy {
	return &singlePolicy{}
}

// Allocate GPUs following a simple policy.
func (p *singlePolicy) Allocate(availableSet types.DeviceSet, size int) types.Devices {
	if size <= 0 {
		return []*types.Device{}
	}

	if len(availableSet.SortedSlice()) < size {
		return []*types.Device{}
	}

	bgn, end := 0, 0
	available := availableSet.SortedSlice()
	for i := 0; i < len(available); i++ {
		if available[bgn].Ip != available[i].Ip {
			bgn = i
			end = i
			continue
		}
		end = i
		if (end-bgn)+1 >= size {
			break
		}
	}
	if (end-bgn)+1 < size {
		return []*types.Device{}
	}

	allocated := append([]*types.Device{}, available[bgn:end+1]...)
	return allocated
}
