package adapter

import (
	"git.woa.com/yt-industry-ai/poseidon/types"
)

type multiplePolicy struct{}

// NewMultiplePolicy creates a new SimplePolicy.
func NewMultiplePolicy() Policy {
	return &multiplePolicy{}
}

// Allocate GPUs following a simple policy.
func (p *multiplePolicy) Allocate(availableSet types.DeviceSet, size int) types.Devices {
	if size <= 0 {
		return []*types.Device{}
	}

	if len(availableSet.SortedSlice()) < size {
		return []*types.Device{}
	}

	allocated := append([]*types.Device{}, availableSet.SortedSlice()[:size]...)
	return allocated
}
