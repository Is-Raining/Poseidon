package adapter

import (
	"testing"

	"git.woa.com/yt-industry-ai/poseidon/types"
	"gotest.tools/assert"
)

func TestMultiplePolicy(t *testing.T) {
	devices1 := types.NewDeviceSet([]*types.Device{
		{
			Ip:    "127.0.0.1",
			Label: nil,
			GpuId: 0,
		},
		{
			Ip:    "127.0.0.2",
			Label: nil,
			GpuId: 0,
		},
	}...)
	devices2 := types.NewDeviceSet([]*types.Device{
		{
			Ip:    "127.0.0.1",
			Label: nil,
			GpuId: 0,
		},
		{
			Ip:    "127.0.0.1",
			Label: nil,
			GpuId: 1,
		},
		{
			Ip:    "127.0.0.2",
			Label: nil,
			GpuId: 0,
		},
	}...)

	p := NewMultiplePolicy()

	res := p.Allocate(devices1, 2)
	assert.Equal(t, 2, len(res))
	t.Log(res)

	res = p.Allocate(devices1, 1)
	assert.Equal(t, 1, len(res))
	t.Log(res)

	res = p.Allocate(devices2, 3)
	assert.Equal(t, 3, len(res))
	t.Log(res)
}
