package adapter

import (
	"testing"

	"git.woa.com/yt-industry-ai/poseidon/types"
	"gotest.tools/assert"
)

func TestMultipleHomoPolicy_case1(t *testing.T) {
	devices := types.NewDeviceSet([]*types.Device{
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
	}...)

	p := NewMultipleHomoPolicy()
	res := p.Allocate(devices, 1)
	assert.Equal(t, 1, len(res))
	t.Log(res)

	res = p.Allocate(devices, 2)
	assert.Equal(t, 2, len(res))
	t.Log(res)

	res = p.Allocate(devices, 3)
	assert.Equal(t, 0, len(res))
	t.Log(res)
}

func TestMultipleHomoPolicy_case2(t *testing.T) {
	devices := types.NewDeviceSet([]*types.Device{
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
		{
			Ip:    "127.0.0.2",
			Label: nil,
			GpuId: 1,
		},
	}...)

	p := NewMultipleHomoPolicy()
	res := p.Allocate(devices, 1)
	assert.Equal(t, 1, len(res))
	t.Log(res)

	res = p.Allocate(devices, 2)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, true, res[0].Ip == res[1].Ip)
	assert.Equal(t, true, res[0].GpuId != res[1].GpuId)
	t.Log(res)

	res = p.Allocate(devices, 3)
	assert.Equal(t, 4, len(res))
	assert.Equal(t, true, res[0].Ip == res[1].Ip)
	assert.Equal(t, true, res[0].GpuId != res[1].GpuId)
	assert.Equal(t, true, res[2].Ip == res[3].Ip)
	assert.Equal(t, true, res[2].GpuId != res[3].GpuId)
	t.Log(res)
}

func TestMultipleHomoPolicy_case3(t *testing.T) {
	devices := types.NewDeviceSet([]*types.Device{
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

	p := NewMultipleHomoPolicy()
	res := p.Allocate(devices, 1)
	assert.Equal(t, 1, len(res))
	t.Log(res)

	res = p.Allocate(devices, 2)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, true, res[0].Ip == res[1].Ip)
	assert.Equal(t, true, res[0].GpuId != res[1].GpuId)
	t.Log(res)

	res = p.Allocate(devices, 3)
	assert.Equal(t, 0, len(res))
	t.Log(res)
}
