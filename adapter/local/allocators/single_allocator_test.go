package adapter

import (
	"testing"

	"git.woa.com/yt-industry-ai/poseidon/types"
)

func TestSinglePolicy(t *testing.T) {
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

	p := NewSinglePolicy()

	res := p.Allocate(devices1, 2)
	if len(res) != 0 {
		t.Error("allocate error")
		return
	}
	t.Log(res)

	res = p.Allocate(devices1, 1)
	if len(res) != 1 {
		t.Error("allocate error")
		return
	}
	t.Log(res)

	res = p.Allocate(devices2, 2)
	if len(res) != 2 {
		t.Error("allocate error")
		return
	}
	t.Log(res)
}
