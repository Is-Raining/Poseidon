package adapter

import (
	"sort"

	"git.woa.com/yt-industry-ai/poseidon/types"
)

type multipleHomoPolicy struct{}

// 多机同构（每机上 GPU 个数相同）
func NewMultipleHomoPolicy() Policy {
	return &multipleHomoPolicy{}
}

// Allocate GPUs following a simple policy.
func (p *multipleHomoPolicy) Allocate(availableSet types.DeviceSet, size int) types.Devices {
	if size <= 0 {
		// 需要 GPU 个数为 0
		return []*types.Device{}
	}

	available := availableSet.SortedSlice()
	if len(available) < size {
		// 空闲的 GPU 个数不足，分配失败
		return []*types.Device{}
	}

	hostNums := map[int]int{}                // 单个节点上的 GPU 个数 -> 这样的节点的个数
	hostGpus := map[string][]*types.Device{} // 节点 IP -> 该节点上的 GPU 列表
	for _, d := range available {
		hostGpus[d.Ip] = append(hostGpus[d.Ip], d)
	}
	for _, devices := range hostGpus {
		hostNums[len(devices)] += 1
	}
	gpuNums := []int{} // gpu 个数的集合
	for gpuNum := range hostNums {
		gpuNums = append(gpuNums, gpuNum)
	}
	sort.Ints(gpuNums)
	gpuNum := 0 // 单个节点的 GPU 个数
	for i := len(gpuNums) - 1; i >= 0; i -= 1 {
		// 单机 GPU 个数尽可能大，以提升训练效率
		gpuNum = gpuNums[i]
		hostNum := hostNums[gpuNum]
		if hostNum*gpuNum >= size {
			// 找到满足要求的单机 GPU 个数
			break
		}
		if i > 0 {
			hostNums[gpuNums[i-1]] += hostNum // 把当前的节点累加到下一个 GPU 数量的节点数上
		}
	}

	hosts := []string{} // 可选择的节点列表，按 GPU 个数从小到大排序
	for ip, devices := range hostGpus {
		if len(devices) >= gpuNum {
			hosts = append(hosts, ip)
		}
	}
	sort.Slice(hosts, func(i, j int) bool {
		return len(hostGpus[hosts[i]]) < len(hostGpus[hosts[j]])
	})

	allocated := []*types.Device{}
	for _, ip := range hosts {
		if len(allocated) >= size {
			break
		}
		n := gpuNum
		if size-len(allocated) < gpuNum && len(allocated) == 0 {
			n = size - len(allocated)
		}
		allocated = append(allocated, hostGpus[ip][:n]...)
	}
	if len(allocated) < size {
		return []*types.Device{}
	}
	return allocated
}
