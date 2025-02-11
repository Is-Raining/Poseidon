package adapter

import (
	"context"
	"sort"
	"strings"

	pb_local "git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/adapter/local"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	types_ "git.woa.com/yt-industry-ai/poseidon/types"
)

// 获取本地集群信息，节点状态从缓存中读取
func (x *LocalAdapter) DescribeNodes(
	ctx context.Context, req *pb_local.DescribeNodesRequest,
) (resp *pb_local.DescribeNodesResponse, err error) {
	resp = &pb_local.DescribeNodesResponse{}

	gather, err := x.Gather(ctx)
	if err != nil {
		return resp, err
	}

	free := make(map[string]*types.Resource)
	for _, device := range gather.FreeGpus {
		if _, ok := free[device.Ip]; !ok {
			free[device.Ip] = &types.Resource{
				Ip:           device.Ip,
				GpuType:      device.GetGpuType(),
				ResourceType: types.ResourceType_RESOURCE_TYPE_LOCAL,
			}
		}
		node := free[device.Ip]
		node.Gpus = append(node.Gpus, device.GpuId)
	}

	total := make(map[string]*types.Resource)
	for _, device := range gather.TotalGpus {
		if _, ok := total[device.Ip]; !ok {
			total[device.Ip] = &types.Resource{
				Ip:           device.Ip,
				GpuType:      device.GetGpuType(),
				ResourceType: types.ResourceType_RESOURCE_TYPE_LOCAL,
			}
		}
		total[device.Ip].Gpus = append(total[device.Ip].Gpus, device.GpuId)
	}

	nodes := make(map[string]*types.Node)
	for ip := range free { // free gpu
		if _, ok := nodes[ip]; !ok {
			nodes[ip] = &types.Node{Ip: ip}
		}
		nodes[ip].FreeResource = free[ip]
	}
	for ip := range total { // total gpu
		if _, ok := nodes[ip]; !ok {
			nodes[ip] = &types.Node{Ip: ip}
		}
		nodes[ip].TotalResource = total[ip]
	}
	for _, node := range gather.TotalNodes { // cpu
		if _, ok := nodes[node.Ip]; !ok {
			nodes[node.Ip] = &types.Node{Ip: node.Ip}
		}
	}

	resp.Nodes = make([]*types.Node, 0)
	for ip := range nodes {
		if stat, ok := x.NodeUsage(ip); ok {
			nodes[ip].DiskUsageStat = stat
		}
		resp.Nodes = append(resp.Nodes, nodes[ip])
	}
	sort.Slice(resp.Nodes, func(i, j int) bool {
		return strings.Compare(resp.Nodes[i].Ip, resp.Nodes[j].Ip) < 0
	})

	return resp, nil
}

// 获取总体已使用资源，每个项目已使用资源信息
func (x *LocalAdapter) DescribeUsedResources(
	ctx context.Context, req *pb_local.DescribeUsedResourcesRequest,
) (resp *pb_local.DescribeUsedResourcesResponse, err error) {
	resp = &pb_local.DescribeUsedResourcesResponse{}

	_, totalGpus, err := x.GetDevices(ctx)
	if err != nil {
		return resp, err
	}

	jobs, err := x.jobDao.GetJobs(ctx, []string{model.ColJobAdapterType}, []string{model.ColJobJobStatus}, nil,
		map[string]interface{}{
			model.ColJobJobStatus:   types.GetResourceConsumingJobStatuses().EnumInt32s(),
			model.ColJobAdapterType: LocalAdapterType,
		},
	)
	if err != nil {
		return nil, err
	}

	projects := make(map[string]*types.Resources)
	nodes := make(map[string]*types.Resource)
	for _, job := range jobs {
		for _, resource := range job.GetResources() {
			if len(resource.GetGpus()) < 1 {
				continue
			}
			missing := false
			for _, gpuId := range resource.GetGpus() {
				device := types_.NewDevice(resource.GetIp(), gpuId)
				if !totalGpus.Contains(device) {
					missing = true
					break
				}
			}
			if missing {
				continue
			}

			if _, ok := projects[job.ProjectId]; !ok {
				projects[job.ProjectId] = &types.Resources{
					Resources: map[string]*types.Resource{},
				}
			}
			project := projects[job.ProjectId].GetResources()
			if _, ok := project[resource.GetGpuType()]; !ok {
				project[resource.GetGpuType()] = &types.Resource{}
			}
			ip := resource.GetIp()
			if _, ok := nodes[ip]; !ok {
				nodes[ip] = &types.Resource{}
			}

			p := project[resource.GetGpuType()]
			p.GpuType = resource.GetGpuType()
			p.Gpus = append(p.Gpus, resource.GetGpus()...)

			n := nodes[ip]
			n.Ip = resource.GetIp()
			n.GpuType = resource.GetGpuType()
			n.Gpus = append(n.Gpus, resource.GetGpus()...)
		}
	}
	resp.Projects = projects
	resp.Nodes = nodes

	return resp, nil
}
