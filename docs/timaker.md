# timaker 集群对接

## 背景
timaker 是一个云原生深度学习训练平台调度器。它基于云原生 k8s 底座, 可在公有云/私有化环境部署, 
通过 HTTP API 对外提供了深度学习训练任务相关功能, 例如提交、查询任务, 可利用云原生集群中的 CPU/GPU 作为训练资源。

Poseidon 实现了对接 timaker 的外部集群适配器。
上层业务只需要增加少量代码, 即可完成接入 timaker, 通过调用 Poseidon 标准 API，使用相同的代码, 在本地集群、timaker 集群中无缝切换。

## 整体架构

timaker 实现方案的整体架构如下：

![任务状态流转](./images/poseidon_timaker.drawio.svg)

### TimakerAdapter

其中，`TimakerAdapter` 实现了标准的 `DispatcherAdapter` 接口，为核心调度器 `Dispatcher` 提供启动、停止、查询任务的方法。
由于任务最终运行在云原生平台上，因此目前只支持使用容器类型任务，对于本地函数类型任务，请参考 [local.md](./local.md) 获取关于如何在云原生环境通过 `LocalAdapter` 运行本地函数任务的指引。

除了对任务标准化接口的支持，`TimakerAdapter` 额外提供了 `DescribeResource` 接口，供业务查询集群中的资源使用情况。

在 `TimakerAdapter` 内部：
- `ExternalAdapter` 提供了一般性的外部集群适配器逻辑，它调用业务实现的 `Container` 接口实例，完成准备数据、读取日志/结果等业务后台工作，同时调用 `TimakerHandler` 提供的方法完成对 timaker 的调用。关于 `ExternalAdapter` 更多介绍可参考 [external.md](./external.md)
- `TimakerHandler` 调用业务实现的 `TimakerJobParser` 将 Poseidon 的 `Job` 对象转换为 timaker 的任务参数，综合通过 `Container.Prepare` 接口获取到的容器参数后，用该参数调用 timaker 接口 `CreateTrainingJob` 接口创建任务。同时，它也封装了 timaker 的 `StartTrainingJob`、`StopTrainingJob`、`DescribeTrainingJob` 接口， 对 `ExternalAdapter` 暴露 `StartJob`、`StopJob`、`DescribeJob` 三个标准方法

`TimakerHandler` 调用 timaker 的接口时，通常通过 Ti-Gateway，此时需要填写签名参数。签名所需要的密钥通过 `GetTimakerTaskInfo` 返回，这是为了支持不同的任务来自不同的开发者，因此需要采用不同的密钥；当 Poseidon 与 timaker 服务部署在同一个集群时，也可以不通过 Ti-Gateway 调用 API，此时不需要填写签名参数（暂未实现）

### timaker

在 timaker 服务内部，它会调用云原生平台的 k8s API，创建并管理 k8s Job 的生命周期，云原生平台可以是 k8s/tcs/tke 等，timaker 对外提供一致的 API，依次屏蔽不同云原生平台的差别。

在 Job 内部的每个 Pod 中，将会运行业务指定的脚本，脚本通过读取（由业务后台写入）共享存储的数据，完成算法，然后将结果写入到共享存储（由业务后台读取）。由于 Pod 可能运行在集群的任意节点上，而 Poseidon 只会部署到一个节点上，因此需要使用共享存储（例如 Ceph/NAS）将二者连接起来。如果是单机部署，使用本地硬盘作为共享存储也是可以的。

## 使用方法

### 通用逻辑

1. 实现 `Container` 接口。该接口定义了在任务开始前、任务运行中、任务完成后等各个阶段对业务逻辑的回调方法，对所有类型的集群均适用。因此，业务针对本地集群环境（LocalAdapter）开发的 `Container` 实现类，无需任何改动，即可在 timaker 集群环境中使用。关于 `Container` 接口的使用指南，请参考 [user.md](./user.md)

2. 实现 `ErrorMarshaler` 接口(可选)。该接口定义了错误码转换的逻辑。同样对所有类型集群适用，请参考 [user.md](./user.md)

### 针对 timaker 的逻辑

1. 引入 `git.woa.com/yt-industry-ai/poseidon/adapter/timaker`

2. 实现 `TimakerJobParser` 接口

    接口在调度任务时使用, 用于 timaker handler 解析任务所需的参数, 接口定义如下
    ```go
   // timaker 任务参数解析器
    type TimakerJobParser interface {
        // 将 Poseidon 任务转换为 timaker 任务的参数
        // job: 任务元数据
        // info: timaker 任务参数
        GetTimakerTaskInfo(ctx context.Context, job *types.Job) (info *pb_timaker.TimakerTaskInfo, err error)
    }
    ```
    接口返回的 `TimakerTaskInfo` 定义如下
    ```go
    type TimakerTaskInfo struct {
        // 启动任务参数
        Host             string                       // 访问网关的 host
        Addr             string                       // 访问网关的地址
        SecretId         string                       // 获取签名的凭证
        SecretKey        string                       // 获取签名的凭证
        Uin              string                       // 用户名
        ProjectId        int32                        // 训练任务使用资源所属项目
        Memory           int32                        // memory数，单位为MB
        CpuNum           int32                        // cpu个数，单位为1/1000核
        GpuNum           int32                        // gpu个数，单位为1/100
        InstanceCount    int32                        // 实例数，如果是多机任务，可以填多实例
        IsElasticity     bool                         // 是否使用弹性资源
        EnvConfig        []*TimakerEnv                // 训练任务环境变量配置，key需要符合linux规范
        StartCmd         string                       // 启动命令
        EnableRdma       bool                         // 是否使用rdma，默认是false
        FileSystemOption *TimakerFileSystemDataSource // 文件存储配置
        TaskName         string                       // 训练任务名称
        ImageName        string                       // 镜像名称
        // 任务状态信息
        TrainingJobId string // 训练任务id信息
        InstanceId    string // 任务实例 ID
        Msg           string // 失败信息
        State         string // 训练任务状态
        CreateTime    string // 训练任务创建时间
        EndTime       string // 训练任务结束时间
    }

    type TimakerEnv struct {
        Name  string // 环境变量名称
        Value string // 环境变量值
    }

    type TimakerFileSystemDataSource struct {
        FileSystemType     string // 文件系统类型
        DirectoryPath      string // 文件存储目录
        ReadOnly           bool   // 是否只读挂载
        FileSystemEndpoint string // 文件系统地址
        User               string // 如果是ceph，则需要传用户账户名
        SecretKey          string // 如果是ceph，则需要传用户账户密钥
    }
    ```
    生成 `TimakerTaskInfo` 的注意事项:

    - `GpuNum` 参数单位是 1/100, `CpuNum` 参数单位是 1/1000, 传入 cpu 和 gpu 的个数时注意单位转换
    - 确认 timaker 服务支持 RDMA 后才能把 `EnableRDMA` 设置为 true, 否则 timaker 服务会因为缺少 RDMA 资源无法调度任务
    - `ProjectId` 为 timaker 的项目 ID, 默认项目的 ID 为 1, 多项目的情况下需要准确提供项目 ID
    - `FileSystemOption` 用于配置挂载目录, 配置的目录将被挂载到容器内 /opt/ml/code 路径下
    - `TrainingJobId`, `InstanceId`, `Msg`, `State`, `CreateTime`, `EndTime` 参数用于保存调度时的任务状态信息, 实现接口时不需要提供(由于 timaker handler 无状态, 所以每次调用 timaker handler 的接口都会新建 `TimakerTaskInfo`, 不能依赖这些参数在不同的调用轮次间传递数据)

3. 初始化 timaker adapter, 添加到调度器的适配器列表中即可以使用 timaker 适配器调度任务
    
    可以参考如下示例初始化 timaker adapter

    ```go
	opt := psd_timaker.NewTimakerAdapterOption()                         // 初始化 timaker adapter 所需的参数
	opt.DB = provider.GetSqlDB()                                         // 设置数据库实例
	opt.DBTimeout = 5 * time.Second                                      // 设置超时时间
	opt.JobDir = path.Join(provider.GetConfig().App.LocalTmpDir, "jobs") // 设置 job 工作的根目录
	opt.LogDir = path.Join(provider.GetConfig().App.LocalTmpDir, "logs") // 设置 job 输出日志的根目录
	opt.ErrorMarshaler = &common.ErrorMarshaler{}                        // 设置自定义的错误转换接口实例

	opt.JobParsers = map[int32]psd_timaker.TimakerJobParser{} // 初始化参数转换器的 map
	registry.RegisterTimakerParser(opt.JobParsers)            // 给需要 timaker adapter 处理的任务类型注册参数转换器

	adapter := psd_timaker.NewTimakerAdapter(opt) // 调用接口初始化 timaker adapter
    ```

## 延伸阅读

1. [如何部署到云原生环境](https://iwiki.woa.com/pages/viewpage.action?pageId=4007408359)
2. [外部集群实现](./external.md)
