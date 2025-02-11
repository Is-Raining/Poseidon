# 外部适配器

## 整体架构

外部适配器实现方案的整体架构如下：

![外部适配器架构](./images/poseidon_external_arch.drawio.svg)

- `ExternalAdapter` 实现了 `DispatcherAdapter` 接口, 给 poseidon 提供了创建、查询、停止外部集群上任务的能力
- `ExternalAdapter` 首先从缓存中读取 `Container` 的实例, 如果缓存中没有实例则保存初始化的 `Container` 实例到缓存中, 后续调用 `Initialize` 方法再注入数据到实例中
- `StartJob` 方法调用 `Container` 实例的 `Prepare` 方法准备容器执行所需的数据, 而后转到 `ExternalHandler` 与外部集群交互
- `DescribeJob` 方法把任务状态返回给上层, 从而根据任务不同的状态采取不同的调度流程
- `StopJob` 方法成功停止任务后, 会调用 `Container` 实例的 `Submit` 方法处理任务输出的数据和日志
- `Container` 实例的 `Check` 方法在容器运行期间异步定时执行, 检查任务的执行情况

## 接口流程

接口执行的流程图如下: 

![外部适配器流程构](./images/poseidon_external_flow.drawio.svg)
### StartJob

1. 根据任务类型获取工厂方法, 调用工厂方法得到用户实现的 container, 使用 job 和 container 创建容器任务运行实例
2. 如果当前实例在缓存中则退出流程, 避免重复开启任务; 否则把实例保存到缓存中, 转入下一步
3. 创建任务日志目录和任务临时目录, 将日志目录软链到临时目录的 log 目录
4. 调用任务运行实例的 `Initialize` 和 `Prepare` 方法, 向 job context 中注入一些参数
5. 调用 external handler 的 `StartJob` 方法, 开启任务
6. 开启任务失败且需要重试时, 从缓存中移除当前的任务运行实例
7. external handler 无状态, 需要把 `StartJob` 方法返回的任务 id 更新到 t_schedule_job 表的 external_job_id 列中
8. 启动检查线程, 定时调用任务运行实例的 `Check` 方法, 检查任务是否在正常执行

### DescribeJob

1. 根据任务类型获取工厂方法, 调用工厂方法得到用户实现的 container, 使用 job 和 container 创建容器任务运行实例
2. 尝试从缓存中读取容器任务运行实例
    - 如果缓存中存在实例则使用该实例, 并且从缓存中读取检查结果, 如果任务检查失败推出流程
    - 如果缓存中不存在实例说明在开启任务后 scheduler 曾重启, 因此重新调用任务运行实例的 `Initialize` 方法, 初始化查询 job 所需的必要数据, 同时重新启动检查线程, 定时调用任务运行实例的 `Check` 方法
3. 调用 external handler 的 `DescribeJob` 方法, 查询得到任务当前的信息

### StopJob

1. 根据任务类型获取工厂方法, 调用工厂方法得到用户实现的 container, 使用 job 和 container 创建容器任务运行实例
2. 尝试从缓存中读取容器任务运行实例
    - 如果缓存中存在实例则使用该实例
    - 如果缓存中不存在实例说明在开启任务后 scheduler 曾重启, 因此重新调用任务运行实例的 `Initialize` 方法, 初始化停止 job 所需的必要数据
3. 停止任务后不需要继续检查任务, 所以取消检查线程
4. 调用 external handler 的 `StopJob` 方法, 终止当前任务
5. 调用 external handler 的 `DescribeJob` 方法, 查询任务状态, 并且判断任务是否存在, 不存在退出流程
6. 如果任务执行成功则调用任务运行实例的 `Submit` 方法读取任务输出的数据和日志
7. 无论任务是否成功都需要调用任务运行实例的 `Clean` 方法, 执行清理工作

### Gather

1. 从任务存储层读取指定任务和适配器类型的 job, 添加到 gather 中
2. 给任务排序后返回 gather

### Allocate

外部集群不需要分配本地集群的资源, 所以该接口使用 UnimplementedDispatcherAdapter 的默认 Allocate 方法返回空值。