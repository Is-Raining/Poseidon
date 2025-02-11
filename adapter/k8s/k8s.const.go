package k8s

// [K8s] Meta Label
const (
	LabelOwner        = "owner"
	LabelTaskName     = "task-name"
	LabelProjectId    = "project-id"
	LabelDefaultOwner = "tiocr" // default
	LabelTask         = "task-from"
)

// [K8S] Meta Annotation
const (
	AnnotationTaskID     string = "task-id"
	AnnotationJobID      string = "job-id"
	AnnotationJobName    string = "job-name"
	AnnotationUpdateTime string = "update-time"

	AnnotationCreator    string = "ti.cloud.tencent.com/creator"
	AnnotationGPUName    string = "ti.cloud.tencent.com/gpu-name"
	AnnotationAppGroupId string = "ti.cloud.tencent.com/group-id"
	AnnotationTaskType   string = "ti.cloud.tencent.com/task-type"
	AnnotationProjectId  string = "BusiModuleID"
)

const (
	// cuda gpu resource key
	CudaCoreResourceKey  = "nvidia.com/gpu"
	CudaCoreResourceType = "tencent.com/vcuda-type"

	ContainerDefaultName = "main"
)
