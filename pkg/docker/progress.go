package docker

import (
	"github.com/docker/docker/pkg/progress"
)

type Callback func(id string, current, total int64)

type ImageBuildProgress struct {
	callback Callback
}

func (p ImageBuildProgress) WriteProgress(prog progress.Progress) error {
	if prog.Message != "" {
		// prog.ID, prog.Message
	} else {
		p.callback(prog.ID, prog.Current, prog.Total)
	}
	return nil
}
