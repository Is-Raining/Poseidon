package adapter

import (
	"fmt"
	"sync"
	"time"

	"git.woa.com/yt-industry-ai/poseidon/internal/master/model"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
)

// 缓存临时状态
type LocalAdapterCache struct {
	cache sync.Map
}

// 首次重试
func (c *LocalAdapterCache) firstRetryTimeKey(job *model.Job) string {
	return fmt.Sprintf("%s_first_retry_time", job.JobId)
}

// 获取指定 job 的首次重试时间
func (c *LocalAdapterCache) GetOrSetJobFirstRetryTime(job *model.Job) time.Time {
	key := c.firstRetryTimeKey(job)
	val, ok := c.cache.Load(key)
	if !ok {
		t := time.Now()
		c.cache.Store(key, utils.FormatTime(t))
		return t
	}

	if t, err := utils.ParseTime(val.(string)); err == nil {
		return t
	}

	c.cache.Delete(key)
	t := time.Now()
	c.cache.Store(key, utils.FormatTime(t))
	return t
}

// 清除指定 job 的首次重试时间
func (c *LocalAdapterCache) ClearFirstRetryTime(job *model.Job) {
	c.cache.Delete(c.firstRetryTimeKey(job))
}
