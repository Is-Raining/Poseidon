package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZipFile(t *testing.T) {
	zipped := "1.zip"
	defer os.RemoveAll(zipped)

	err := ZipFile(zipped, "1", "1/4")
	t.Log(err)
	assert.Nil(t, err)
}

func TestListSubDirs(t *testing.T) {
	dirs, err := ListSubDirs("/tmp/")
	if err != nil {
		t.Error(err)
		return
	}
	for i := range dirs {
		t.Log(dirs[i])
	}
}

func TestListSubDirsWithDepth(t *testing.T) {
	dirs, err := ListSubDirsWithDepth("/tmp/", 1)
	if err != nil {
		t.Error(err)
		return
	}
	for i := range dirs {
		t.Log(dirs[i])
	}
}

func TestRemoveDir(t *testing.T) {
	if err := RemoveDir("/tmp/1"); err != nil {
		t.Error(err)
		return
	}
}

func TestTrimExt(t *testing.T) {
	assert.Equal(t, "abc", TrimExt("abc.json"))
	assert.Equal(t, "xyz/abc", TrimExt("xyz/abc.jpg"))
	assert.Equal(t, "xyz/abc.123", TrimExt("xyz/abc.123.jpg"))
}

func TestRemoveEmptySubDirsRecursive(t *testing.T) {
	assert.Nil(t, MkdirAll("./tmp/a"))
	assert.Nil(t, MkdirAll("./tmp/b"))
	assert.Nil(t, MkdirAll("./tmp/c"))
	assert.Nil(t, MkdirAll("./tmp/d/e"))
	assert.Nil(t, MkdirAll("./tmp/d/f/g"))
	assert.Nil(t, os.WriteFile("./tmp/a/x.txt", []byte("aa"), 0644))
	assert.Nil(t, os.WriteFile("./tmp/d/e/y.txt", []byte("aa"), 0644))
	assert.Nil(t, RemoveEmptySubDirsRecursive("./tmp"))

	subDirs, err := ListSubDirsWithDepth("./tmp", -1)
	assert.Nil(t, err)
	assert.True(t, len(subDirs) == 3)
}
