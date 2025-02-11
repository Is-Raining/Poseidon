//go:build !appengine && !js && windows
// +build !appengine,!js,windows

package device

func DetectDevices() (name string, gpus []int) {
	return "", nil
}
