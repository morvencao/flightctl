package resource

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/flightctl/flightctl/api/v1alpha1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"
)

type Device struct {
	UID               kubetypes.UID
	Version           string
	DeletionTimestamp *metav1.Time
	Spec              v1alpha1.RenderedDeviceSpec
	Status            v1alpha1.DeviceStatus
}

func (d *Device) GetUID() kubetypes.UID {
	return d.UID
}

func (r *Device) GetResourceVersion() string {
	return r.Version
}

func (r *Device) GetDeletionTimestamp() *metav1.Time {
	return r.DeletionTimestamp
}

type DeviceLister struct {
	Devices []*Device
}

func NewDeviceLister(devices ...*Device) *DeviceLister {
	return &DeviceLister{
		Devices: devices,
	}
}

func (l *DeviceLister) List(opt types.ListOptions) ([]*Device, error) {
	return l.Devices, nil
}

func StatusHash(d *Device) (string, error) {
	statusBytes, err := json.Marshal(d.Status)
	if err != nil {
		return "", fmt.Errorf("failed to marshal device status, %v", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(statusBytes)), nil
}
