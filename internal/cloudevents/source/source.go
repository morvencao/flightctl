package source

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/flightctl/flightctl/api/v1alpha1"
	"github.com/flightctl/flightctl/internal/cloudevents/resource"
	"github.com/openshift-online/maestro/pkg/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	grpcoptions "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"

	kubetypes "k8s.io/apimachinery/pkg/types"
)

type deviceCodec struct{}

func newDeviceCodec() *deviceCodec {
	return &deviceCodec{}
}

func (c *deviceCodec) EventDataType() types.CloudEventsDataType {
	return constants.FileSyncerEventDataType
}

func (c *deviceCodec) Encode(source string, eventType types.CloudEventsType, device *resource.Device) (*ce.Event, error) {
	evt := ce.NewEvent()
	evt.SetID(uuid.New().String())
	evt.SetSource(source)
	evt.SetType(eventType.String())
	evt.SetTime(time.Now())
	evt.SetExtension("resourceid", string(device.UID))
	evt.SetExtension("resourceversion", device.Version)
	evt.SetExtension("clustername", string(device.UID))
	// TODO: handle deletion timestamp
	// if obj.GetDeletionTimestamp() != nil {
	// 	evt.SetExtension("deletiontimestamp", obj.DeletionTimestamp.Time)
	// }

	// specJSON, err := json.Marshal(device.Spec)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to marshal device spec: %v", err)
	// }
	// fileSyncerSpec := filesyncerapi.FileSyncerSpec{
	// 	Files: []filesyncerapi.FileObject{
	// 		{
	// 			Name:    "spec",
	// 			Content: string(specJSON),
	// 		},
	// 	},
	// }

	if err := evt.SetData(ce.ApplicationJSON, device.Spec); err != nil {
		return nil, fmt.Errorf("failed to set data for device spec: %v", err)
	}

	return &evt, nil
}

func (c *deviceCodec) Decode(evt *ce.Event) (*resource.Device, error) {
	// resourceID, err := evt.Context.GetExtension("resourceid")
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to get resource ID: %v", err)
	// }

	version, err := evt.Context.GetExtension("resourceversion")
	if err != nil {
		return nil, fmt.Errorf("failed to get resource version: %v", err)
	}

	deviceName, err := evt.Context.GetExtension("clustername")
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster name: %v", err)
	}

	// TODO: handle deletion timestamp

	// evtData := evt.Data()
	// fileSyncerStatus := filesyncerapi.FileSyncerStatus{}
	// if err := json.Unmarshal([]byte(evtData), &fileSyncerStatus); err != nil {
	// 	return nil, fmt.Errorf("failed to unmarshal filesyncer status: %v", err)
	// }

	// if len(fileSyncerStatus.ContentStatus) != 1 {
	// 	return nil, fmt.Errorf("unexpected number of content statuses: %d", len(fileSyncerStatus.ContentStatus))
	// }

	// if fileSyncerStatus.ContentStatus[0].Name != "spec" {
	// 	return nil, fmt.Errorf("unexpected content status name: %s", fileSyncerStatus.ContentStatus[0].Name)
	// }

	// status := v1alpha1.DeviceStatus{}
	// if err := json.Unmarshal([]byte(fileSyncerStatus.ContentStatus[0].Content), &status); err != nil {
	// 	return nil, fmt.Errorf("failed to unmarshal rendered status: %v", err)
	// }

	status := v1alpha1.DeviceStatus{}
	if err := evt.DataAs(&status); err != nil {
		return nil, fmt.Errorf("failed to unmarshal device status: %v", err)
	}

	dev := &resource.Device{
		UID:     kubetypes.UID(fmt.Sprintf("%s", deviceName)),
		Version: fmt.Sprintf("%s", version),
		Status:  status,
	}

	return dev, nil
}

type CloudEventSourceClient interface {
	PublishDeviceSpec(ctx context.Context, device *resource.Device) error
	SubscribeDeviceStatus(ctx context.Context, handlers ...generic.ResourceHandler[*resource.Device])
}

type cloudEventSourceClient struct {
	sourceClient *generic.CloudEventSourceClient[*resource.Device]
}

func NewCloudEventSourceClient(ctx context.Context, server string) (CloudEventSourceClient, error) {
	grpcOptions := grpcoptions.NewGRPCOptions()
	grpcOptions.URL = server
	grpcOptions.CAFile = "/root/.flightctl/service-ca.crt"

	cloudEventsClient, err := generic.NewCloudEventSourceClient[*resource.Device](ctx,
		grpcoptions.NewSourceOptions(grpcOptions, "flightctl"),
		resource.NewDeviceLister(),
		resource.StatusHash,
		newDeviceCodec(),
	)
	if err != nil {
		return nil, err
	}

	return &cloudEventSourceClient{
		sourceClient: cloudEventsClient,
	}, nil
}

func (c *cloudEventSourceClient) PublishDeviceSpec(ctx context.Context, device *resource.Device) error {
	eventType := types.CloudEventsType{
		CloudEventsDataType: constants.FileSyncerEventDataType,
		SubResource:         "spec",
		Action:              "update_request",
	}

	return c.sourceClient.Publish(ctx, eventType, device)
}

func (c *cloudEventSourceClient) SubscribeDeviceStatus(ctx context.Context, handlers ...generic.ResourceHandler[*resource.Device]) {
	c.sourceClient.Subscribe(ctx, handlers...)
}
