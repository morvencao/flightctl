package agent

import (
	"context"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"

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

func (c *deviceCodec) Encode(source string, eventType types.CloudEventsType, obj *resource.Device) (*cloudevents.Event, error) {
	evt := cloudevents.NewEvent()
	evt.SetID(uuid.New().String())
	evt.SetSource(source)
	evt.SetType(eventType.String())
	evt.SetTime(time.Now())
	evt.SetExtension("resourceid", string(obj.UID))
	evt.SetExtension("resourceversion", obj.Version)
	evt.SetExtension("clustername", string(obj.UID))
	if obj.GetDeletionTimestamp() != nil {
		evt.SetExtension("deletiontimestamp", obj.DeletionTimestamp.Time)
	}

	// contentStatusJSON, err := json.Marshal(obj.Status)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to marshal device status: %v", err)
	// }

	// fileSyncerStatus := filesyncerapi.FileSyncerStatus{
	// 	ContentStatus: []filesyncerapi.FileSyncerContentStatus{
	// 		{
	// 			Name:    "spec",
	// 			Content: string(contentStatusJSON),
	// 		},
	// 	},
	// 	ReconcileStatus: []filesyncerapi.FileSyncerReconcileStatus{
	// 		{
	// 			Name: "spec",
	// 			Conditions: []metav1.Condition{
	// 				{
	// 					Type:               "DeviceStatusSynced",
	// 					Status:             "True",
	// 					LastTransitionTime: metav1.Now(),
	// 					Message:            "Device status synced",
	// 					Reason:             "DeviceStatusSynced",
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	if err := evt.SetData(cloudevents.ApplicationJSON, obj.Status); err != nil {
		return nil, fmt.Errorf("failed to set filesyncer status data: %v", err)
	}

	return &evt, nil
}

func (c *deviceCodec) Decode(evt *cloudevents.Event) (*resource.Device, error) {
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
	// fileSyncerSpec := filesyncerapi.FileSyncerSpec{}
	// if err := json.Unmarshal([]byte(evtData), &fileSyncerSpec); err != nil {
	// 	return nil, fmt.Errorf("failed to unmarshal filesyncer spec: %v", err)
	// }

	// if len(fileSyncerSpec.Files) != 1 {
	// 	return nil, fmt.Errorf("expected exactly one file in filesyncer spec, got %d", len(fileSyncerSpec.Files))
	// }

	// renderedSpec := v1alpha1.RenderedDeviceSpec{}
	// if err := json.Unmarshal([]byte(fileSyncerSpec.Files[0].Content), &renderedSpec); err != nil {
	// 	return nil, fmt.Errorf("failed to unmarshal rendered spec: %v", err)
	// }

	renderedSpec := v1alpha1.RenderedDeviceSpec{}
	if err := evt.DataAs(&renderedSpec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal rendered spec: %v", err)
	}
	dev := &resource.Device{
		UID:     kubetypes.UID(fmt.Sprintf("%s", deviceName)),
		Version: fmt.Sprintf("%s", version),
		Spec:    renderedSpec,
	}

	return dev, nil
}

type CloudEventAgentClient interface {
	PublishDeviceStatus(ctx context.Context, device *resource.Device) error
	SubscribeDeviceSpec(ctx context.Context, handlers ...generic.ResourceHandler[*resource.Device])
}

type cloudEventAgentClient struct {
	agentClient *generic.CloudEventAgentClient[*resource.Device]
}

func NewCloudEventAgentClient(ctx context.Context, deviceName, server string) (CloudEventAgentClient, error) {
	grpcOptions := grpcoptions.NewGRPCOptions()
	grpcOptions.URL = server
	grpcOptions.CAFile = "/etc/flightctl/certs/maestro-ca.crt"

	cloudEventsClient, err := generic.NewCloudEventAgentClient[*resource.Device](ctx,
		grpcoptions.NewAgentOptions(grpcOptions, deviceName, deviceName),
		resource.NewDeviceLister(),
		resource.StatusHash,
		newDeviceCodec(),
	)
	if err != nil {
		return nil, err
	}

	return &cloudEventAgentClient{
		agentClient: cloudEventsClient,
	}, nil
}

func (c *cloudEventAgentClient) PublishDeviceStatus(ctx context.Context, device *resource.Device) error {
	eventType := types.CloudEventsType{
		CloudEventsDataType: constants.FileSyncerEventDataType,
		SubResource:         "status",
		Action:              "update_request",
	}

	return c.agentClient.Publish(ctx, eventType, device)
}

func (c *cloudEventAgentClient) SubscribeDeviceSpec(ctx context.Context, handlers ...generic.ResourceHandler[*resource.Device]) {
	c.agentClient.Subscribe(ctx, handlers...)
}
