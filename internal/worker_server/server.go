package workerserver

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	api "github.com/flightctl/flightctl/api/v1alpha1"
	"github.com/flightctl/flightctl/internal/cloudevents/resource"
	"github.com/flightctl/flightctl/internal/cloudevents/source"
	"github.com/flightctl/flightctl/internal/config"
	"github.com/flightctl/flightctl/internal/store"
	"github.com/flightctl/flightctl/internal/tasks"
	"github.com/flightctl/flightctl/pkg/k8sclient"
	"github.com/flightctl/flightctl/pkg/queues"
	"github.com/sirupsen/logrus"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

type Server struct {
	cfg              *config.Config
	log              logrus.FieldLogger
	store            store.Store
	provider         queues.Provider
	k8sClient        k8sclient.K8SClient
	cloudeventClient source.CloudEventSourceClient
}

// New returns a new instance of a flightctl server.
func New(
	cfg *config.Config,
	log logrus.FieldLogger,
	store store.Store,
	provider queues.Provider,
	k8sClient k8sclient.K8SClient,
	cloudeventClient source.CloudEventSourceClient,

) *Server {
	return &Server{
		cfg:              cfg,
		log:              log,
		store:            store,
		provider:         provider,
		k8sClient:        k8sClient,
		cloudeventClient: cloudeventClient,
	}
}

func (s *Server) Run() error {
	s.log.Println("Initializing async jobs")
	publisher, err := tasks.TaskQueuePublisher(s.provider)
	if err != nil {
		s.log.WithError(err).Error("failed to create fleet queue publisher")
		return err
	}
	callbackManager := tasks.NewCallbackManager(publisher, s.log)
	if err = tasks.LaunchConsumers(context.Background(), s.provider, s.store, callbackManager, s.k8sClient, s.cloudeventClient, 1, 1); err != nil {
		s.log.WithError(err).Error("failed to launch consumers")
		return err
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	s.cloudeventClient.SubscribeDeviceStatus(ctx, func(action types.ResourceAction, device *resource.Device) error {
		s.log.Infof("Received device status for device %s via cloudevents", device.UID)
		deviceName := string(device.UID)
		s.store.Device().UpdateStatus(ctx, store.NullOrgId, &api.Device{
			Metadata: api.ObjectMeta{
				Name: &deviceName,
			},
			Status: &device.Status,
		})
		return nil
	})

	sigShutdown := make(chan os.Signal, 1)
	signal.Notify(sigShutdown, os.Interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigShutdown
		s.log.Println("Shutdown signal received")
		s.provider.Stop()
		ctxCancel()
	}()
	s.provider.Wait()

	return nil
}
