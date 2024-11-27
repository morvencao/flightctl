package main

import (
	"context"

	"github.com/flightctl/flightctl/internal/cloudevents/source"
	"github.com/flightctl/flightctl/internal/config"
	"github.com/flightctl/flightctl/internal/store"
	workerserver "github.com/flightctl/flightctl/internal/worker_server"
	"github.com/flightctl/flightctl/pkg/k8sclient"
	"github.com/flightctl/flightctl/pkg/log"
	"github.com/flightctl/flightctl/pkg/queues"

	"github.com/sirupsen/logrus"
)

func main() {
	log := log.InitLogs()
	log.Println("Starting worker service")
	defer log.Println("Worker service stopped")

	cfg, err := config.LoadOrGenerate(config.ConfigFile())
	if err != nil {
		log.Fatalf("reading configuration: %v", err)
	}
	log.Printf("Using config: %s", cfg)

	logLvl, err := logrus.ParseLevel(cfg.Service.LogLevel)
	if err != nil {
		logLvl = logrus.InfoLevel
	}
	log.SetLevel(logLvl)

	log.Println("Initializing data store")
	db, err := store.InitDB(cfg, log)
	if err != nil {
		log.Fatalf("initializing data store: %v", err)
	}

	store := store.NewStore(db, log.WithField("pkg", "store"))
	defer store.Close()

	provider := queues.NewAmqpProvider(cfg.Queue.AmqpURL, log)
	k8sClient, err := k8sclient.NewK8SClient()
	if err != nil {
		log.WithError(err).Warning("initializing k8s client, assuming k8s is not supported")
		k8sClient = nil
	}
	// init cloudevents client
	cloudEventClient, err := source.NewCloudEventSourceClient(context.Background(), cfg.Service.MaestroAddress)
	if err != nil {
		log.Fatalf("Error creating cloud event source client: %s", err)
	}

	server := workerserver.New(cfg, log, store, provider, k8sClient, cloudEventClient)
	if err := server.Run(); err != nil {
		log.Fatalf("Error running server: %s", err)
	}
}
