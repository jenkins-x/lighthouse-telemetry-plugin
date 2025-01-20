package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/jenkins-x/lighthouse-telemetry-plugin/internal/kube"
	"github.com/jenkins-x/lighthouse-telemetry-plugin/internal/lighthouse"
	"github.com/jenkins-x/lighthouse-telemetry-plugin/internal/version"
	"github.com/jenkins-x/lighthouse-telemetry-plugin/pkg/trace"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	exporttrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	options struct {
		namespace              string
		resyncInterval         time.Duration
		childPullRequestDelay  time.Duration
		tracesExporterType     string
		tracesExporterEndpoint string
		lighthouseHMACKey      string
		kubeConfigPath         string
		listenAddr             string
		logLevel               string
		printVersion           bool
	}
)

func init() {
	pflag.StringVar(&options.namespace, "namespace", "jx", "Name of the jx namespace")
	pflag.DurationVar(&options.resyncInterval, "resync-interval", 5*time.Minute, "Resync interval between full re-list operations")
	pflag.DurationVar(&options.childPullRequestDelay, "child-pr-delay", 10*time.Minute, "Time to wait for a possible child pull request to be created, when generating gitops traces")
	pflag.StringVar(&options.tracesExporterType, "traces-exporter-type", os.Getenv("TRACES_EXPORTER_TYPE"), "OpenTelemetry traces exporter type: otlp:grpc:insecure, otlp:http:insecure")
	pflag.StringVar(&options.tracesExporterEndpoint, "traces-exporter-endpoint", os.Getenv("TRACES_EXPORTER_ENDPOINT"), "OpenTelemetry traces exporter endpoint (host:port)")
	pflag.StringVar(&options.lighthouseHMACKey, "lighthouse-hmac-key", os.Getenv("LIGHTHOUSE_HMAC_KEY"), "HMAC key used by Lighthouse to sign the webhooks")
	pflag.StringVar(&options.listenAddr, "listen-addr", ":8080", "Address on which the HTTP server will listen for incoming connections")
	pflag.StringVar(&options.logLevel, "log-level", "INFO", "Log level - one of: trace, debug, info, warn(ing), error, fatal or panic")
	pflag.StringVar(&options.kubeConfigPath, "kubeconfig", kube.DefaultKubeConfigPath(), "Kubernetes Config Path. Default: KUBECONFIG env var value")
	pflag.BoolVar(&options.printVersion, "version", false, "Print the version")
}

func main() {
	pflag.Parse()

	if options.printVersion {
		fmt.Printf("Version %s - Revision %s - Date %s", version.Version, version.Revision, version.Date)
		return
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	logger := logrus.New()
	logLevel, err := logrus.ParseLevel(options.logLevel)
	if err != nil {
		logger.WithField("logLevel", options.logLevel).WithError(err).Error("Invalid log level")
	} else {
		logger.SetLevel(logLevel)
	}
	logger.WithField("logLevel", logLevel).Info("Starting")

	kConfig, err := kube.NewConfig(options.kubeConfigPath)
	if err != nil {
		logger.WithError(err).Fatal("failed to create a Kubernetes config")
	}

	lighthouseHandler := &lighthouse.Handler{
		SecretToken: options.lighthouseHMACKey,
		Logger:      logger,
	}

	var spanExporter exporttrace.SpanExporter
	if len(options.tracesExporterType) > 0 && len(options.tracesExporterEndpoint) > 0 {
		logger.WithField("type", options.tracesExporterType).WithField("endpoint", options.tracesExporterEndpoint).Info("Initializing OpenTelemetry Traces Exporter")
		switch options.tracesExporterType {
		case "otlp:grpc:insecure":
			spanExporter, err = otlptracegrpc.New(ctx,
				otlptracegrpc.WithEndpoint(options.tracesExporterEndpoint),
				otlptracegrpc.WithInsecure(),
			)
		case "otlp:http:insecure":
			spanExporter, err = otlptracehttp.New(ctx,
				otlptracehttp.WithEndpoint(options.tracesExporterEndpoint),
				otlptracehttp.WithInsecure(),
			)
			if err != nil {
				logger.WithError(err).Fatal("failed to create an OpenTelemetry Exporter")
			}
		}
	}

	if spanExporter != nil {
		logger.WithField("namespace", options.namespace).WithField("resyncInterval", options.resyncInterval).Info("Starting Trace Controller")
		err = (&trace.Controller{
			KubeConfig:            kConfig,
			Namespace:             options.namespace,
			ResyncInterval:        options.resyncInterval,
			ChildPullRequestDelay: options.childPullRequestDelay,
			SpanExporter:          spanExporter,
			LighthouseHandler:     lighthouseHandler,
			Logger:                logger,
		}).Start(ctx)
		if err != nil {
			logger.WithError(err).Fatal("Failed to start the trace controller")
		}
	} else {
		logger.Warning("NOT starting the Trace Controller!")
	}

	http.Handle("/lighthouse/events", lighthouseHandler)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	logger.WithField("listenAddr", options.listenAddr).Info("Starting HTTP Server")
	err = http.ListenAndServe(options.listenAddr, nil)
	if !errors.Is(err, http.ErrServerClosed) {
		logger.WithError(err).Fatal("failed to start HTTP server")
	}
}
