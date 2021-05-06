package trace

import (
	"context"
	"fmt"
	"time"

	jxclientset "github.com/jenkins-x/jx-api/v4/pkg/client/clientset/versioned"
	jxinformers "github.com/jenkins-x/jx-api/v4/pkg/client/informers/externalversions"
	"github.com/jenkins-x/lighthouse-telemetry-plugin/internal/lighthouse"
	lhclientset "github.com/jenkins-x/lighthouse/pkg/client/clientset/versioned"
	lhinformers "github.com/jenkins-x/lighthouse/pkg/client/informers/externalversions"
	"github.com/sirupsen/logrus"
	tknclientset "github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	tkninformers "github.com/tektoncd/pipeline/pkg/client/informers/externalversions"
	exporttrace "go.opentelemetry.io/otel/sdk/export/trace"
	"go.opentelemetry.io/otel/semconv"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Controller struct {
	KubeConfig        *rest.Config
	Namespace         string
	ResyncInterval    time.Duration
	SpanExporter      exporttrace.SpanExporter
	LighthouseHandler *lighthouse.Handler
	Logger            *logrus.Logger
}

func (c *Controller) Start(ctx context.Context) error {
	kClient, err := kubernetes.NewForConfig(c.KubeConfig)
	if err != nil {
		return fmt.Errorf("failed to create a Kubernetes client: %w", err)
	}
	lhClient, err := lhclientset.NewForConfig(c.KubeConfig)
	if err != nil {
		return fmt.Errorf("failed to create a Lighthouse client: %w", err)
	}
	jxClient, err := jxclientset.NewForConfig(c.KubeConfig)
	if err != nil {
		return fmt.Errorf("failed to create a Jenkins X client: %w", err)
	}
	tknClient, err := tknclientset.NewForConfig(c.KubeConfig)
	if err != nil {
		return fmt.Errorf("failed to create a Tekton client: %w", err)
	}

	store := NewStore()
	store.Logger = c.Logger
	store.FallbackTimeout = 1 * time.Second
	store.KubePodClient = kClient.CoreV1().Pods(c.Namespace)
	store.LighthouseJobClient = lhClient.LighthouseV1alpha1().LighthouseJobs(c.Namespace)
	store.JxPipelineActivityClient = jxClient.JenkinsV1().PipelineActivities(c.Namespace)
	store.TknPipelineRunClient = tknClient.TektonV1beta1().PipelineRuns(c.Namespace)
	store.TknTaskRunClient = tknClient.TektonV1beta1().TaskRuns(c.Namespace)

	baseHander := BaseResourceEventHandler{
		Store:        store,
		SpanExporter: c.SpanExporter,
		Logger:       c.Logger,
	}

	c.registerLighthouseWebhookHandlers(baseHander)

	c.startLighthouseInformers(ctx, lhClient, baseHander)
	c.startJenkinsXInformers(ctx, jxClient, baseHander)
	c.startTektonInformers(ctx, tknClient, baseHander)
	c.startKubernetesInformers(ctx, kClient, baseHander)

	return nil
}

func (c *Controller) registerLighthouseWebhookHandlers(baseHandler BaseResourceEventHandler) {
	handler := &LighthouseEventHandler{
		BaseResourceEventHandler: baseHandler,
		Tracer: baseHandler.TracerProviderFor(
			semconv.ServiceNamespaceKey.String(c.Namespace),
			semconv.ServiceNameKey.String("WebHookEvent"),
		).Tracer(""),
	}

	c.LighthouseHandler.RegisterWebhookHandler(handler.handleWebhook)
}

func (c *Controller) startLighthouseInformers(ctx context.Context, client *lhclientset.Clientset, baseHandler BaseResourceEventHandler) {
	lighthouseInformerFactory := lhinformers.NewSharedInformerFactoryWithOptions(
		client,
		c.ResyncInterval,
		lhinformers.WithNamespace(c.Namespace),
	)
	lighthouseInformerFactory.Lighthouse().V1alpha1().LighthouseJobs().Informer().AddEventHandler(&LighthouseJobHandler{
		BaseResourceEventHandler: baseHandler,
		Tracer: baseHandler.TracerProviderFor(
			semconv.ServiceNamespaceKey.String(c.Namespace),
			semconv.ServiceNameKey.String("LighthouseJob"),
		).Tracer(""),
		LighthouseJobClient: client.LighthouseV1alpha1().LighthouseJobs(c.Namespace),
	})
	lighthouseInformerFactory.Start(ctx.Done())
}

func (c *Controller) startJenkinsXInformers(ctx context.Context, client *jxclientset.Clientset, baseHandler BaseResourceEventHandler) {
	jenkinsxInformerFactory := jxinformers.NewSharedInformerFactoryWithOptions(
		client,
		c.ResyncInterval,
		jxinformers.WithNamespace(c.Namespace),
	)
	jenkinsxInformerFactory.Jenkins().V1().PipelineActivities().Informer().AddEventHandler(&JenkinsXPipelineActivityHandler{
		BaseResourceEventHandler: baseHandler,
		Tracer: baseHandler.TracerProviderFor(
			semconv.ServiceNamespaceKey.String(c.Namespace),
			semconv.ServiceNameKey.String("PipelineActivity"),
		).Tracer(""),
		PipelineActivityClient: client.JenkinsV1().PipelineActivities(c.Namespace),
	})
	jenkinsxInformerFactory.Start(ctx.Done())
}

func (c *Controller) startTektonInformers(ctx context.Context, client *tknclientset.Clientset, baseHandler BaseResourceEventHandler) {
	tektonInformerFactory := tkninformers.NewSharedInformerFactoryWithOptions(
		client,
		c.ResyncInterval,
		tkninformers.WithNamespace(c.Namespace),
	)
	tektonInformerFactory.Tekton().V1beta1().PipelineRuns().Informer().AddEventHandler(&TektonPipelineRunHandler{
		BaseResourceEventHandler: baseHandler,
		Tracer: baseHandler.TracerProviderFor(
			semconv.ServiceNamespaceKey.String(c.Namespace),
			semconv.ServiceNameKey.String("Tekton.PipelineRun"),
		).Tracer(""),
		PipelineRunClient: client.TektonV1beta1().PipelineRuns(c.Namespace),
	})
	tektonInformerFactory.Tekton().V1beta1().TaskRuns().Informer().AddEventHandler(&TektonTaskRunHandler{
		BaseResourceEventHandler: baseHandler,
		Tracer: baseHandler.TracerProviderFor(
			semconv.ServiceNamespaceKey.String(c.Namespace),
			semconv.ServiceNameKey.String("Tekton.TaskRun"),
		).Tracer(""),
		TaskRunClient: client.TektonV1beta1().TaskRuns(c.Namespace),
	})
	tektonInformerFactory.Start(ctx.Done())
}

func (c *Controller) startKubernetesInformers(ctx context.Context, client *kubernetes.Clientset, baseHandler BaseResourceEventHandler) {
	kubernetesInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		client,
		c.ResyncInterval,
		informers.WithNamespace(c.Namespace),
	)
	kubernetesInformerFactory.Core().V1().Pods().Informer().AddEventHandler(&KubernetesPodHandler{
		BaseResourceEventHandler: baseHandler,
		Tracer: baseHandler.TracerProviderFor(
			semconv.ServiceNamespaceKey.String(c.Namespace),
			semconv.ServiceNameKey.String("Pod"),
		).Tracer(""),
		PodClient: client.CoreV1().Pods(c.Namespace),
	})
	kubernetesInformerFactory.Core().V1().Events().Informer().AddEventHandler(&KubernetesEventHandler{
		BaseResourceEventHandler: baseHandler,
	})
	kubernetesInformerFactory.Start(ctx.Done())
}
