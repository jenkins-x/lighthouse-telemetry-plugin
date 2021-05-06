package trace

import (
	"context"
	"errors"
	"sync"
	"time"

	jxv1 "github.com/jenkins-x/jx-api/v4/pkg/apis/jenkins.io/v1"
	jenkinsiov1 "github.com/jenkins-x/jx-api/v4/pkg/client/clientset/versioned/typed/jenkins.io/v1"
	lhv1alpha1 "github.com/jenkins-x/lighthouse/pkg/apis/lighthouse/v1alpha1"
	lighthousev1alpha1 "github.com/jenkins-x/lighthouse/pkg/client/clientset/versioned/typed/lighthouse/v1alpha1"
	"github.com/sirupsen/logrus"
	tknv1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	tektonv1beta1 "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/typed/pipeline/v1beta1"
	"go.opentelemetry.io/otel/trace"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	kv1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

var (
	ErrEventTraceNotFound = errors.New("the EventTrace could not be found in the Store")
)

type Store struct {
	LighthouseJobClient      lighthousev1alpha1.LighthouseJobInterface
	JxPipelineActivityClient jenkinsiov1.PipelineActivityInterface
	TknPipelineRunClient     tektonv1beta1.PipelineRunInterface
	TknTaskRunClient         tektonv1beta1.TaskRunInterface
	KubePodClient            kv1.PodInterface
	FallbackTimeout          time.Duration
	Logger                   *logrus.Logger

	eventTraces               map[trace.TraceID]*EventTrace
	eventTracesMutex          sync.RWMutex
	lhJobs                    map[string]*lhv1alpha1.LighthouseJob
	lhJobsMutex               sync.RWMutex
	jxPipelineActivities      map[string]*jxv1.PipelineActivity
	jxPipelineActivitiesMutex sync.RWMutex
	tknPipelineRuns           map[string]*tknv1beta1.PipelineRun
	tknPipelineRunsMutex      sync.RWMutex
	tknTaskRuns               map[string]*tknv1beta1.TaskRun
	tknTaskRunsMutex          sync.RWMutex
	kubePods                  map[string]*v1.Pod
	kubePodsMutex             sync.RWMutex
	kubeEvents                map[Entity]*KubeEvents
	kubeEventsMutex           sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		eventTraces:          make(map[trace.TraceID]*EventTrace),
		lhJobs:               make(map[string]*lhv1alpha1.LighthouseJob),
		jxPipelineActivities: make(map[string]*jxv1.PipelineActivity),
		tknPipelineRuns:      make(map[string]*tknv1beta1.PipelineRun),
		tknTaskRuns:          make(map[string]*tknv1beta1.TaskRun),
		kubePods:             make(map[string]*v1.Pod),
		kubeEvents:           make(map[Entity]*KubeEvents),
	}
}

func (s *Store) GetEventTrace(traceID trace.TraceID) (*EventTrace, error) {
	s.eventTracesMutex.RLock()
	defer s.eventTracesMutex.RUnlock()
	eventTrace := s.eventTraces[traceID]
	if eventTrace == nil {
		return nil, ErrEventTraceNotFound
	}
	return eventTrace, nil
}

func (s *Store) FindEventTraceByEventGUID(eventGUID string) (*EventTrace, error) {
	s.eventTracesMutex.RLock()
	defer s.eventTracesMutex.RUnlock()
	for _, eventTrace := range s.eventTraces {
		if eventTrace.EventGUID == eventGUID {
			return eventTrace, nil
		}
	}
	return nil, ErrEventTraceNotFound
}

func (s *Store) AddEventTrace(eventTrace *EventTrace) {
	s.eventTracesMutex.Lock()
	defer s.eventTracesMutex.Unlock()
	s.eventTraces[eventTrace.RootSpan.SpanContext().TraceID()] = eventTrace
}

func (s *Store) AddLighthouseJob(job *lhv1alpha1.LighthouseJob) {
	s.lhJobsMutex.Lock()
	defer s.lhJobsMutex.Unlock()
	s.lhJobs[job.Name] = job
}

func (s *Store) FindLighthouseJob(labels map[string]string) *lhv1alpha1.LighthouseJob {
	s.lhJobsMutex.RLock()
	for _, job := range s.lhJobs {
		if mapContainsAll(job.Labels, labels) {
			s.lhJobsMutex.RUnlock()
			return job
		}
	}
	s.lhJobsMutex.RUnlock()

	// fallback to the kube API if it's not (yet) in the local store
	ctx, cancelFunc := context.WithTimeout(context.Background(), s.FallbackTimeout)
	defer cancelFunc()
	list, err := s.LighthouseJobClient.List(ctx, metav1.ListOptions{
		LabelSelector: klabels.FormatLabels(labels),
	})
	if err != nil {
		s.Logger.WithError(err).WithField("labels", klabels.FormatLabels(labels)).Debug("failed to list LighthouseJobs using Kubernetes API")
	}
	if list != nil && len(list.Items) > 0 {
		return &list.Items[0]
	}
	return nil
}

func (s *Store) DeleteLighthouseJob(jobName string) {
	s.lhJobsMutex.Lock()
	delete(s.lhJobs, jobName)
	s.lhJobsMutex.Unlock()

	s.DeleteKubeEventsFor(Entity{
		Type: EntityTypeLighthouseJob,
		Name: jobName,
	})
}

func (s *Store) AddJxPipelineActivity(pa *jxv1.PipelineActivity) {
	s.jxPipelineActivitiesMutex.Lock()
	defer s.jxPipelineActivitiesMutex.Unlock()
	s.jxPipelineActivities[pa.Name] = pa
}

func (s *Store) GetJxPipelineActivity(paName string) *jxv1.PipelineActivity {
	s.jxPipelineActivitiesMutex.RLock()
	defer s.jxPipelineActivitiesMutex.RUnlock()
	return s.jxPipelineActivities[paName]
}

func (s *Store) DeleteJxPipelineActivity(paName string) {
	s.jxPipelineActivitiesMutex.Lock()
	delete(s.jxPipelineActivities, paName)
	s.jxPipelineActivitiesMutex.Unlock()

	s.DeleteKubeEventsFor(Entity{
		Type: EntityTypeJxPipelineActivity,
		Name: paName,
	})
}

func (s *Store) AddTknPipelineRun(pr *tknv1beta1.PipelineRun) {
	s.tknPipelineRunsMutex.Lock()
	defer s.tknPipelineRunsMutex.Unlock()
	s.tknPipelineRuns[pr.Name] = pr
}

func (s *Store) GetTknPipelineRun(prName string) *tknv1beta1.PipelineRun {
	s.tknPipelineRunsMutex.RLock()
	pr := s.tknPipelineRuns[prName]
	s.tknPipelineRunsMutex.RUnlock()
	if pr != nil {
		return pr
	}

	// fallback to the kube API if it's not (yet) in the local store
	ctx, cancelFunc := context.WithTimeout(context.Background(), s.FallbackTimeout)
	defer cancelFunc()
	pr, err := s.TknPipelineRunClient.Get(ctx, prName, metav1.GetOptions{})
	if err != nil {
		s.Logger.WithError(err).Warningf("failed to get pipelinerun %s", prName)
	}
	if pr != nil {
		s.AddTknPipelineRun(pr)
	}
	return pr
}

func (s *Store) DeleteTknPipelineRun(prName string) {
	s.tknPipelineRunsMutex.Lock()
	delete(s.tknPipelineRuns, prName)
	s.tknPipelineRunsMutex.Unlock()

	s.DeleteKubeEventsFor(Entity{
		Type: EntityTypeTknPipelineRun,
		Name: prName,
	})
}

func (s *Store) AddTknTaskRun(tr *tknv1beta1.TaskRun) {
	s.tknTaskRunsMutex.Lock()
	defer s.tknTaskRunsMutex.Unlock()
	s.tknTaskRuns[tr.Name] = tr
}

func (s *Store) GetTknTaskRun(trName string) *tknv1beta1.TaskRun {
	s.tknTaskRunsMutex.RLock()
	tr := s.tknTaskRuns[trName]
	s.tknTaskRunsMutex.RUnlock()
	if tr != nil {
		return tr
	}

	// fallback to the kube API if it's not (yet) in the local store
	ctx, cancelFunc := context.WithTimeout(context.Background(), s.FallbackTimeout)
	defer cancelFunc()
	tr, err := s.TknTaskRunClient.Get(ctx, trName, metav1.GetOptions{})
	if err != nil {
		s.Logger.WithError(err).Warningf("failed to get taskrun %s", trName)
	}
	if tr != nil {
		s.AddTknTaskRun(tr)
	}
	return tr
}

func (s *Store) DeleteTknTaskRun(trName string) {
	s.tknTaskRunsMutex.Lock()
	delete(s.tknTaskRuns, trName)
	s.tknTaskRunsMutex.Unlock()

	s.DeleteKubeEventsFor(Entity{
		Type: EntityTypeTknTaskRun,
		Name: trName,
	})
}

func (s *Store) AddKubePod(pod *v1.Pod) {
	s.kubePodsMutex.Lock()
	defer s.kubePodsMutex.Unlock()
	s.kubePods[pod.Name] = pod
}

func (s *Store) GetKubePod(podName string) *v1.Pod {
	s.kubePodsMutex.RLock()
	defer s.kubePodsMutex.RUnlock()
	return s.kubePods[podName]
}

func (s *Store) DeleteKubePod(podName string) {
	s.kubePodsMutex.Lock()
	delete(s.kubePods, podName)
	s.kubePodsMutex.Unlock()

	s.DeleteKubeEventsFor(Entity{
		Type: EntityTypeKubePod,
		Name: podName,
	})
}

func (s *Store) AddKubeEvent(event *v1.Event) {
	entity := ObjectReferenceToEntity(event.InvolvedObject)
	if entity == nil {
		return
	}

	s.kubeEventsMutex.Lock()
	defer s.kubeEventsMutex.Unlock()

	if _, ok := s.kubeEvents[*entity]; !ok {
		s.kubeEvents[*entity] = new(KubeEvents)
	}
	s.kubeEvents[*entity].AddEvent(event)
}

func (s *Store) GetKubeEventsFor(entity Entity) []*v1.Event {
	s.kubeEventsMutex.RLock()
	defer s.kubeEventsMutex.RUnlock()
	return s.kubeEvents[entity].GetOrderedEvents()
}

func (s *Store) DeleteKubeEventsFor(entity Entity) {
	s.kubeEventsMutex.Lock()
	defer s.kubeEventsMutex.Unlock()
	delete(s.kubeEvents, entity)
}

func (s *Store) CollectGarbage() {
	// TODO iterate over all event traces, and for each:
	// if all the entities linked with the spans are not in the store, then remove the event trace
}

func mapContainsAll(candidate, filter map[string]string) bool {
	for k, v := range filter {
		if candidate[k] != v {
			return false
		}
	}
	return true
}
