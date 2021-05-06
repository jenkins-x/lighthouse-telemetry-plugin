package trace

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
)

type KubernetesEventHandler struct {
	BaseResourceEventHandler
}

func (h *KubernetesEventHandler) OnAdd(obj interface{}) {
	event, ok := obj.(*v1.Event)
	if !ok {
		h.Logger.Warningf("KubernetesEventHandler called with non Event object: %T", obj)
		return
	}

	log := h.Logger.WithField("Event", event.String())
	log.Trace("Handling KubernetesEvent Added Event")

	if time.Since(event.CreationTimestamp.Time) < 2*time.Second {
		event.CreationTimestamp.Time = time.Now()
	}

	h.Store.AddKubeEvent(event)
}

func (h *KubernetesEventHandler) OnUpdate(oldObj, newObj interface{}) {
	newEvent, ok := newObj.(*v1.Event)
	if !ok {
		h.Logger.Warningf("KubernetesEventHandler called with non Event object: %T", newObj)
		return
	}

	log := h.Logger.WithField("Event", newEvent.String())
	log.Trace("Handling KubernetesEvent Updated Event")

	if time.Since(newEvent.CreationTimestamp.Time) < 2*time.Second {
		newEvent.CreationTimestamp.Time = time.Now()
	}

	h.Store.AddKubeEvent(newEvent)
}

func (h *KubernetesEventHandler) OnDelete(obj interface{}) {
}

type KubeEvents struct {
	Events map[string]*v1.Event
	Names  []string

	mutex sync.RWMutex
}

func (e *KubeEvents) AddEvent(event *v1.Event) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.Events == nil {
		e.Events = make(map[string]*v1.Event)
	}
	if _, found := e.Events[event.Name]; !found {
		e.Names = append(e.Names, event.Name)
	}
	e.Events[event.Name] = event
}

func (e *KubeEvents) GetOrderedEvents() []*v1.Event {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	events := make([]*v1.Event, 0, len(e.Names))
	for _, name := range e.Names {
		if event, found := e.Events[name]; found {
			events = append(events, event)
		}
	}
	return events
}

type Events []*v1.Event

func (e Events) FirstMatchingEvent(reason string, fieldPaths ...string) *v1.Event {
	var fieldPath string
	if len(fieldPaths) > 0 {
		fieldPath = fieldPaths[0]
	}
	for _, event := range e {
		if event.Reason != reason {
			continue
		}
		if fieldPath != "" && event.InvolvedObject.FieldPath == fieldPath {
			return event
		}
	}
	return nil
}
