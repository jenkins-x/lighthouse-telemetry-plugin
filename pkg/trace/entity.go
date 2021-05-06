package trace

import (
	"errors"

	v1 "k8s.io/api/core/v1"
)

var (
	ErrParentEntityNotFound = errors.New("the parent entity could not be found")
)

type Entity struct {
	Type EntityType
	Name string
}

func ObjectReferenceToEntity(objectRef v1.ObjectReference) *Entity {
	var entityType EntityType
	switch objectRef.Kind {
	case "LighthouseJob":
		entityType = EntityTypeLighthouseJob
	case "PipelineActivity":
		entityType = EntityTypeJxPipelineActivity
	case "PipelineRun":
		entityType = EntityTypeTknPipelineRun
	case "TaskRun":
		entityType = EntityTypeTknTaskRun
	case "Pod":
		entityType = EntityTypeKubePod
	default:
		return nil
	}
	return &Entity{
		Type: entityType,
		Name: objectRef.Name,
	}
}

type EntityType int

const (
	EntityTypeNone EntityType = iota
	EntityTypeLighthouseJob
	EntityTypeJxPipelineActivity
	EntityTypeTknPipelineRun
	EntityTypeTknTaskRun
	EntityTypeKubePod
	EntityTypeKubeEvent
)

func labelValue(labels map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := labels[key]; value != "" {
			return value
		}
	}
	return ""
}
