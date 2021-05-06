package otelcarrier

import (
	"strings"
)

const (
	AnnotationsKeyPrefix = "lighthouse.jenkins-x.io/"
)

// AnnotationsCarrier adapts a Kubernetes annotations map to satisfy the OpenTelemetry TextMapCarrier interface.
// Note that the keys will be prefixed with the `AnnotationsKeyPrefix`
type AnnotationsCarrier map[string]string

// Get returns the value associated with the passed key.
func (ac AnnotationsCarrier) Get(key string) string {
	return ac[AnnotationsKeyPrefix+key]
}

// Set stores the key-value pair.
func (ac AnnotationsCarrier) Set(key string, value string) {
	ac[AnnotationsKeyPrefix+key] = value
}

// Keys lists the keys stored in this carrier.
func (ac AnnotationsCarrier) Keys() []string {
	var keys []string
	for k, _ := range ac {
		if strings.HasPrefix(k, AnnotationsKeyPrefix) {
			key := strings.TrimPrefix(k, AnnotationsKeyPrefix)
			keys = append(keys, key)
		}
	}
	return keys
}
