module github.com/jenkins-x/lighthouse-telemetry-plugin

go 1.16

require (
	github.com/jenkins-x/go-scm v1.10.10
	github.com/jenkins-x/jx-api/v4 v4.0.28
	github.com/jenkins-x/lighthouse v1.0.25
	github.com/mitchellh/go-homedir v1.1.0
	github.com/scylladb/go-set v1.0.2
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0 // indirect
	github.com/tektoncd/pipeline v0.23.0
	github.com/ucarion/urlpath v0.0.0-20200424170820-7ccc79b76bbb // indirect
	go.opentelemetry.io/otel v0.19.0
	go.opentelemetry.io/otel/exporters/otlp v0.19.0
	go.opentelemetry.io/otel/exporters/trace/jaeger v0.19.0
	go.opentelemetry.io/otel/sdk v0.19.0
	go.opentelemetry.io/otel/trace v0.19.0
	k8s.io/api v0.20.2
	k8s.io/apimachinery v0.20.2
	k8s.io/client-go v11.0.1-0.20190805182717-6502b5e7b1b5+incompatible
	mvdan.cc/xurls/v2 v2.3.0 // indirect
)

replace (
	k8s.io/api => k8s.io/api v0.19.2
	k8s.io/client-go => k8s.io/client-go v0.19.2
)
