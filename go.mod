module telemetry-collector

go 1.26.2

require (
	github.com/dhruvit2/messagebroker v0.0.2
	github.com/influxdata/influxdb-client-go/v2 v2.14.0
	go.uber.org/zap v1.27.1
	google.golang.org/grpc v1.80.0
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/influxdata/line-protocol v0.0.0-20200327222509-2487e7298839 // indirect
	github.com/oapi-codegen/runtime v1.4.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// replace github.com/dhruvit2/messagebroker => ../messagebroker
