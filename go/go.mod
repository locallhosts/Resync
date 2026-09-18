module github.com/example/soar-engine

go 1.22

require (
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.12.3
	github.com/redis/go-redis/v9 v9.5.1
	github.com/segmentio/kafka-go v0.4.47
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

// The replace directives below point golang.org/x/* and gopkg.in/*
// modules at their official GitHub mirrors. They exist because this
// module was built in a network-sandboxed environment that could not
// reach golang.org or gopkg.in directly. On a normal machine with full
// internet access these are not needed — delete this block and run
// `go mod tidy` to resolve everything against the canonical module
// paths instead.
replace golang.org/x/net => github.com/golang/net v0.17.0

replace golang.org/x/crypto => github.com/golang/crypto v0.17.0

replace golang.org/x/sys => github.com/golang/sys v0.13.0

replace golang.org/x/text => github.com/golang/text v0.14.0

replace golang.org/x/term => github.com/golang/term v0.13.0

replace gopkg.in/yaml.v3 => github.com/go-yaml/yaml v0.0.0-20220521103104-8f96da9f5d5e

replace gopkg.in/check.v1 => github.com/go-check/check v0.0.0-20161208181325-20d25e280405

replace golang.org/x/tools => github.com/golang/tools v0.6.0

replace golang.org/x/mod => github.com/golang/mod v0.8.0

replace golang.org/x/sync => github.com/golang/sync v0.1.0
