SHELL := /bin/bash

OUT_FILE=arke
GOPKGS:=$(shell go list ./... | grep -v api | grep -v tests/integration | tr '\n' ',' | sed 's/,$$//')

HAVE_PROTOC:=$(shell which protoc 2>/dev/null)
PROTOC=:
ifneq ("$(HAVE_PROTOC)","")
    PROTOC=protoc
else
    $(info No protoc command found, skipping generate task.)
endif

HAVE_PROTOC_DOC:=$(shell which protoc-gen-doc 2>/dev/null)
PROTOCDOC=:
ifneq ("$(HAVE_PROTOC_DOC)","")
    PROTOCDOC=protoc
else
    $(info No protoc-gen-doc command found, skipping generate task.)
endif

HAVE_PROTOC_JAVA:=$(shell which protoc-gen-grpc-java.exe 2>/dev/null)
PROTOCJAVA=:
ifneq ("$(HAVE_PROTOC_JAVA)","")
    PROTOCJAVA=protoc
else
    $(info No protoc-gen-grpc-java.exe command found, skipping generate task.)
endif

UNAME_S := $(shell uname -s | tr A-Z a-z)
ifeq ($(UNAME_S),linux)
	proto_libs = /usr/include
endif
ifeq ($(UNAME_S),darwin)
	proto_libs = /opt/homebrew/include
endif

RABBIT_CONTAINER_RUNNING:=$(shell docker ps -aq -f status=running -f name=integration_rabbitmq_1 2>/dev/null)
RUN_COMPOSE=:
ifneq ("$(RABBIT_CONTAINER_RUNNING)", "")
	RUN_COMPOSE=true
else
	RUN_COMPOSE=false
endif

GOCOVERDIR := $(PWD)/coverage
export GOCOVERDIR

# used for integration test builds
UNAME_ARCH := $(shell uname -m)
arch=:arm64
ifeq ($(UNAME_S),x86_64)
	arch = amd64
endif

ci: clean setup ## Builds binary for linux_amd64 (lax)
	${BUILD_ENV} GOARCH=amd64 GOOS=linux go build -o build/linux/${OUT_FILE} ./cmd

all: clean setup generate osx # osx windows ## Cleans, installs dependencies, generates I18n resource bundle and builds all binaries
.PHONY: all

clean: ## Deletes go build output
	rm -rf build

setup: ## Makes build directories
	mkdir -p build/linux
	mkdir -p build/darwin
	mkdir -p build/windows
	ln -nsf darwin build/osx

generate: generate-proto generate-doc ## Generates Go protobuf files and protobuf docs

generate-proto: ## Generates protobufs
	$(PROTOC) -I$(proto_libs) --proto_path=api/protobuf-spec --go_out=api --go_opt=paths=source_relative --go-grpc_out=api --go-grpc_opt=paths=source_relative api/protobuf-spec/arke.proto

generate-proto-java: ## Generates protobufs for java
	$(PROTOCJAVA) -I$(proto_libs) --plugin=protoc-gen-grpc-java=$(HAVE_PROTOC_JAVA) --proto_path=api/protobuf-spec --grpc-java_out=api/java api/protobuf-spec/*.proto
	$(PROTOCJAVA) -I$(proto_libs) --proto_path=api/protobuf-spec --java_out=api/java api/protobuf-spec/*.proto

generate-doc: ## Generates protobuf docs
	$(PROTOCDOC) -I$(proto_libs) --proto_path=api/protobuf-spec --doc_out=./doc --doc_opt=markdown,arke_protocol.md api/protobuf-spec/*.proto

test-clients: ## Builds test clients
	$(MAKE) -C test/test_producer $(UNAME_S) & \
	$(MAKE) -C test/test_consumer  $(UNAME_S) & \
	$(MAKE) -C test/simple_consumer  $(UNAME_S) & \
	$(MAKE) -C test/simple_producer  $(UNAME_S) & \
	$(MAKE) -C test/healthz  $(UNAME_S) & \
	wait
	@echo "Test clients built"

test-clients-linux: ## Builds test clients
	$(MAKE) -C test/test_producer linux
	$(MAKE) -C test/test_consumer  linux
	$(MAKE) -C test/simple_consumer  linux
	$(MAKE) -C test/simple_producer  linux
	$(MAKE) -C test/healthz  linux


linux: linux-nogen generate ## Builds binary for linux_amd64 (lax)

linux-nogen: setup ## Builds binary for linux_amd64 (lax)
	${BUILD_ENV} GOARCH=amd64 GOOS=linux go build -coverpkg ${GOPKGS} -cover -o build/linux/${OUT_FILE} ./cmd

osx: darwin ## Builds binary for darwin_amd64 (osx)

darwin: setup generate ## Builds binary for darwin_amd64 (osx)
	${BUILD_ENV} GOARCH=arm64 GOOS=darwin go build -coverpkg ${GOPKGS} -cover -o build/darwin/${OUT_FILE} ./cmd

windows: setup generate ## Builds binary for windows_amd64 (wx6)
	${BUILD_ENV} GOARCH=amd64 GOOS=windows go build -coverpkg ${GOPKGS} -cover -o build/windows/${OUT_FILE} ./cmd

build: $(UNAME_S) ## Builds binary for current platform

run:
	GOCOVERDIR=$(GOCOVERDIR) ./build/$(UNAME_S)/${OUT_FILE} &
stop:
	pkill -2 -f build/$(UNAME_S)/${OUT_FILE}

test: generate lint test-nogen ## Executes unit tests

test-nogen: ## Executes unit tests without protoc generation
	mkdir -p $(GOCOVERDIR)
	LOG_FORMAT=term go test -cover -v -timeout 30s --coverprofile unit_coverage.out ./... -args -test.gocoverdir=$(GOCOVERDIR)
	go tool covdata textfmt -i=$(GOCOVERDIR) -o unit_counter_coverage.out
	go tool cover -html=unit_coverage.out -o coverage.html

# integration test related

build_test_c:
	${BUILD_ENV} OTEL_SDK_DISABLED=true go build -coverpkg ${GOPKGS} -cover -o build/$(UNAME_S)/${OUT_FILE}.test ./cmd

pre_stop_test_c:
	killall -2 arke.test || true
	sleep 2

stop_test_c:
	killall -2 arke.test || true
	sleep 2

run_test_c:
	mkdir -p coverage/integration
	LOG_FORMAT=term ./build/$(UNAME_S)/arke.test -cover -v -test.coverprofile integration-coverage.out ./pkg/... -args -test.gocoverdir=${PWD}/coverage/integration &

integration_coverage_report:
	go tool cover -html=integration-coverage.out -o integration-coverage.html

integration_coverage: pre_stop_test_c compose build_test_c run_test_c integration_rabbitmq stop_test_c integration_coverage_report ## Builds arke test binary and runs targets for gathering code coverage from integration tests

integration_rabbitmq: ## Runs integration tests for RabbitMQ
	source env-rabbitmq && go test -count=1 -v -tags=integration ./tests/integration/

lint: ## Run golangci-lint tool
	golangci-lint run --config .github/linters/golangci-config.yaml --allow-parallel-runners ./...

compose: linux compose_only ## Builds and runs docker image(s) for integration tests

compose_only: ## runs docker image(s) for integration tests
	cp ./build/linux/arke tests/integration/
	$(RUN_COMPOSE) || (cd tests/integration && \
		docker compose -f docker-compose-certs.yml down && \
		docker compose -f docker-compose-certs.yml build && \
		docker compose -f docker-compose-certs.yml up --remove-orphans && \
		docker compose -f docker-compose.yml build && \
		docker compose -f docker-compose.yml down && \
		docker compose -f docker-compose.yml up --remove-orphans -d rabbitmq arke && \
		sleep 10)

compose_down: ## Removes integration tests Docker resources
	cd tests/integration ; \
		docker compose down

integration_test: ## Runs integration tests
	echo -e "\033[0;36mNo providerTLS\033[0m"
	cd tests/integration ; go test -count=1 -v -timeout 5m -cover -coverprofile=int_coverage.out -tags=integration ./

integration_test_tls: ## Runs integration tests with TLS enabled
	echo -e "\033[0;31mProvider TLS enabled\033[0m"
	cd tests/integration ; ARKE_PROVIDER_TLS=true ARKE_BROKER_PORT=5671 go test -timeout 5m -count=1 -v -cover -coverprofile=int_coverage.out -tags=integration ./

integration_test_tls_send_ca: ## Runs integraiton tests with TLS enabled by sending TLS certs
	echo "\033[0;31mProvider TLS enabled (sending CA cert)\033[0m"
	cd tests/integration ; ARKE_PROVIDER_TLS=sendCA ARKE_BROKER_PORT=5671 go test -count=1 -v -cover -coverprofile=int_coverage.out -tags=integration ./
integration: compose integration_test ## Runs compose and integration_test

integration_all: integration integration_test_tls integration_test_tls_send_ca ## Runs all integration_test* targets.

help: ## Lists the makefile's targets
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
