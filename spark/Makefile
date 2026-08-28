RELEASE := 'v26.08.2'

all: format lint build

# -v graphsense-spark-temp-volume:/tmp/spark:z 
RUNTRANSFORM=sh -c '\
  docker run --rm \
	-e RAW_KEYSPACE=$$1_raw_dev \
	-e TGT_KEYSPACE=$$1_transformed_dev \
	-e NETWORK=$$1 \
	-p 4040:4040 \
	--network="host" \
	graphsense-spark ./submit.sh' RUNTRANSFORM

test:
	sbt test

test-account:
	sbt Test/compile "testOnly org.graphsense.account.*"
	sbt Test/compile "testOnly org.graphsense.common.*"

test-utxo:
	sbt Test/compile "testOnly org.graphsense.utxo.*"
	sbt Test/compile "testOnly org.graphsense.common.*"

test-common:
	sbt Test/compile "testOnly org.graphsense.common.*"

format:
	sbt scalafmt

lint:
	sbt compile
	sbt scalafix

build:
	sbt package

build-fat:
	sbt assembly

DOCKER_BUILD_FLAGS := $(shell docker build --help 2>&1 | grep -q -- --provenance && echo --provenance=false)

build-docker:
	docker build $(DOCKER_BUILD_FLAGS) . -t graphsense-spark

run-docker-eth-transform-local: build-docker
	${RUNTRANSFORM} eth

run-docker-ltc-transform-local: build-docker
	${RUNTRANSFORM} ltc

run-docker-btc-transform-local: build-docker
	${RUNTRANSFORM} btc

run-docker-zec-transform-local: build-docker
	${RUNTRANSFORM} zec

run-docker-bch-transform-local: build-docker
	${RUNTRANSFORM} bch

run-docker-trx-transform-local: build-docker
	${RUNTRANSFORM} trx

tag-version:
	#-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)


.PHONY: all test lint format build tag-version start-local-cassandra stop-local-cassandra run-local-transform build-docker test-account test-utxo test-common
