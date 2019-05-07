GOPATH := $(shell cd ../../../.. && pwd)
export GOPATH

init-dep:
	@dep init

dep:
	@dep ensure

status-dep:
	@dep status

update-dep:
	@dep ensure -update

run:
	@go run main.go

build:
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o target/docker/stage/opt/docker/bin/na_service .

.PHONY: test
