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

test:
	@cd ./na && go test -v -race

cover:
	@cd ./na && go test -coverprofile=coverage.out
	@cd ./na && go tool cover -html=coverage.out

build:
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o stage/bin/na_service .

.PHONY: test
