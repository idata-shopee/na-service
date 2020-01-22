GO111MODULE := on
export GO111MODULE

init:
	@go mod init

clean:
	@go mod tidy

update:
	@go get -u

run:
	@go run main.go

test:
	@cd ./na && go test -v -race

cover:
	@cd ./na && go test -coverprofile=coverage.out
	@cd ./na && go tool cover -html=coverage.out

build:
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o stage/bin/service .

.PHONY: test
