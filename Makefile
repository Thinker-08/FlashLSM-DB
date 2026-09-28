# GOTOOLCHAIN=local keeps every target on the installed Go (1.23 or newer).
export GOTOOLCHAIN := local

PKGS := ./...

.PHONY: all build vet staticcheck fmt

all: fmt vet build

build:
	go build $(PKGS)

vet:
	go vet $(PKGS)

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@2024.1.1 $(PKGS)

fmt:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
