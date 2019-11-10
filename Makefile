COMMIT  ?= `git rev-parse HEAD`
REPO    ?= github.com/mikeykhalil/mango

.PHONY: test

cli:
	go install -ldflags "-X ${REPO}/internal/version.CommitSHA=${COMMIT}" ${REPO}

test:
	go test ./...
