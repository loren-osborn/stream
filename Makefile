BLANK =
SPACE = $(BLANK) $(BLANK)
TAB = $(BLANK)	$(BLANK)
define NEWLINE


endef

# Check if Go's bin path is in the $PATH, and add it at front if missing.
GOPATH := $(shell go env GOPATH)
GO_BIN_PATH := $(GOPATH)/bin
ifeq (,$(findstring :$(GO_BIN_PATH):,:$(PATH):))
export PATH := $(GO_BIN_PATH):$(PATH)

ORIGINAL_MAKE_GOALS := $(MAKECMDGOALS)

MAKECMDGOALS := restart

restart:
	@echo Restarting make after updating PATH...
	$(MAKE) $(ORIGINAL_MAKE_GOALS)


# Updating MAKECMDGOALS does not appear to be working properly. This is a hack
# To restart make if MAKECMDGOALS is non-empty
$(eval $(foreach target,$(ORIGINAL_MAKE_GOALS),$(target): restart$(NEWLINE)$(TAB)@# no-op$(NEWLINE)$(NEWLINE)))

else

NON_EMPTY_GO_PACKAGES = $(shell find . -type f -name '*.go' | sed -e 's,//*[^/][^/]*$$,,' | sort -u)

GENERATED = coverage.html coverage.out

.PHONY: fmt lint test clean

# Run all checks (test, lint, format)
all: fmt lint test

# Run tests with race detection and coverage
test: coverage.html

coverage.html: coverage.out Makefile
	go tool cover -html=coverage.out -o coverage.html

coverage.out: $(shell find $(NON_EMPTY_GO_PACKAGES) -type f -name '*.go' | sed -e 's,^\./,,' | sort -u) Makefile
	go test -race -cover -coverprofile=coverage.out  ./... || (rm coverage.out ; false)

# Lint the code
lint:
	golangci-lint run

# Format the code
fmt:
	go fmt ./...
	@#echo '$$PATH = ' $$PATH
	goimports -w $(NON_EMPTY_GO_PACKAGES)

clean:
	rm -rf $(GENERATED)


endif

