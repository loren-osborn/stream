BLANK =
SPACE = $(BLANK) $(BLANK)
TAB = $(BLANK)	$(BLANK)
COMMA = ,
define NEWLINE


endef
OPEN_PAREN := (
CLOSE_PAREN := )

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

PACKAGE_PATH := $(shell go list .)

##
# @brief Finds the first $(2) free TCP ports in a specified range.
#
# @param 1 The port range in the format `start-end` (e.g., `6060-6070`).
# @param 2 The number of free ports to find.
# 
# @return A space-separated list of the first $(2) free ports, or fewer if unavailable.
#
FIND_FREE_PORTS = $(shell \
    count=0; \
    for port in $$(seq $(subst -, ,$(1))); do \
        if ! lsof -iTCP:$$port -sTCP:LISTEN >/dev/null 2>&1; then \
            echo $$port; \
            count=$$((count + 1)); \
            if [ $$count -ge $(2) ]; then break; fi; \
        fi; \
    done \
)

PORT_RANGE := 6060-6070
FREE_PORTS := $(call FIND_FREE_PORTS,$(PORT_RANGE),2)
GODOC_PORT := $(word 1,$(FREE_PORTS))
GODOC_URL := http://localhost:$(GODOC_PORT)
PKGSITE_PORT := $(word 2,$(FREE_PORTS))
PKGSITE_URL := http://localhost:$(PKGSITE_PORT)

ifneq ($(words $(FREE_PORTS)), 2)
    $(error Only found $(words $(FREE_PORTS)) free ports (We need 2 for doc server scraping))
endif


##
# @brief Quote $(1) for bash with single quotes
#
# @param 1 The string to quote
# 
# @return The quoted string.
#
BASH_SINGLE_QUOTE = '$(subst \\,'\\',$(subst ','\'',$(subst \,\\,$(1))))'

##
# @brief Echo command $(1) to stderr before executing. This is intended for
# echoing commands from within bash control structures within `@` recipe lines.
# The echo is automatically suppressed when make is run in "quiet" mode.
#
# @param 1 The command to run
# 
# @return Bash command string to echo then execute given command
#
ECHO_THEN_EXECUTE = $(if $(findstring q,$(MAKEFLAGS)),,echo $(call BASH_SINGLE_QUOTE,$(1)) >&2 ;) $(1)

##
# @brief Start http server $(1) in the background on port $(2), and block until
# liveness URL $(3) responds, or 10 second timeout. Schedules server shutdown for
# shell exit.
#
# @param 1 The name of the server binary
# @param 2 The port to start the server on
# @param 3 The liveness URL to monitor to detect the server is ready
# 
# @return Bash command string preceed scraper commands
#
define START_SERVER
	echo "Starting $(1) server on port $(2)..." >&2; \
	$(call ECHO_THEN_EXECUTE,$(1) -http=:$(2) &) echo $$! > $(1).pid; \
	trap $(call BASH_SINGLE_QUOTE,$(call ECHO_THEN_EXECUTE,kill $$(cat $(1).pid)); rm $(1).pid) EXIT; \
	timeout 10 sh -c $(call BASH_SINGLE_QUOTE,until wget --quiet --output-document=- --tries=1 --timeout=10 \
	--server-response --spider $(call BASH_SINGLE_QUOTE,$(3)) > /dev/null ; do sleep 0.2; done) || \
		(echo "Error: $(1) server did not respond in time" >&2; exit 1)
endef

DOCS_OUTPUT_DIR := doc

# Helper for starting godoc
START_GODOC = $(call START_SERVER,godoc,$(GODOC_PORT),$(GODOC_URL)/pkg)

PROD_GO_FILES = $(patsubst ./%,%,$(filter-out %_test.go,$(shell find . -type f -name '*.go')))
PROD_GO_PACKAGES = $(patsubst ./%,%,$(patsubst %/,%,$(sort $(dir $(PROD_GO_FILES)))))

##
# @brief Find the production (non-test) *.go file in a given directory sorted by age
#
# @param 1 The directory to search
# 
# @return The paths of the prod Go file in the directory.
#
AGE_SORTED_GO_FILES_IN_PKG = $(patsubst ./%,%,$(filter-out %_test.go,$(shell ls -t1 $(1)/*.go)))

##
# @brief Find the newest production (non-test) *.go file in the given directory
#
# @param 1 The directory to search
# 
# @return The path of the newest prod Go file in the directory.
#
NEWEST_GO_FILE = $(word 1,$(call AGE_SORTED_GO_FILES_IN_PKG,$(1)))

##
# @brief Flatten deep path name into a flattened filename
#
# @param 1 The path name to convert
# 
# @return The flattened filename
#
FLATTEN_SRC = $(subst /,__,$(1))

##
# @brief If a file is up to date compared to its source file, print something
#
# @param 1 The output file to check staleness of
# @param 2 The source file to check timestamp of
# @param 3 The text to output when up to date
# @param 4 The text to output when missing or stale
# 
# @return The flattened filename
#
IF_UP_TO_DATE = $(shell $\
	if [[ -e $(1) && $(1) -nt $(2) ]] ; then $\
		echo $(call BASH_SINGLE_QUOTE,$(3)) ; $\
	else $\
		echo $(call BASH_SINGLE_QUOTE,$(4)) ; $\
	fi$\
)


##
# @brief get a `.gitkeep` filename to enable it to be committed to git
#
# @note We don't really want the .gitkeep files, we're just working around a
# GNU Make glitch. 
#
# @param 1 The the directory names to adapt
# 
# @return The resulting directory names
#
GET_GITKEEP = $(patsubst %,%/.gitkeep,$(patsubst %/,%,$(1)))


##
# @brief Transform a %.go source code filename into a raw downloaded .html filename
#
# @param 1 The the filename to adapt
# 
# @return The resulting filename
#
TO_SRC_RAWHRML_FILENAME = $(patsubst %.go,$(DOCS_OUTPUT_DIR)/raw/godoc/src/$(PACKAGE_PATH)/%.go.html,$(1))


##
# @brief Transform a package directory name into raw downloaded .html filename
#
# @param 1 The the filename to adapt
# 
# @return The resulting filename
#
TO_DOC_RAWHRML_FILENAME = $(patsubst %/..html,%.html,$(patsubst %,$(DOCS_OUTPUT_DIR)/raw/pkgsite/$(PACKAGE_PATH)/%.html,$(patsubst %/,%,$(1))))


##
# @brief Transform a go source filename to a rendered %.go.html filename
#
# @param 1 The the filename to adapt
# 
# @return The resulting filename
#
TO_RENDERED_SRC_FILENAME = $(patsubst %.go,$(DOCS_OUTPUT_DIR)/src/%.go.html,$(call FLATTEN_SRC,$(1)))

##
# @brief Transform a directory name containing go source files to a package documentation %.html filename
#
# @param 1 The the directory name to adapt
# 
# @return The resulting filename
#
TO_PACKAGE_DOCS_FILENAME = $(patsubst %,$(DOCS_OUTPUT_DIR)/%.html,$(call FLATTEN_SRC,$(if $(filter .,$(1)),index,$(1))))

##
# @brief Extract a colon-delimited field by index from each space-delimited blob and replace commas with spaces
#
# @param 1 The list of space-delimited blobs
# @param 2 The 1-based index of the colon-delimited field to extract
#
# @return A space-separated list of the extracted and transformed fields
#
GET_BLOB_FIELD = $(strip $(foreach blob,$(1),\
	$(subst $(COMMA),$(SPACE),$(word $(2),$(subst :,$(SPACE),$(blob))))))

BLOB_FIELD_SOURCES    := 1
BLOB_FIELD_RAW_SCRAPE := 2
BLOB_FIELD_FINAL_OUT  := 3
BLOB_FIELD_SERVER_URL := 4

# For each html file we generate a blob of the form:
# {comma seperated list of source files, sorted newest to oldest}:{raw scraped rendered file}:{file with adjusted URLs}:{server URL}
RENDERED_SRC_BLOBS := $(foreach \
	source,$\
	$(PROD_GO_FILES),$\
	$(source):$\
	$(call \
		TO_SRC_RAWHRML_FILENAME,$\
		$(source)$\
	):$\
	$(call \
		TO_RENDERED_SRC_FILENAME,$\
		$(source)$\
	):$\
	/src/$(PACKAGE_PATH)/$(source)$\
)
PACKAGE_DOCS_BLOBS := $(foreach \
	pkg,$\
	$(PROD_GO_PACKAGES),$\
	$(subst \
		$(SPACE),$\
		$(COMMA),$\
		$(call \
			AGE_SORTED_GO_FILES_IN_PKG,$\
			$(pkg)$\
		)$\
	):$\
	$(call \
		TO_DOC_RAWHRML_FILENAME,$\
		$(pkg)$\
	):$\
	$(call \
		TO_PACKAGE_DOCS_FILENAME,$\
		$(pkg)$\
	):$\
	$(patsubst %/.,%,/$(PACKAGE_PATH)/$(pkg))$\
)

# Find RENDERED_SRC blobs that need rebuilding
STALE_RENDERED_SRC_BLOBS := $(strip \
	$(foreach \
		blob,$\
		$(RENDERED_SRC_BLOBS),$\
		$(call \
			IF_UP_TO_DATE,$\
			$(call \
				GET_BLOB_FIELD,$\
				$(blob),$\
				$(BLOB_FIELD_RAW_SCRAPE)$\
			),$\
			$(firstword \
				$(shell ls -1t $(call \
					GET_BLOB_FIELD,$\
					$(blob),$\
					$(BLOB_FIELD_SOURCES)$\
				) Makefile)$\
			),$\
			,$\
			$(blob)$\
		)$\
	)$\
)

# Find PACKAGE_DOCS blobs that need rebuilding
STALE_PACKAGE_DOCS_BLOBS := $(strip \
	$(foreach \
		blob,$\
		$(PACKAGE_DOCS_BLOBS),$\
		$(call \
			IF_UP_TO_DATE,$\
			$(call \
				GET_BLOB_FIELD,$\
				$(blob),$\
				$(BLOB_FIELD_RAW_SCRAPE)$\
			),$\
			$(firstword \
				$(shell ls -1t $(call \
					GET_BLOB_FIELD,$\
					$(blob),$\
					$(BLOB_FIELD_SOURCES)$\
				) Makefile)$\
			),$\
			,$\
			$(blob)$\
		)$\
	)$\
)


# Run all checks (test, lint, format)
all: fmt lint test docs

GENERATED = coverage.html coverage.out godoc.pid $(DOCS_OUTPUT_DIR)

.PHONY: fmt lint test clean docs .FORCE

.SECONDARY: godoc_scrape_group pkgsite_scrape_group

# Run tests with race detection and coverage
test: coverage.html

# empty dirs:
%/.gitkeep:
	mkdir -p $(dir $@)
	touch $@ # We don't **really** want a .gitkeep file... just working around a GNU Make glitch.

# If there are stale pkgsite files, startup the server, scrape them, and bring it down:
$(call \
	GET_BLOB_FIELD,$\
	$(RENDERED_SRC_BLOBS),$\
	$(BLOB_FIELD_RAW_SCRAPE)$\
) : godoc_scrape_group

godoc_scrape_group: $(sort \
	$(call \
		GET_GITKEEP,$\
		$(dir \
			$(call \
				GET_BLOB_FIELD,$\
				$(if \
					$(STALE_RENDERED_SRC_BLOBS),$\
					$(STALE_RENDERED_SRC_BLOBS),$\
					$(RENDERED_SRC_BLOBS)$\
				),$\
				$(BLOB_FIELD_RAW_SCRAPE)$\
			)$\
		) $(DOCS_OUTPUT_DIR)/raw/godoc/$\
	) $(call \
		GET_BLOB_FIELD,$\
		$(if \
			$(STALE_RENDERED_SRC_BLOBS),$\
			$(STALE_RENDERED_SRC_BLOBS),$\
			$(RENDERED_SRC_BLOBS)$\
		),$\
		$(BLOB_FIELD_SOURCES)$\
	) Makefile $(if \
		$(STALE_RENDERED_SRC_BLOBS),$\
		.FORCE,$\
		$\
	)$\
)
	@-$(if \
		$(STALE_RENDERED_SRC_BLOBS),$\
		$(call \
			START_SERVER,$\
			godoc,$\
			$(GODOC_PORT),$\
			http://localhost:$(GODOC_PORT)/pkg$\
		) $(foreach \
			page_blob,$\
			$(if \
				$(STALE_RENDERED_SRC_BLOBS),$\
				$(STALE_RENDERED_SRC_BLOBS),$\
				$(RENDERED_SRC_BLOBS)$\
			),$\
			; $(OPEN_PAREN) $(call \
				ECHO_THEN_EXECUTE,$\
				$(strip wget \
					--page-requisites \
					--convert-links \
					--adjust-extension \
					--no-host-directories \
					--directory-prefix=$(DOCS_OUTPUT_DIR)/raw/godoc \
					$(call \
						BASH_SINGLE_QUOTE,$\
						http://localhost:$(GODOC_PORT)/$(call \
							GET_BLOB_FIELD,$\
							$(page_blob),$\
							$(BLOB_FIELD_SERVER_URL)$\
						)$\
					)$\
				)$\
			) $(CLOSE_PAREN) $\
		),$\
		# no op$\
	)

# If there are stale pkgsite files, startup the server, scrape them, and bring it down:


$(call \
	GET_BLOB_FIELD,$\
	$(PACKAGE_DOCS_BLOBS),$\
	$(BLOB_FIELD_RAW_SCRAPE)$\
) : pkgsite_scrape_group

pkgsite_scrape_group: $(sort \
	$(call \
		GET_GITKEEP,$\
		$(dir \
			$(call \
				GET_BLOB_FIELD,$\
				$(if \
					$(STALE_PACKAGE_DOCS_BLOBS),$\
					$(STALE_PACKAGE_DOCS_BLOBS),$\
					$(PACKAGE_DOCS_BLOBS)$\
				),$\
				$(BLOB_FIELD_RAW_SCRAPE)$\
			)$\
		) $(DOCS_OUTPUT_DIR)/raw/pkgsite/$\
	) $(call \
		GET_BLOB_FIELD,$\
		$(if \
			$(STALE_PACKAGE_DOCS_BLOBS),$\
			$(STALE_PACKAGE_DOCS_BLOBS),$\
			$(PACKAGE_DOCS_BLOBS)$\
		),$\
		$(BLOB_FIELD_SOURCES)$\
	) Makefile $(if \
		$(STALE_PACKAGE_DOCS_BLOBS),$\
		.FORCE,$\
		$\
	)$\
)
	@-$(if \
		$(STALE_PACKAGE_DOCS_BLOBS),$\
		$(call \
			START_SERVER,$\
			pkgsite,$\
			$(PKGSITE_PORT),$\
			http://localhost:$(PKGSITE_PORT)/$\
		) $(foreach \
			page_blob,$\
			$(if \
				$(STALE_PACKAGE_DOCS_BLOBS),$\
				$(STALE_PACKAGE_DOCS_BLOBS),$\
				$(PACKAGE_DOCS_BLOBS)$\
			),$\
			; $(OPEN_PAREN) $(call \
				ECHO_THEN_EXECUTE,$\
				$(strip wget \
					--page-requisites \
					--convert-links \
					--adjust-extension \
					--no-host-directories \
					--directory-prefix=$(DOCS_OUTPUT_DIR)/raw/pkgsite \
					$(call \
						BASH_SINGLE_QUOTE,$\
						http://localhost:$(PKGSITE_PORT)/$(call \
							GET_BLOB_FIELD,$\
							$(page_blob),$\
							$(BLOB_FIELD_SERVER_URL)$\
						)$\
					)$\
				)$\
			) $(CLOSE_PAREN) $\
		),$\
		# no op$\
	)

docs: $(call \
	GET_BLOB_FIELD,$\
	$(RENDERED_SRC_BLOBS),$\
	$(BLOB_FIELD_RAW_SCRAPE)$\
) $(call \
	GET_BLOB_FIELD,$\
	$(PACKAGE_DOCS_BLOBS),$\
	$(BLOB_FIELD_RAW_SCRAPE)$\
)

coverage.html: coverage.out Makefile
	go tool cover -html=coverage.out -o coverage.html

coverage.out: $(shell find $(PROD_GO_PACKAGES) -type f -name '*.go' | sed -e 's,^\./,,' | sort -u) Makefile
	go test -race -cover -coverprofile=coverage.out  ./... || (rm coverage.out ; false)

# Lint the code
lint:
	revive ./...
	golangci-lint run

# Format the code
fmt:
	go fmt ./...
	@#echo '$$PATH = ' $$PATH
	goimports -w $(PROD_GO_PACKAGES)



clean:
	@echo "Cleaning generated files..." >&2
	@if [ -f godoc.pid ]; then \
	    echo "Stopping godoc server..." >&2; \
	    PID=$$(cat godoc.pid); \
	    if kill -0 $$PID 2>/dev/null; then \
	        kill -1 $$PID; \
	        TIMEOUT=20; \
	        while kill -0 $$PID 2>/dev/null && [ $$TIMEOUT -gt 0 ]; do \
	            sleep 0.5; \
	            TIMEOUT=$$((TIMEOUT - 1)); \
	        done; \
	        if kill -0 $$PID 2>/dev/null; then \
	            echo "Process did not terminate gracefully, sending SIGKILL..." >&2; \
	            kill -9 $$PID; \
	        fi; \
	    else \
	        echo "Process already stopped or does not exist. (Stale godoc.pid file)" >&2; \
	    fi; \
	    rm -f godoc.pid; \
	fi
	@if [ -f pkgsite.pid ]; then \
	    echo "Stopping pkgsite server..." >&2; \
	    PID=$$(cat pkgsite.pid); \
	    if kill -0 $$PID 2>/dev/null; then \
	        kill -1 $$PID; \
	        TIMEOUT=20; \
	        while kill -0 $$PID 2>/dev/null && [ $$TIMEOUT -gt 0 ]; do \
	            sleep 0.5; \
	            TIMEOUT=$$((TIMEOUT - 1)); \
	        done; \
	        if kill -0 $$PID 2>/dev/null; then \
	            echo "Process did not terminate gracefully, sending SIGKILL..." >&2; \
	            kill -9 $$PID; \
	        fi; \
	    else \
	        echo "Process already stopped or does not exist. (Stale pkgsite.pid file)" >&2; \
	    fi; \
	    rm -f pkgsite.pid; \
	fi
	rm -rf $(GENERATED)


endif

.FORCE:


