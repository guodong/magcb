BUILD_DIR = build

P4C = p4c-bm2-ss
P4C_ARGS += --p4runtime-files $(BUILD_DIR)/$(basename $@).p4.p4info.txt

source = $(wildcard *.p4)
compiled_json := $(source:.p4=.json)

all: build

build: dirs $(compiled_json)

%.json: %.p4
	$(P4C) --p4v 16 $(P4C_ARGS) -o $(BUILD_DIR)/$@ $<

dirs:
	mkdir -p $(BUILD_DIR)

clean: stop
	rm -rf $(BUILD_DIR)