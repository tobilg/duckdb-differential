PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=differential
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Dependencies come from vcpkg at this commit (also set in CI and the community descriptor).
VCPKG_COMMIT ?= cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3

# The duckdb submodule is pinned to the untagged commit the community repository builds and deploys as
# v2.0.0 (d54cbbe3df): name its version explicitly instead of `git describe`.
# Empty once the submodule is pinned to a release tag.
OVERRIDE_GIT_DESCRIBE ?= v2.0.0

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# ---------------------------------------------------------------------------------------
# Integration build: test hooks compiled in, and the extension NOT linked into the CLI or
# libduckdb, so the Python suites exercise the loadable artifact exactly as users load it.
# Release artifacts (`make release`) never contain the hooks.
.PHONY: bench-tpch integration test_integration test_merge_qualification test_minio test_upstream test_upstream_pr test_upstream_skips test_upstream_harness test_publication vcpkg bump-version check_skill

# TPC-H benchmarks (native, Differential local, Differential on a disposable MinIO) with the release
# extension; renders build/evidence/benchmarks-tpch.md. TPCH_ARGS passes options (e.g. --queries 1,6 --runs 3).
bench-tpch:
	DIFFERENTIAL_EXTENSION=$(PROJ_DIR)build/release/extension/differential/differential.duckdb_extension \
		python3 benchmarks/tpch.py $(TPCH_ARGS)
	python3 benchmarks/tpch_report.py build/evidence/tpch.json

# Static checks of the agent skill (references copied from docs/, metadata, links, manifests); needs
# scripts/requirements-skill.txt and never runs a coding-agent harness (README.md#agent-skill).
check_skill:
	python3 scripts/sync_skill.py --check
	python3 scripts/check_skill.py
	python3 -m unittest discover -s test/skill -p 'test_*.py'

test_publication:
	python3 scripts/check_publication.py

bump-version:
	@test -n "$(V)" || { echo "Usage: make bump-version V=X.Y.Z" >&2; exit 2; }
	@python3 "$(PROJ_DIR)scripts/bump_version.py" "$(V)"

INTEGRATION_FLAGS=$(BUILD_FLAGS) -DDUCKDB_EXTENSION_CONFIGS='${PROJ_DIR}extension_config_integration.cmake' \
	-DDIFFERENTIAL_TEST_HOOKS=ON -DBUILD_UNITTESTS=1

integration:
	mkdir -p build/integration
	cmake $(GENERATOR) $(INTEGRATION_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/integration
	cmake --build build/integration --config Release

# C++ unit tests and the stock-host Python suites (no network, no containers).
test_integration:
	ctest --test-dir build/integration/extension/differential --output-on-failure
	python3 scripts/run_integration.py

# Bounded local pilot rehearsal, stateful soak and immutable compatibility fixtures.
test_merge_qualification:
	python3 test/integration/merge_qualification.py

# Unmodified DuckDB SQLLogicTests against both storage backends, with audited exclusions.
UPSTREAM_BUILD_DIR ?= build/integration
UPSTREAM_JOBS ?= 2
UPSTREAM_ARGS ?=
test_upstream:
	python3 scripts/test_upstream.py --build-dir $(UPSTREAM_BUILD_DIR) --jobs $(UPSTREAM_JOBS) $(UPSTREAM_ARGS)

test_upstream_pr:
	python3 scripts/test_upstream.py --build-dir $(UPSTREAM_BUILD_DIR) --jobs $(UPSTREAM_JOBS) --profile pr $(UPSTREAM_ARGS)

test_upstream_skips:
	python3 scripts/test_upstream.py --build-dir $(UPSTREAM_BUILD_DIR) --jobs $(UPSTREAM_JOBS) --audit-skips $(UPSTREAM_ARGS)

test_upstream_harness:
	python3 test/upstream/test_harness.py --build-dir $(UPSTREAM_BUILD_DIR)

# S3 suites against a disposable, pinned MinIO container plus the HTTP fault harness.
test_minio:
	python3 test/integration/s3_faults.py
	python3 test/integration/minio.py

# A vcpkg checkout at VCPKG_COMMIT for local builds:
#   make vcpkg && VCPKG_TOOLCHAIN_PATH=$PWD/build/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make release
vcpkg:
	test -d build/vcpkg/.git || git clone --filter=blob:none https://github.com/microsoft/vcpkg build/vcpkg
	git -C build/vcpkg fetch --filter=blob:none origin $(VCPKG_COMMIT)
	git -C build/vcpkg checkout -q $(VCPKG_COMMIT)
	test -x build/vcpkg/vcpkg || build/vcpkg/bootstrap-vcpkg.sh -disableMetrics
