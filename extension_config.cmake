# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(differential
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    EXTENSION_VERSION v1.1.0
)
# DuckDB 2.0 builds extensions without linking them unless listed here: link it into the CLI.
duckdb_extension_statically_link(differential)
