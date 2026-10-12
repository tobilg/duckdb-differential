# Integration build (`make integration`): build the loadable extension without linking it into
# the DuckDB CLI or libduckdb, so tests LOAD the same artifact users install.
duckdb_extension_load(differential
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    DONT_LINK
    EXTENSION_VERSION v0.1.0
)
