# Integration build (`make integration`): build the loadable extension without linking it into
# the DuckDB CLI or libduckdb (DuckDB 2.0 links only what duckdb_extension_statically_link lists),
# so tests LOAD the same artifact users install.
duckdb_extension_load(differential
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    EXTENSION_VERSION v1.0.0
)

# Bundled dependencies used by the upstream SQLLogicTests; no extension downloads.
duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)
duckdb_extension_load(autocomplete)
duckdb_extension_statically_link(json icu tpch tpcds autocomplete)
duckdb_extension_load(upstream_bridge
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/test/upstream/bridge
)
