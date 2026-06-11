/**
 * DuckDB SQL dialect for CodeMirror's @codemirror/lang-sql.
 *
 * Extends PostgreSQL's keyword set with DuckDB-specific:
 *   - Types: HUGEINT, UBIGINT, UINTEGER, USMALLINT, UTINYINT, TINYINT,
 *            VARINT, BLOB, BITSTRING, MAP, STRUCT, UNION, LIST, ARRAY,
 *            TIMESTAMPTZ, TIMESTAMP_S/MS/NS, INTERVAL, ENUM, VARIANT
 *   - Keywords: EXCLUDE, REPLACE, PIVOT, UNPIVOT, QUALIFY, SAMPLE,
 *               TABLESAMPLE, POSITIONAL, ASOF, ANTI, SEMI, NATURAL,
 *               PRAGMA, COPY, EXPORT, INSTALL, LOAD, ATTACH, DETACH,
 *               USE, SUMMARIZE, DESCRIBE
 *   - Builtins (functions): scalar, aggregate, table, window, and
 *     DuckDB-specific functions (list_*, map_*, struct_*, regexp_*,
 *     read_csv, read_parquet, read_json, glob, age, epoch, etc.)
 */

import { SQLDialect, PostgreSQL } from '@codemirror/lang-sql'

// ── Extra types ───────────────────────────────────────────────────────────────

const DUCKDB_TYPES = [
  // Integers
  'tinyint', 'int8', 'int16', 'int32', 'int64', 'int128',
  'utinyint', 'usmallint', 'uinteger', 'ubigint', 'uhugeint', 'hugeint', 'varint',
  // Floats
  'float4', 'float8', 'decimal',
  // Text / binary
  'blob', 'bytea', 'varchar', 'text', 'string', 'bitstring',
  // Date/time
  'timestamptz', 'timestamp_s', 'timestamp_ms', 'timestamp_ns', 'timetz', 'interval',
  // Nested / complex
  'list', 'array', 'struct', 'map', 'union', 'enum',
  // DuckDB special
  'variant', 'json',
]

// ── Extra keywords ────────────────────────────────────────────────────────────

const DUCKDB_KEYWORDS = [
  // Clauses
  'qualify', 'exclude', 'replace', 'sample', 'tablesample',
  // Pivoting
  'pivot', 'unpivot',
  // Join types
  'positional', 'asof', 'anti', 'semi',
  // Management
  'pragma', 'copy', 'export', 'import', 'install', 'load',
  'attach', 'detach', 'use',
  // Introspection
  'summarize', 'describe', 'show',
  // Lambda
  'lambda',
  // Miscellaneous
  'format', 'compression', 'partition', 'by', 'header',
]

// ── Builtin functions ─────────────────────────────────────────────────────────

const DUCKDB_BUILTINS = [
  // ── Aggregate ──────────────────────────────────────────────────────────────
  'count', 'sum', 'avg', 'min', 'max',
  'count_star', 'arbitrary', 'first', 'last', 'any_value',
  'bit_and', 'bit_or', 'bit_xor', 'bool_and', 'bool_or',
  'corr', 'covar_pop', 'covar_samp', 'regr_avgx', 'regr_avgy',
  'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope',
  'regr_sxx', 'regr_sxy', 'regr_syy',
  'entropy', 'kurtosis', 'kurtosis_pop', 'mad', 'median',
  'mode', 'quantile', 'quantile_cont', 'quantile_disc',
  'percentile_cont', 'percentile_disc',
  'skewness', 'stddev', 'stddev_pop', 'stddev_samp',
  'variance', 'var_pop', 'var_samp',
  'product', 'sumKahan',
  'string_agg', 'group_concat', 'listagg',
  'array_agg', 'list',
  'histogram',
  'approx_count_distinct', 'approx_quantile',
  'reservoir_quantile',

  // ── Window ─────────────────────────────────────────────────────────────────
  'row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist',
  'ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value',

  // ── String ─────────────────────────────────────────────────────────────────
  'concat', 'concat_ws', 'format', 'printf', 'string_format',
  'length', 'strlen', 'array_length',
  'lower', 'upper', 'trim', 'ltrim', 'rtrim', 'strip_accents',
  'lpad', 'rpad',
  'left', 'right', 'substr', 'substring',
  'instr', 'position',
  'starts_with', 'ends_with', 'contains',
  'prefix', 'suffix',
  'like_escape', 'ilike_escape',
  'replace', 'translate',
  'repeat', 'reverse',
  'split', 'split_part', 'string_split', 'string_split_regex', 'str_split', 'str_split_regex',
  'regexp_matches', 'regexp_extract', 'regexp_extract_all',
  'regexp_replace', 'regexp_split_to_array', 'regexp_full_match',
  'regexp_split_to_table',
  'to_base64', 'from_base64', 'base64',
  'encode', 'decode',
  'md5', 'sha256',
  'unicode', 'chr', 'ord', 'ascii',
  'ucase', 'lcase',
  'bar', 'pad',
  'editdist3', 'hamming', 'levenshtein', 'jaccard', 'jaro_winkler',
  'mismatches',
  'nfc_normalize',
  'printf',
  'array_to_string', 'list_to_string',
  'parse_ident',

  // ── Math ───────────────────────────────────────────────────────────────────
  'abs', 'ceil', 'ceiling', 'floor', 'round', 'trunc', 'truncate',
  'sqrt', 'cbrt', 'exp', 'ln', 'log', 'log2', 'log10',
  'pow', 'power',
  'mod', 'div',
  'sign', 'signum',
  'pi', 'factorial',
  'degrees', 'radians',
  'sin', 'cos', 'tan', 'asin', 'acos', 'atan', 'atan2',
  'sinh', 'cosh', 'tanh', 'asinh', 'acosh', 'atanh',
  'bit_count', 'bit_length',
  'gcd', 'lcm',
  'greatest', 'least',
  'random', 'setseed',
  'isnan', 'isinf', 'isfinite',
  'nanvl', 'nextafter',

  // ── Date / time ────────────────────────────────────────────────────────────
  'now', 'current_date', 'current_time', 'current_timestamp',
  'today', 'yesterday', 'tomorrow',
  'date_part', 'datepart', 'extract',
  'date_trunc', 'datetrunc', 'time_bucket',
  'date_diff', 'datediff', 'date_sub', 'date_add',
  'age', 'epoch', 'epoch_ms', 'epoch_us', 'epoch_ns',
  'to_timestamp', 'from_unixtime',
  'make_timestamp', 'make_timestamptz', 'make_date', 'make_time', 'make_interval',
  'strftime', 'strptime',
  'century', 'decade', 'quarter',
  'yearweek', 'isodow', 'isoyear',
  'timezone', 'at_time_zone',
  'julianday',

  // ── Type conversion / casting ──────────────────────────────────────────────
  'cast', 'try_cast', 'typeof', 'type_of',
  'coalesce', 'ifnull', 'nullif', 'if', 'iff', 'iif',
  'nvl', 'nvl2', 'zeroifnull', 'nullifzero',

  // ── Conditional ────────────────────────────────────────────────────────────
  'case', 'decode', 'greatest', 'least',

  // ── List / array ───────────────────────────────────────────────────────────
  'list_value', 'list_pack',
  'list_concat', 'list_cat', 'array_concat', 'array_cat',
  'list_append', 'array_append', 'array_push_back',
  'list_prepend', 'array_prepend', 'array_push_front',
  'list_contains', 'list_has', 'array_contains',
  'list_position', 'list_indexof', 'array_position',
  'list_count',
  'list_distinct', 'list_unique',
  'list_except', 'list_intersect', 'list_union',
  'list_aggregate', 'list_agg',
  'list_apply', 'list_transform', 'list_filter', 'list_reduce',
  'list_sort', 'list_reverse_sort', 'list_reverse',
  'list_slice', 'array_slice',
  'list_extract', 'list_element', 'array_extract',
  'list_first', 'list_last',
  'list_flatten', 'flatten',
  'list_grade_up', 'list_grade_down',
  'list_resize', 'array_resize',
  'generate_series', 'range',
  'unnest', 'flatten',

  // ── Struct ─────────────────────────────────────────────────────────────────
  'struct_pack', 'row',
  'struct_extract', 'struct_insert',
  'struct_keys', 'struct_values',
  'union_value', 'union_tag',

  // ── Map ────────────────────────────────────────────────────────────────────
  'map', 'map_from_entries', 'map_entries', 'map_keys', 'map_values',
  'map_contains', 'map_extract', 'map_element',
  'cardinality',

  // ── JSON ───────────────────────────────────────────────────────────────────
  'to_json', 'from_json', 'json', 'json_valid',
  'json_extract', 'json_extract_string', 'json_value', 'json_query',
  'json_keys', 'json_object_keys',
  'json_object', 'json_array', 'json_merge_patch',
  'json_contains', 'json_structure', 'json_type',
  'json_transform', 'json_transform_strict',
  'json_array_length',
  'json_serialize_sql', 'json_deserialize_sql',

  // ── Bitstring ──────────────────────────────────────────────────────────────
  'bitstring', 'get_bit', 'set_bit', 'bit_count', 'bit_position',
  'octet_length',

  // ── Numeric formatting ─────────────────────────────────────────────────────
  'to_hex', 'from_hex', 'hex',

  // ── Null handling ──────────────────────────────────────────────────────────
  'coalesce', 'nullif', 'ifnull', 'if',

  // ── Table functions ────────────────────────────────────────────────────────
  'read_csv', 'read_csv_auto', 'csv_scan',
  'read_parquet',
  'read_json', 'read_json_auto', 'read_ndjson', 'read_ndjson_auto',
  'glob', 'glob_sql',
  'read_text',

  // ── Aggregate statistics ───────────────────────────────────────────────────
  'approx_count_distinct', 'approx_quantile',
  'fsum', 'favg',

  // ── DuckDB / system ────────────────────────────────────────────────────────
  'version', 'current_setting', 'get_current_timestamp',
  'txid_current', 'nextval',
  'hash', 'md5_number', 'murmurhash',
  'gen_random_uuid', 'uuid',
  'current_schema', 'current_database',
  'error', 'printf',
  'duckdb_tables', 'duckdb_views', 'duckdb_columns', 'duckdb_types',
  'duckdb_schemas', 'duckdb_databases', 'duckdb_extensions',
  'duckdb_settings', 'duckdb_functions', 'duckdb_sequences',
  'duckdb_indexes', 'duckdb_constraints',
  'checkpoint',
  'scan_arrow', 'scan_parquet',
  'parquet_schema', 'parquet_metadata',
]

// ── Build dialect ─────────────────────────────────────────────────────────────

const pgSpec = PostgreSQL.spec

const mergedKeywords = [
  ...new Set([
    ...(pgSpec.keywords ?? '').split(/\s+/).filter(Boolean),
    ...DUCKDB_KEYWORDS,
  ]),
].join(' ')

const mergedTypes = [
  ...new Set([
    ...(pgSpec.types ?? '').split(/\s+/).filter(Boolean),
    ...DUCKDB_TYPES,
  ]),
].join(' ')

const mergedBuiltin = [
  ...new Set([
    ...(pgSpec.builtin ?? '').split(/\s+/).filter(Boolean),
    ...DUCKDB_BUILTINS,
  ]),
].join(' ')

/** Re-export so consumers import both dialects from one place. */
export { PostgreSQL } from '@codemirror/lang-sql'

export const DuckDB = SQLDialect.define({
  ...pgSpec,
  keywords: mergedKeywords,
  types: mergedTypes,
  builtin: mergedBuiltin,
})
