#include <php.h>
#include <Zend/zend_API.h>
#include <Zend/zend_hash.h>
#include <Zend/zend_types.h>
#include <Zend/zend_exceptions.h>
#include <ext/spl/spl_exceptions.h>
#include <stddef.h>
#include <string.h>

#include "clickhousephp.h"
#include "clickhousephp_arginfo.h"
#include "_cgo_export.h"

// Helper: if result starts with "ERROR: ", throw RuntimeException and return 1.
static int ch_throw_on_error(zend_string *result) {
    if (result && ZSTR_LEN(result) > 7 && memcmp(ZSTR_VAL(result), "ERROR: ", 7) == 0) {
        zend_throw_exception(spl_ce_RuntimeException, ZSTR_VAL(result) + 7, 0);
        zend_string_release(result);
        return 1;
    }
    return 0;
}

PHP_MINIT_FUNCTION(clickhousephp) {
    return SUCCESS;
}

zend_module_entry clickhousephp_module_entry = {STANDARD_MODULE_HEADER,
                                         "clickhousephp",
                                         ext_functions,
                                         PHP_MINIT(clickhousephp),
                                         NULL, NULL, NULL, NULL,
                                         "1.0.0",
                                         STANDARD_MODULE_PROPERTIES};

PHP_FUNCTION(clickhouse_connect)
{
    zend_string *dsn = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(dsn)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_connect(dsn);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_disconnect)
{
    if (zend_parse_parameters_none() == FAILURE) { RETURN_THROWS(); }
    zend_string *result = clickhouse_disconnect();
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_insert)
{
    zend_string *table = NULL;
    zval *values = NULL;
    zval *columns = NULL;
    zval *options = NULL;
    ZEND_PARSE_PARAMETERS_START(2, 4)
        Z_PARAM_STR(table)
        Z_PARAM_ARRAY(values)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_OR_NULL(columns)
        Z_PARAM_ARRAY_OR_NULL(options)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_insert(table, values, columns, options);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    // Also catch "Insert error" and "Send error" prefixes from Go
    if (result && (
        (ZSTR_LEN(result) > 13 && memcmp(ZSTR_VAL(result), "Insert error", 12) == 0) ||
        (ZSTR_LEN(result) > 10 && memcmp(ZSTR_VAL(result), "Send error", 10) == 0)
    )) {
        zend_throw_exception(spl_ce_RuntimeException, ZSTR_VAL(result), 0);
        zend_string_release(result);
        RETURN_THROWS();
    }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_exec)
{
    zend_string *query = NULL;
    zval *params = NULL;
    zval *options = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 3)
        Z_PARAM_STR(query)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_OR_NULL(params)
        Z_PARAM_ARRAY_OR_NULL(options)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_exec(query, params, options);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_query_array)
{
    zend_string *query = NULL;
    zval *params = NULL;
    zval *options = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 3)
        Z_PARAM_STR(query)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_OR_NULL(params)
        Z_PARAM_ARRAY_OR_NULL(options)
    ZEND_PARSE_PARAMETERS_END();
    zend_array *result = clickhouse_query_array(query, params, options);
    if (result == NULL) {
        zend_string *err = clickhouse_get_last_error();
        const char *msg = err ? ZSTR_VAL(err) : "ClickHouse query failed";
        zend_throw_exception(spl_ce_RuntimeException, msg, 0);
        if (err) { zend_string_release(err); }
        RETURN_THROWS();
    }
    RETURN_ARR(result);
}

PHP_FUNCTION(clickhouse_query_cursor)
{
    zend_string *query = NULL;
    zval *params = NULL;
    zval *options = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 3)
        Z_PARAM_STR(query)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_OR_NULL(params)
        Z_PARAM_ARRAY_OR_NULL(options)
    ZEND_PARSE_PARAMETERS_END();
    int64_t id = clickhouse_query_cursor(query, params, options);
    if (id < 0) {
        zend_string *err = clickhouse_get_last_error();
        const char *msg = err ? ZSTR_VAL(err) : "ClickHouse cursor open failed";
        zend_throw_exception(spl_ce_RuntimeException, msg, 0);
        if (err) { zend_string_release(err); }
        RETURN_THROWS();
    }
    RETURN_LONG((zend_long)id);
}

PHP_FUNCTION(clickhouse_cursor_fetch)
{
    zend_long id = 0;
    zend_long max_rows = 10000;
    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_LONG(id)
        Z_PARAM_OPTIONAL
        Z_PARAM_LONG(max_rows)
    ZEND_PARSE_PARAMETERS_END();
    zend_array *result = clickhouse_cursor_fetch((int64_t)id, (int64_t)max_rows);
    if (result == NULL) {
        zend_string *err = clickhouse_get_last_error();
        const char *msg = err ? ZSTR_VAL(err) : "ClickHouse cursor fetch failed";
        zend_throw_exception(spl_ce_RuntimeException, msg, 0);
        if (err) { zend_string_release(err); }
        RETURN_THROWS();
    }
    RETURN_ARR(result);
}

PHP_FUNCTION(clickhouse_cursor_close)
{
    zend_long id = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(id)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_cursor_close((int64_t)id);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_ping)
{
    if (zend_parse_parameters_none() == FAILURE) { RETURN_THROWS(); }
    zend_string *result = clickhouse_ping();
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_server_version)
{
    if (zend_parse_parameters_none() == FAILURE) { RETURN_THROWS(); }
    zend_string *result = clickhouse_server_version();
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_batch_begin)
{
    zend_string *table = NULL;
    zval *columns = NULL;
    zval *options = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 3)
        Z_PARAM_STR(table)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_OR_NULL(columns)
        Z_PARAM_ARRAY_OR_NULL(options)
    ZEND_PARSE_PARAMETERS_END();
    int64_t id = clickhouse_batch_begin(table, columns, options);
    if (id < 0) {
        zend_string *err = clickhouse_get_last_error();
        const char *msg = err ? ZSTR_VAL(err) : "ClickHouse batch open failed";
        zend_throw_exception(spl_ce_RuntimeException, msg, 0);
        if (err) { zend_string_release(err); }
        RETURN_THROWS();
    }
    RETURN_LONG((zend_long)id);
}

PHP_FUNCTION(clickhouse_batch_append)
{
    zend_long id = 0;
    zval *values = NULL;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_LONG(id)
        Z_PARAM_ARRAY(values)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_batch_append((int64_t)id, values);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_batch_flush)
{
    zend_long id = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(id)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_batch_flush((int64_t)id);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_batch_send)
{
    zend_long id = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(id)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_batch_send((int64_t)id);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_batch_abort)
{
    zend_long id = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(id)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_batch_abort((int64_t)id);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_async_insert)
{
    zend_string *query = NULL;
    bool wait = 1;
    zval *params = NULL;
    zval *options = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 4)
        Z_PARAM_STR(query)
        Z_PARAM_OPTIONAL
        Z_PARAM_BOOL(wait)
        Z_PARAM_ARRAY_OR_NULL(params)
        Z_PARAM_ARRAY_OR_NULL(options)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_async_insert(query, wait ? 1 : 0, params, options);
    if (ch_throw_on_error(result)) { RETURN_THROWS(); }
    if (result) { RETURN_STR(result); }
    RETURN_EMPTY_STRING();
}
