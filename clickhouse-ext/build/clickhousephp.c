#include <php.h>
#include <Zend/zend_API.h>
#include <Zend/zend_hash.h>
#include <Zend/zend_types.h>
#include <stddef.h>

#include "clickhousephp.h"
#include "clickhousephp_arginfo.h"
#include "_cgo_export.h"


PHP_MINIT_FUNCTION(clickhousephp) {
    
    return SUCCESS;
}

zend_module_entry clickhousephp_module_entry = {STANDARD_MODULE_HEADER,
                                         "clickhousephp",
                                         ext_functions,             /* Functions */
                                         PHP_MINIT(clickhousephp),  /* MINIT */
                                         NULL,                      /* MSHUTDOWN */
                                         NULL,                      /* RINIT */
                                         NULL,                      /* RSHUTDOWN */
                                         NULL,                      /* MINFO */
                                         "1.0.0",                   /* Version */
                                         STANDARD_MODULE_PROPERTIES};

PHP_FUNCTION(clickhouse_connect)
{
    zend_string *dsn = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(dsn)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_connect(dsn);
    if (result) {
        RETURN_STR(result);
    }

	RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_disconnect)
{
    if (zend_parse_parameters_none() == FAILURE) {
        RETURN_THROWS();
    }
    zend_string *result = clickhouse_disconnect();
    if (result) {
        RETURN_STR(result);
    }

	RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_insert)
{
    zend_string *table = NULL;
    zval *values = NULL;
    zval *columns = NULL;
    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STR(table)
        Z_PARAM_ARRAY(values)
        Z_PARAM_ARRAY(columns)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_insert(table, values, columns);
    if (result) {
        RETURN_STR(result);
    }

	RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_exec)
{
    zend_string *query = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(query)
    ZEND_PARSE_PARAMETERS_END();
    zend_string *result = clickhouse_exec(query);
    if (result) {
        RETURN_STR(result);
    }

	RETURN_EMPTY_STRING();
}

PHP_FUNCTION(clickhouse_query_array)
{
    zend_string *query = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(query)
    ZEND_PARSE_PARAMETERS_END();
    zend_array *result = clickhouse_query_array(query);
    if (result) {
        RETURN_ARR(result);
    }

	RETURN_EMPTY_ARRAY();
}

