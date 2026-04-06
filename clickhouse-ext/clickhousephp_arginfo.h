/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 0a740cc65292a70c02a5787f7e257a7bde3ccc5e */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_connect, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, dsn, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_disconnect, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_insert, 0, 3, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, values, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, columns, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_exec, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_query_array, 0, 1, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(clickhouse_connect);
ZEND_FUNCTION(clickhouse_disconnect);
ZEND_FUNCTION(clickhouse_insert);
ZEND_FUNCTION(clickhouse_exec);
ZEND_FUNCTION(clickhouse_query_array);

static const zend_function_entry ext_functions[] = {
	ZEND_FE(clickhouse_connect, arginfo_clickhouse_connect)
	ZEND_FE(clickhouse_disconnect, arginfo_clickhouse_disconnect)
	ZEND_FE(clickhouse_insert, arginfo_clickhouse_insert)
	ZEND_FE(clickhouse_exec, arginfo_clickhouse_exec)
	ZEND_FE(clickhouse_query_array, arginfo_clickhouse_query_array)
	ZEND_FE_END
};
