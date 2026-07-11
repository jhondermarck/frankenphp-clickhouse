/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 0a740cc65292a70c02a5787f7e257a7bde3ccc5e */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_connect, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, dsn, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_disconnect, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_insert, 0, 2, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, values, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, columns, IS_ARRAY, 1, "null")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, options, IS_ARRAY, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_exec, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 1, "null")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, options, IS_ARRAY, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_query_array, 0, 1, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 1, "null")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, options, IS_ARRAY, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_query_cursor, 0, 1, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 1, "null")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, options, IS_ARRAY, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_cursor_fetch, 0, 1, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, cursor, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, max_rows, IS_LONG, 0, "10000")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_cursor_close, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, cursor, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_ping, 0, 0, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, connection, IS_LONG, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_server_version, 0, 0, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, connection, IS_LONG, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_open, 0, 1, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, dsn, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_close, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_batch_begin, 0, 1, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, columns, IS_ARRAY, 1, "null")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, options, IS_ARRAY, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_batch_append, 0, 2, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, batch, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, values, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_batch_flush, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, batch, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_batch_send, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, batch, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_batch_abort, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, batch, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_async_insert, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, wait, _IS_BOOL, 0, "true")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 1, "null")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, options, IS_ARRAY, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_stats, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(clickhouse_connect);
ZEND_FUNCTION(clickhouse_disconnect);
ZEND_FUNCTION(clickhouse_insert);
ZEND_FUNCTION(clickhouse_exec);
ZEND_FUNCTION(clickhouse_query_array);
ZEND_FUNCTION(clickhouse_query_cursor);
ZEND_FUNCTION(clickhouse_cursor_fetch);
ZEND_FUNCTION(clickhouse_cursor_close);
ZEND_FUNCTION(clickhouse_ping);
ZEND_FUNCTION(clickhouse_server_version);
ZEND_FUNCTION(clickhouse_batch_begin);
ZEND_FUNCTION(clickhouse_batch_append);
ZEND_FUNCTION(clickhouse_batch_flush);
ZEND_FUNCTION(clickhouse_batch_send);
ZEND_FUNCTION(clickhouse_batch_abort);
ZEND_FUNCTION(clickhouse_async_insert);
ZEND_FUNCTION(clickhouse_open);
ZEND_FUNCTION(clickhouse_close);
ZEND_FUNCTION(clickhouse_stats);

static const zend_function_entry ext_functions[] = {
	ZEND_FE(clickhouse_connect, arginfo_clickhouse_connect)
	ZEND_FE(clickhouse_disconnect, arginfo_clickhouse_disconnect)
	ZEND_FE(clickhouse_insert, arginfo_clickhouse_insert)
	ZEND_FE(clickhouse_exec, arginfo_clickhouse_exec)
	ZEND_FE(clickhouse_query_array, arginfo_clickhouse_query_array)
	ZEND_FE(clickhouse_query_cursor, arginfo_clickhouse_query_cursor)
	ZEND_FE(clickhouse_cursor_fetch, arginfo_clickhouse_cursor_fetch)
	ZEND_FE(clickhouse_cursor_close, arginfo_clickhouse_cursor_close)
	ZEND_FE(clickhouse_ping, arginfo_clickhouse_ping)
	ZEND_FE(clickhouse_server_version, arginfo_clickhouse_server_version)
	ZEND_FE(clickhouse_batch_begin, arginfo_clickhouse_batch_begin)
	ZEND_FE(clickhouse_batch_append, arginfo_clickhouse_batch_append)
	ZEND_FE(clickhouse_batch_flush, arginfo_clickhouse_batch_flush)
	ZEND_FE(clickhouse_batch_send, arginfo_clickhouse_batch_send)
	ZEND_FE(clickhouse_batch_abort, arginfo_clickhouse_batch_abort)
	ZEND_FE(clickhouse_async_insert, arginfo_clickhouse_async_insert)
	ZEND_FE(clickhouse_open, arginfo_clickhouse_open)
	ZEND_FE(clickhouse_close, arginfo_clickhouse_close)
	ZEND_FE(clickhouse_stats, arginfo_clickhouse_stats)
	ZEND_FE_END
};
