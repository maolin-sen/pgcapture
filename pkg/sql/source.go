package sql

// QueryAttrTypeOID 查询列名和列类型
var QueryAttrTypeOID = `SELECT nspname, relname, attname, atttypid
FROM pg_catalog.pg_namespace n
JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relkind = 'r'
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 and a.attisdropped = false
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pglogical') AND n.nspname !~ '^pg_toast';`

// QueryIdentityKeys 查询索引列
var QueryIdentityKeys = `SELECT
	nspname,
	relname,
	array(select attname from pg_catalog.pg_attribute where attrelid = i.indrelid AND attnum > 0 AND attnum = ANY(i.indkey)) as keys,
	array(select column_name::text from information_schema.columns where table_schema = n.nspname AND table_name = c.relname AND identity_generation IS NOT NULL) as identity_generation_columns,
	array(select column_name::text from information_schema.columns where table_schema = n.nspname AND table_name = c.relname AND is_generated = 'ALWAYS') as generated_columns
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indrelid AND c.relkind = 'r'
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pglogical') AND n.nspname !~ '^pg_toast'
WHERE (i.indisprimary OR i.indisunique) AND i.indisvalid AND i.indpred IS NULL ORDER BY indisprimary;`

//CreateLogicalSlot
/*
	pg_create_logical_replication_slot是一个PostgreSQL的函数，用于创建一个逻辑复制槽。这个函数需要两个参数：
	第一个参数是复制槽的名称。这个名称是一个字符串，用于标识复制槽。
	第二个参数是输出插件的名称。这个插件用于将数据从内部格式转换为复制协议可以理解的格式。
	这个函数的返回值是一个包含两个字段（槽名称和LSN）的行。LSN（Log Sequence Number）是一个表示复制开始位置的值。
*/
var CreateLogicalSlot = `SELECT pg_create_logical_replication_slot($1, $2);`

var CreatePublication = `CREATE PUBLICATION %s FOR ALL TABLES;`

var InstallExtension = `CREATE EXTENSION IF NOT EXISTS pgcapture;`

var ServerVersionNum = `SHOW server_version_num;`
