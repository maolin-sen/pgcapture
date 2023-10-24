-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgcapture" to load this file. \quit

CREATE TABLE pgcapture.ddl_logs (id SERIAL PRIMARY KEY, query TEXT, tags TEXT[], activity JSONB);
CREATE TABLE pgcapture.sources (id TEXT PRIMARY KEY, commit pg_lsn, seq int, commit_ts timestamptz, mid bytea, apply_ts timestamptz DEFAULT CURRENT_TIMESTAMP);

CREATE FUNCTION pgcapture.current_query()
    RETURNS TEXT AS
    'MODULE_PATHNAME', 'pgl_ddl_deploy_current_query'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pgcapture.sql_command_tags(p_sql TEXT)
    RETURNS TEXT[] AS
    'MODULE_PATHNAME', 'sql_command_tags'
LANGUAGE C VOLATILE STRICT;

-- 函数的逻辑如下：
--
-- 首先，函数调用 pgcapture.current_query() 来获取当前执行的 SQL 查询字符串，并将其存储在 qstr 变量中。
--
-- 然后，函数调用 pgcapture.sql_command_tags(qstr) 来获取 SQL 查询的命令标签，并将其存储在 tags 数组中。
--
-- 接着，函数从 pg_stat_activity 视图中选择一些信息（数据库名、用户名、应用程序名、客户端地址、后端启动时间和事务开始时间），
-- 并将这些信息转换为 JSON 格式，然后存储在 acti 变量中。这个选择操作只针对当前的后端进程（通过 pg_backend_pid() 函数获取）。
--
-- 最后，函数将 qstr、tags 和 acti 插入到 pgcapture.ddl_logs 表中。
--
-- 这个函数可能被用于审计或监控，因为它可以记录每个 DDL（数据定义语言）操作的详细信息，包括操作的类型、执行操作的用户和操作的时间等。这个函数可能是一个事件触发器，
-- 当一个 DDL 事件发生时，它会被自动调用。

CREATE FUNCTION pgcapture.log_ddl() RETURNS event_trigger AS $$
declare
qstr TEXT;
tags TEXT[];
acti JSONB;
begin
qstr = pgcapture.current_query();
tags = pgcapture.sql_command_tags(qstr);
select row_to_json(a.*) into acti from (select datname,usename,application_name,client_addr,backend_start,xact_start from pg_stat_activity where pid = pg_backend_pid()) a;
insert into pgcapture.ddl_logs(query, tags, activity) values (qstr,tags,acti);
end;
$$ LANGUAGE plpgsql STRICT;

-- 创建了一个名为 pgcapture_ddl_command_start 的事件触发器。事件触发器是 PostgreSQL 的一个特性，它允许你在某些系统事件发生时自动执行一些操作。
--
-- 这个事件触发器的逻辑如下：
--
-- 它监听 ddl_command_start 事件，这个事件在任何数据定义语言（DDL）命令开始执行时发生。
--
-- 它只在特定的命令标签出现时触发，这些命令标签包括 'CREATE TABLE AS'、'SELECT INTO'、'DROP TRIGGER' 和 'DROP FUNCTION'。
--
-- 当触发器被触发时，它执行 pgcapture.log_ddl() 函数。这个函数可能会记录一些信息，例如执行的 SQL 查询、命令标签和一些关于后端进程的信息。
--
-- 这个事件触发器可以用于审计或监控，因为它可以帮助你跟踪和记录特定类型的 DDL 操作。例如，你可以用它来检查是否有人尝试删除一个触发器或函数，或者是否有人尝试创建一个新的表。
CREATE EVENT TRIGGER pgcapture_ddl_command_start ON ddl_command_start WHEN tag IN (
    'CREATE TABLE AS',
    'SELECT INTO',
    'DROP TRIGGER',
    'DROP FUNCTION'
) EXECUTE PROCEDURE pgcapture.log_ddl();

-- 创建了一个名为 pgcapture_ddl_command_end 的事件触发器。事件触发器是 PostgreSQL 的一个特性，它允许你在某些系统事件发生时自动执行一些操作。
--
-- 这个事件触发器的逻辑如下：
--
-- 它监听 ddl_command_end 事件，这个事件在任何数据定义语言（DDL）命令执行结束时发生。
--
-- 它只在特定的命令标签出现时触发，这些命令标签包括 'ALTER AGGREGATE'、'ALTER COLLATION'、'ALTER CONVERSION'、'ALTER DOMAIN' 等等，总共有大约 80 个不同的命令标签。
--
-- 当触发器被触发时，它执行 pgcapture.log_ddl() 函数。这个函数可能会记录一些信息，例如执行的 SQL 查询、命令标签和一些关于后端进程的信息。
--
-- 这个事件触发器可以用于审计或监控，因为它可以帮助你跟踪和记录特定类型的 DDL 操作。例如，你可以用它来检查是否有人尝试修改一个表或视图，或者是否有人尝试创建或删除一个索引。
CREATE EVENT TRIGGER pgcapture_ddl_command_end ON ddl_command_end WHEN TAG IN (
    'ALTER AGGREGATE',
    'ALTER COLLATION',
    'ALTER CONVERSION',
    'ALTER DOMAIN',
    'ALTER DEFAULT PRIVILEGES',
    'ALTER EXTENSION',
    'ALTER FOREIGN DATA WRAPPER',
    'ALTER FOREIGN TABLE',
    'ALTER FUNCTION',
    'ALTER LANGUAGE',
    'ALTER LARGE OBJECT',
    'ALTER MATERIALIZED VIEW',
    'ALTER OPERATOR',
    'ALTER OPERATOR CLASS',
    'ALTER OPERATOR FAMILY',
    'ALTER POLICY',
    'ALTER SCHEMA',
    'ALTER SEQUENCE',
    'ALTER SERVER',
    'ALTER TABLE',
    'ALTER TEXT SEARCH CONFIGURATION',
    'ALTER TEXT SEARCH DICTIONARY',
    'ALTER TEXT SEARCH PARSER',
    'ALTER TEXT SEARCH TEMPLATE',
    'ALTER TRIGGER',
    'ALTER TYPE',
    'ALTER USER MAPPING',
    'ALTER VIEW',
    'COMMENT',
    'CREATE ACCESS METHOD',
    'CREATE AGGREGATE',
    'CREATE CAST',
    'CREATE COLLATION',
    'CREATE CONVERSION',
    'CREATE DOMAIN',
    'CREATE EXTENSION',
    'CREATE FOREIGN DATA WRAPPER',
    'CREATE FOREIGN TABLE',
    'CREATE FUNCTION',
    'CREATE INDEX',
    'CREATE LANGUAGE',
    'CREATE MATERIALIZED VIEW',
    'CREATE OPERATOR',
    'CREATE OPERATOR CLASS',
    'CREATE OPERATOR FAMILY',
    'CREATE POLICY',
    'CREATE RULE',
    'CREATE SCHEMA',
    'CREATE SEQUENCE',
    'CREATE SERVER',
    'CREATE TABLE',
    'CREATE TEXT SEARCH CONFIGURATION',
    'CREATE TEXT SEARCH DICTIONARY',
    'CREATE TEXT SEARCH PARSER',
    'CREATE TEXT SEARCH TEMPLATE',
    'CREATE TRIGGER',
    'CREATE TYPE',
    'CREATE USER MAPPING',
    'CREATE VIEW',
    'DROP ACCESS METHOD',
    'DROP AGGREGATE',
    'DROP CAST',
    'DROP COLLATION',
    'DROP CONVERSION',
    'DROP DOMAIN',
    'DROP EXTENSION',
    'DROP FOREIGN DATA WRAPPER',
    'DROP FOREIGN TABLE',
    'DROP INDEX',
    'DROP LANGUAGE',
    'DROP MATERIALIZED VIEW',
    'DROP OPERATOR',
    'DROP OPERATOR CLASS',
    'DROP OPERATOR FAMILY',
    'DROP OWNED',
    'DROP POLICY',
    'DROP RULE',
    'DROP SCHEMA',
    'DROP SEQUENCE',
    'DROP SERVER',
    'DROP TABLE',
    'DROP TEXT SEARCH CONFIGURATION',
    'DROP TEXT SEARCH DICTIONARY',
    'DROP TEXT SEARCH PARSER',
    'DROP TEXT SEARCH TEMPLATE',
    'DROP TYPE',
    'DROP USER MAPPING',
    'DROP VIEW',
    'GRANT',
    'IMPORT FOREIGN SCHEMA',
    'REVOKE',
    'SECURITY LABEL'
) EXECUTE PROCEDURE pgcapture.log_ddl();