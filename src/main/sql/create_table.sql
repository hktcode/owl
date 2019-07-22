--

CREATE SCHEMA "hktcode_owl";

CREATE TABLE "hktcode_owl"."scp_sql_script"
( "codename"  VARCHAR(64) NOT NULL
, "engineid"  VARCHAR(64) NOT NULL
, "execcode"         TEXT NOT NULL
, "argument"         TEXT NOT NULL DEFAULT ''
, "document"         TEXT NOT NULL DEFAULT ''
, "insertts" timestamp(3) NOT NULL DEFAULT now()
, PRIMARY KEY ("codename")
);
COMMENT ON TABLE  "hktcode_owl"."scp_sql_script" IS 'SQL语句配置表';
COMMENT ON COLUMN "hktcode_owl"."scp_sql_script"."codename" IS 'SQL语句名称';
COMMENT ON COLUMN "hktcode_owl"."scp_sql_script"."engineid" IS '执行引擎名称';
COMMENT ON COLUMN "hktcode_owl"."scp_sql_script"."execcode" IS 'SQL代码';
COMMENT ON COLUMN "hktcode_owl"."scp_sql_script"."argument" IS
    '参数列表. 半角逗号“,”（ASCII为0x2C）分隔，程序内部会处理参数名称的首尾空格';
COMMENT ON COLUMN "hktcode_owl"."scp_sql_script"."document" IS '描述信息';
COMMENT ON COLUMN "hktcode_owl"."scp_sql_script"."insertts" IS '创建时间';
ALTER TABLE "hktcode_owl"."scp_sql_script" REPLICA IDENTITY FULL;

CREATE TABLE "hktcode_owl"."job_cmp_update"
( "taskname" varchar(255) NOT NULL
, "relation" varchar(255) NOT NULL
, "oldtuple" varchar(255) NOT NULL
, "newtuple" varchar(255) NOT NULL
, "tuplekey" varchar(255) NOT NULL
, "document"         TEXT NOT NULL DEFAULT ''
, "isenable" varchar(255) NOT NULL DEFAULT 'EXECUTE'
, "insertts" timestamp(3) NOT NULL DEFAULT now()
, "putfield" varchar(255) NOT NULL DEFAULT 'log_create_datetime'
, "delfield" varchar(255) NOT NULL DEFAULT 'log_delete_datetime'
, "heritage" varchar(255) NOT NULL
, "relogger" varchar(255) NOT NULL
, PRIMARY KEY ("taskname")
);

COMMENT ON TABLE  "hktcode_owl"."job_cmp_update" IS '比较更新任务';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."taskname" IS '任务名称';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."relation" IS '目标表名称';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."oldtuple" IS '查询旧数据的SQL语句';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."newtuple" IS '查询新数据的SQL语句';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."tuplekey" IS
    '主键字段，以逗号分隔，如果一个字段出现多次，只保存第一次出现';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."putfield" IS '记录创建时间字段';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."delfield" IS '记录删除时间字段';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."document" IS '描述信息';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."isenable" IS
    '当前任务是否启用，EXECUTE--标识已经启用，其他（推荐SUCCESS或者FAILURE）表示未启用';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."insertts" IS '本记录写入时间';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."heritage" IS '遗产关系名称';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_update"."relogger" IS '记录更新日志的表名';
ALTER TABLE "hktcode_owl"."job_cmp_update" REPLICA IDENTITY FULL;

CREATE TABLE "hktcode_owl"."job_cmp_insert"
( "taskname" varchar(255) NOT NULL
, "metadata" varchar(255) NOT NULL
, "relation" varchar(255) NOT NULL
, "oldtuple" varchar(255) NOT NULL
, "newtuple" varchar(255) NOT NULL
, "tuplekey" varchar(255) NOT NULL
, "dmlfield" varchar(255) NOT NULL DEFAULT 'dmlfield'
, "isenable" varchar(255) NOT NULL DEFAULT 'EXECUTE'
, "insertts" timestamp(3) NOT NULL DEFAULT now()
, "document" varchar(255) NOT NULL DEFAULT ''
, PRIMARY KEY ("taskname")
);
COMMENT ON  TABLE "hktcode_owl"."job_cmp_insert" IS '比较写入任务';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."taskname" IS '任务名称';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."metadata" IS
    '主要relation名称，如果是空字符串，表示单独维护原数据';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."relation" IS '细节relation名称';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."oldtuple" IS '旧数据查询的SQL语句';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."newtuple" IS '查询新数据的SQL语句';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."tuplekey" IS
    '主键字段以逗号分隔，如果一个字段出现多次，只保存第一次出现';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."document" IS '描述信息';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."isenable" IS
    '当前任务是否启用，EXECUTE--标识已经启用，其他（推荐SUCCESS或者FAILURE）表示未启用';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."insertts" IS '本记录写入时间';
COMMENT ON COLUMN "hktcode_owl"."job_cmp_insert"."dmlfield" IS '主要relation记录DML操作的属性名称';
ALTER TABLE "hktcode_owl"."job_cmp_insert" REPLICA IDENTITY FULL;

CREATE TABLE "hktcode_owl"."job_exe_update"
( "taskname" varchar(255) NOT NULL
, "prevcode" varchar(255) NOT NULL DEFAULT ''
, "corecode" varchar(255) NOT NULL DEFAULT ''
, "postcode" varchar(255) NOT NULL DEFAULT ''
, "isenable" varchar(255) NOT NULL DEFAULT 'EXECUTE'
, "insertts" timestamp(3) NOT NULL DEFAULT now()
, "document"         TEXT NOT NULL DEFAULT ''
, PRIMARY KEY ("taskname")
);
COMMENT ON  TABLE "hktcode_owl"."job_exe_update" IS '剪切粘贴列表';
COMMENT ON COLUMN "hktcode_owl"."job_exe_update"."taskname" IS '任务名称';
COMMENT ON COLUMN "hktcode_owl"."job_exe_update"."prevcode" IS '预先执行的SQL语句名称，如果是空字符串，则不执行任何内容';
COMMENT ON COLUMN "hktcode_owl"."job_exe_update"."corecode" IS '需要执行的SQL语句名称，如果是空字符串，则不执行任何内容';
COMMENT ON COLUMN "hktcode_owl"."job_exe_update"."postcode" IS '后续执行的SQL语句名称，如果是空字符串，则不执行任何内容';
COMMENT ON COLUMN "hktcode_owl"."job_exe_update"."isenable" IS
    '当前任务是否启用，EXECUTE--标识已经启用，其他（推荐SUCCESS或者FAILURE）表示未启用';
COMMENT ON COLUMN "hktcode_owl"."job_exe_update"."insertts" IS '本记录写入时间';
COMMENT ON COLUMN "hktcode_owl"."job_exe_update"."document" IS '描述信息';
ALTER TABLE "hktcode_owl"."job_exe_update" REPLICA IDENTITY FULL;

CREATE TABLE "hktcode_owl"."job_exe_insert"
( "taskname" varchar(255) NOT NULL
, "prevcode" varchar(255) NOT NULL DEFAULT ''
, "corecode" varchar(255) NOT NULL DEFAULT ''
, "postcode" varchar(255) NOT NULL DEFAULT ''
, "isenable" varchar(255) NOT NULL DEFAULT 'EXECUTE'
, "insertts" timestamp(3) NOT NULL DEFAULT now()
, "document"         TEXT NOT NULL DEFAULT ''
, PRIMARY KEY ("taskname")
);
COMMENT ON  TABLE "hktcode_owl"."job_exe_insert" IS '简单任务列表';
COMMENT ON COLUMN "hktcode_owl"."job_exe_insert"."taskname" IS '任务名称';
COMMENT ON COLUMN "hktcode_owl"."job_exe_insert"."prevcode" IS '预先执行的SQL语句名称，如果是空字符串，则不执行任何内容';
COMMENT ON COLUMN "hktcode_owl"."job_exe_insert"."corecode" IS '需要执行的SQL语句名称，如果是空字符串，则不执行任何内容';
COMMENT ON COLUMN "hktcode_owl"."job_exe_insert"."postcode" IS '后续执行的SQL语句名称，如果是空字符串，则不执行任何内容';
COMMENT ON COLUMN "hktcode_owl"."job_exe_insert"."isenable" IS
    '当前任务是否启用，EXECUTE--标识已经启用，其他（推荐SUCCESS或者FAILURE）表示未启用';
COMMENT ON COLUMN "hktcode_owl"."job_exe_insert"."insertts" IS '本记录写入时间';
COMMENT ON COLUMN "hktcode_owl"."job_exe_insert"."document" IS '描述信息';
ALTER TABLE "hktcode_owl"."job_exe_insert" REPLICA IDENTITY FULL;


-- -- 更新日志relation结构，名称和schema可以自行定义
-- CREATE TABLE $$logger_change_schema$$."logger_relation_change"
-- ( "updatets" timestamp(0) NOT NULL
-- , "relation" varchar(255) NOT NULL
-- , "tuplekey"         text NOT NULL
-- , "property" varchar(255) NOT NULL
-- , "oldvalue"         text NOT NULL
-- , "newvalue"         text NOT NULL
-- , PRIMARY KEY("updatets", "relation", "tuplekey", "property")
-- ); -- partition by updatets or relation
--
-- COMMENT ON  TABLE $$logger_change_schema$$."logger_relation_change" IS '记录更新日志';
-- COMMENT ON COLUMN $$logger_change_schema$$."logger_relation_change"."updatets" IS '更新时间';
-- COMMENT ON COLUMN $$logger_change_schema$$."logger_relation_change"."tuplekey" IS 'tuple的主键';
-- COMMENT ON COLUMN $$logger_change_schema$$."logger_relation_change"."property" IS '更新的属性名';
-- COMMENT ON COLUMN $$logger_change_schema$$."logger_relation_change"."oldvalue" IS '更新前的值';
-- COMMENT ON COLUMN $$logger_change_schema$$."logger_relation_change"."newvalue" IS '更新后的值';
-- ALTER TABLE $$logger_change_schema$$."logger_relation_change" REPLICA IDENTITY FULL;
