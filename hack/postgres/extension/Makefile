# source: https://github.com/enova/pgl_ddl_deploy/blob/master/Makefile
#EXTENSION 指定扩展的名称。
#DATA 列出了扩展的 SQL 脚本文件。
#MODULES 指定了扩展的主模块。
EXTENSION = pgcapture
DATA = pgcapture--0.1.sql
MODULES = pgcapture

#PG_CONFIG 是 pg_config 可执行文件的路径，它通常用于查询 PostgreSQL 安装的配置设置。
#PGXS 被赋值为运行 $(PG_CONFIG) --pgxs 所返回的值，它从 PostgreSQL 安装中获取 PostgreSQL 扩展 Makefile。
#include $(PGXS) 包含了所提取的扩展 Makefile，该文件处理标准的构建和安装操作。
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Prevent unintentional inheritance of PGSERVICE while running regression suite
# with make installcheck.  We typically use PGSERVICE in our shell environment but
# not for dev. Require instead explicit PGPORT= or PGSERVICE= to do installcheck
#这行代码在构建过程中取消了 PGSERVICE 环境变量的导出，以防止意外的继承。这样可以确保在使用 make installcheck 运行回归测试时，PGSERVICE 变量不会被传递。
unexport PGSERVICE

#总体而言，该 Makefile 提供了一种使用 PostgreSQL 扩展基础设施进行构建和安装 pgcapture 扩展的标准方法。