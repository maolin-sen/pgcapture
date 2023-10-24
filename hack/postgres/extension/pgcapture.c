// source: https://github.com/enova/pgl_ddl_deploy/blob/master/pgl_ddl_deploy.c

#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "parser/parser.h"


/*
在许多操作系统上，包括 Linux 和类 Unix 系统，共享库的初始化是由链接器提供的初始化函数来实现的。这个初始化函数是通过特殊的链接器指令和操作系统的支持来实现的。

在 C/C++ 编译器中，可以使用特定的属性（attribute）或修饰符来指定函数在链接时的一些特殊行为。例如，在 GCC 中，__attribute__((constructor)) 属性可以应用于函数，
用于指定该函数在共享库加载时会自动调用。

这种机制的原理如下：

编译和链接时，编译器和链接器识别具有 __attribute__((constructor)) 属性的函数，并将它们标记为初始化函数。
链接器会生成特殊的区块（section），其中包含了所有初始化函数的地址和相关信息。
当操作系统加载共享库时，链接器提供的初始化函数会在共享库加载完成后被自动调用。操作系统会寻找初始化函数的地址，并按顺序调用每个初始化函数。
调用初始化函数时，它们会执行所需的初始化操作，如注册函数、设置全局变量等。
通过这种机制，插件的 _PG_init 函数可以被链接器标记为初始化函数，并在共享库加载过程中被自动调用。这样，插件就可以在加载时执行所需的初始化操作。

在插件的源代码文件中，通常会有以下这样的语句：
PG_MODULE_MAGIC;
这个宏会创建一个符号，_PG_init 函数会与该符号一起导出到共享库中，并由 PostgreSQL 在加载插件时使用。
*/

PG_MODULE_MAGIC;

/*
    PG_FUNCTION_INFO_V1 是一个宏，用于在 PostgreSQL 中创建用户自定义函数。这个宏为 PostgreSQL 提供了函数的元数据，如函数的名称和参数的数量和类型。

    当你在 C 语言中创建一个用户自定义函数时，你需要在函数的定义之前使用 PG_FUNCTION_INFO_V1 宏。例如：

        PG_FUNCTION_INFO_V1(my_function);

        Datum my_function(PG_FUNCTION_ARGS)
        {
            // 函数的实现
        }
    在这个例子中，my_function 是函数的名称，PG_FUNCTION_ARGS 是一个宏，表示函数的参数。

    PG_FUNCTION_INFO_V1 宏的名称中的 V1 表示它使用的是版本 1 的函数调用约定。这是 PostgreSQL 当前支持的唯一版本。

    需要注意的是，虽然 PG_FUNCTION_INFO_V1 宏为 PostgreSQL 提供了函数的元数据，但是你仍然需要在 SQL 中创建一个函数，以便在 SQL 查询中使用它。例如：

        CREATE FUNCTION my_function_xys(integer, integer) RETURNS integer
            AS 'MODULE_PATHNAME', 'my_function'
            LANGUAGE C STRICT;
    在这个例子中，my_function 是函数的名称，integer, integer 是函数的参数，integer 是函数的返回类型，'MODULE_PATHNAME' 是包含函数的模块的路径，
    'my_function' 是函数在 C 代码中的名称，LANGUAGE C 表示函数是用 C 语言编写的，STRICT 表示函数在任何参数为 NULL 时返回 NULL。

*/
PG_FUNCTION_INFO_V1(pgl_ddl_deploy_current_query);
PG_FUNCTION_INFO_V1(sql_command_tags);

/* Our own version of debug_query_string - see below */
const char *pgl_ddl_deploy_debug_query_string;

/*
 * A near-copy of the current_query postgres function which caches the value, ignoring
 * the change if the string is changed to NULL, as is being done in pglogical 2.2.2.
 * This allows multiple subsequent calls to pglogical.replicate_ddl_command without
 * losing access to current_query.
 *
 * Please revisit if pglogical changes behavior and stop setting debug_query_string to NULL.
 函数的逻辑如下：

 如果全局变量 debug_query_string（这是 PostgreSQL 内部用于存储当前查询字符串的变量）被设置了，那么函数就返回这个查询字符串。
 这个查询字符串被转换为 text 类型，然后通过 PG_RETURN_TEXT_P 宏返回。

 如果 debug_query_string 是 NULL，但是 pgl_ddl_deploy_debug_query_string（这是一个自定义的全局变量，可能被用于存储之前的查询字符串）被设置了，
 那么函数就返回 pgl_ddl_deploy_debug_query_string。这个查询字符串也被转换为 text 类型，然后通过 PG_RETURN_TEXT_P 宏返回。

 如果 debug_query_string 和 pgl_ddl_deploy_debug_query_string 都是 NULL，那么函数就返回 NULL。这是通过 PG_RETURN_NULL 宏实现的。

 这个函数可能被用于调试或审计，因为它可以让你知道当前正在执行的 SQL 查询是什么。
 */
Datum pgl_ddl_deploy_current_query(PG_FUNCTION_ARGS)
{
    /* If debug_query_string is set, we always want the same value */
    if (debug_query_string)
    {
        pgl_ddl_deploy_debug_query_string = debug_query_string;
        PG_RETURN_TEXT_P(cstring_to_text(pgl_ddl_deploy_debug_query_string));
    }
    /* If it is NULL, we want to take pgl_ddl_deploy_debug_query_string instead,
       which in most cases in this code path is used in pgl_ddl_deploy we expect
       is because pglogical has reset the string to null.  But we still want to
       return the same value in this SQL statement we are executing.
    */
    else if (pgl_ddl_deploy_debug_query_string)
    {
        PG_RETURN_TEXT_P(cstring_to_text(pgl_ddl_deploy_debug_query_string));
    }
    else
    /* If both are NULL, that is legit and we want to return NULL. */
    {
        PG_RETURN_NULL();
    }
}

/*
 * Return a text array of the command tags in SQL command
 用于解析 SQL 查询并返回每个子查询的命令标签。这个函数的名称是 sql_command_tags，它接受一个 text 类型的参数（通过 PG_GETARG_TEXT_P 宏获取），
 并返回一个 array 类型的值，这是 PostgreSQL 中的数组数据类型。

 函数的逻辑如下：

 首先，函数将输入的 text 类型的 SQL 查询转换为 C 字符串（通过 text_to_cstring 函数）。

 然后，函数解析这个 SQL 查询，得到一个解析树列表（通过 pg_parse_query 函数）。

 接着，函数遍历这个解析树列表。对于每个解析树，函数获取它的命令标签（通过 CreateCommandName 或 CreateCommandTag 函数，取决于 PostgreSQL 的版本），
 然后将这个命令标签添加到一个数组中（通过 accumArrayResult 函数）。

 如果没有解析树，函数将抛出一个错误（通过 elog 函数）。

 最后，函数返回这个数组（通过 makeArrayResult 和 PG_RETURN_ARRAYTYPE_P 宏）。

 命令标签是一个字符串，表示 SQL 查询的类型，如 "SELECT"、"UPDATE"、"INSERT" 等。这个函数可以用于分析 SQL 查询的结构，例如，你可以用它来检查一个 SQL 查询是否包含某种类型的子查询。
 */
Datum sql_command_tags(PG_FUNCTION_ARGS)
{
    text            *sql_t  = PG_GETARG_TEXT_P(0);
    char            *sql;
    List            *parsetree_list;
    ListCell        *parsetree_item;
    const char      *commandTag;
    ArrayBuildState *astate = NULL;

    /*
     * Get the SQL parsetree
     */
    sql = text_to_cstring(sql_t);
    parsetree_list = pg_parse_query(sql);

    /*
     * Iterate through each parsetree_item to get CommandTag
     */
    foreach(parsetree_item, parsetree_list)
    {
        Node    *parsetree = (Node *) lfirst(parsetree_item);
#if PG_VERSION_NUM >= 130000
        commandTag         = CreateCommandName(parsetree);
#else
        commandTag         = CreateCommandTag(parsetree);
#endif
        astate             = accumArrayResult(astate, CStringGetTextDatum(commandTag),
                             false, TEXTOID, CurrentMemoryContext);
    }
    if (astate == NULL)
                elog(ERROR, "Invalid sql command");
    PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate, CurrentMemoryContext));
}


