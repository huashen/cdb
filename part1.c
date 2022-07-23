#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>

typedef enum {
    //对第一个字符为 '.' 原始输入命令的解析
    META_COMMAND_SUCCESS,
    // 不识别报错
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

// 识别InputBuffer 类型，成功则转成Statement对象
typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

// 操作类型
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;
