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

// 执行结果状态码
typedef enum {
   EXECUTE_SUCCESS,
   EXECUTE_DUPLICATE_KEY,
   EXECUTE_FULL_TABLE
} ExecuteResult;

// 输入原文的结构
typedef struct {
    char* buffer;
    uint32_t buffer_length;
    uint32_t str_length;
} InputBuffer;

InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->str_length = 0;
    input_buffer->buffer_length = 0;
    return input_buffer;
}

void del_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

//指明username 大小为32字节
const uint32_t COLUMN_USERNAME = 32;

//指明email 大小为255字节
const uint32_t COLUMN_EMAIL = 255;

//Row 代表写入的表类型结构 |<4>id|<33>(username)|<256>(email)|
typedef struct {
    uint32_t id;
    // +1 是为了gei '\0' 保留一位
    char username[COLUMN_USERNAME + 1];
    char email[COLUMN_EMAIL + 1];
} Row;

// Statement 对象为操作对象
typedef struct {
  StatementType type;
  Row row_to_insert;
} Statement;

#define FORLESS(less) for (int i =0; i < less; i++)
// 查看属性大小
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

//ROW_SIZE指真实数据大小
const uint32_t ID_OFFSET = 0;
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;//4
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username); // 33
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;// 37 = 4 + 33
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email); //256
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; //293 = 4 + 33 + 256

const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100; //最大100页
const uint32_t ROW_PER_PAGES = PAGE_SIZE / ROW_SIZE; // 13 = 4096 / 293
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROW_PER_PAGES; //1300

