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

//页面属性
typedef struct {
    void* pages[TABLE_MAX_PAGES];
    int file_descriptor;
    int file_length;
} Pager;

//Table属性
typedef struct {
    //保存页面数据
    Pager* pager;
    //记录当前已有的row数据量，读入/写入
    uint32_t row_nums;
} Table;

/**数据库操作相关方法**/
void del_table(Table* table) {
    free(table->pager);
    table->pager = NULL;
    free(table);
}

//根据打开的文件，返回出Table上下文
Table* db_open(const char* filename);

void print_row(Row* row) {
    printf("(%d %s %s)\n", row->id, row->username, row->email);
}

//读取用户输入
void read_line(InputBuffer* input_buffer) {
    ssize_t  bytes_read = getline(&input_buffer->buffer, &input_buffer->buffer_length, stdin);
    if (bytes_read == -1) {
        printf("get user input error: %s.\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
    input_buffer->buffer[strlen(input_buffer->buffer) - 1] = '\0';
    input_buffer->str_length = strlen(input_buffer->buffer);
}

void db_close(Table* table) {
//    Pager* pager = table->pager;

}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    //模拟退出时保存数据
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    return META_COMMAND_UNRECOGNIZED;
}

Pager* pager_open(const char* filename) {
    int fd = open(filename, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        printf("open file: %s error: %s.\n", filename, strerror(errno));
        exit(EXIT_FAILURE);
    }

    size_t read_bytes = lseek(fd, 0, SEEK_END);
    if (read_bytes == -1) {
        printf("seek file: %s error: %s. \n", filename, strerror(errno));
        exit(EXIT_FAILURE);
    }
    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = read_bytes;
    FORLESS(TABLE_MAX_PAGES) {pager -> pages[i] = NULL;}
    return pager;
}

Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    int num_rows = pager->file_length / ROW_SIZE;

    Table* table = malloc(sizeof(Table));
    table->pager = pager;
    table->row_nums = num_rows;
    return table;
}

PrepareResult  prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    static char* token = " ";
    strtok(input_buffer->buffer, token);
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
}



/**
 * 主入口
 *
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char** argv) {
    char* filename;
    if (argc > 1) {
        filename = argv[1];
    }

    Table* table = db_open(filename);

    while (true) {
        printf("> ");
        InputBuffer* input_buffer = new_input_buffer();
        read_line(input_buffer);

        //对原字符进行识别是否是辅助指令
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED:
                    printf("unrecognized command: %s.\n", input_buffer->buffer);
                    continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
        }
    }
}



