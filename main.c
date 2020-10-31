#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <memory.h>

typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
} Statement;

/****函数定义*****/
void print_prompt();

InputBuffer *new_input_buffer();

void read_input(InputBuffer *buffer);

void close_input_buffer(InputBuffer *buffer);

MetaCommandResult do_meta_command(InputBuffer *buffer);

void execute_statement(Statement* statement);

PrepareResult prepare_statement(InputBuffer *buffer, Statement *ptr);

/**
 * 程序入口
 *
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char *argv[]) {
    InputBuffer *input_buffer = new_input_buffer();
    //循环接收命令行参数
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (strcmp(input_buffer->buffer, ".exit") == 0) {
           close_input_buffer(input_buffer);
           exit(EXIT_SUCCESS);
        }
        if(input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command '%s'.\n", input_buffer->buffer);
                    continue;
            }
        }

        //执行sql语句
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s' .\n", input_buffer->buffer);
                continue;
        }

        execute_statement(&statement);
        printf("Executed.\n");
    }
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }

    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            printf("This is where we would do an insert.\n");
            break;
        case STATEMENT_SELECT:
            printf("This is where we would do an select.\n");
            break;
    }
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

void close_input_buffer(InputBuffer *input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

void read_input(InputBuffer *input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

InputBuffer *new_input_buffer() {
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

//命令提示
void print_prompt() {
    printf("db >");
}
