package com.qiu.core;

/**
 * 操作状态枚举
 */
public enum Status {
    OK("OK"),
    NOT_FOUND("Not found"),
    CORRUPTION("Data corruption"),
    IO_ERROR("IO error"),
    INVALID_ARGUMENT("Invalid argument"),
    NOT_SUPPORTED("Not supported");

    private final String message;

    Status(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public boolean isOk() {
        return this == OK;
    }

    public boolean isNotFound() {
        return this == NOT_FOUND;
    }

    public boolean isCorruption() {
        return this == CORRUPTION;
    }
}
