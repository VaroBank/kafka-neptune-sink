package com.varobank.common.gremlin.utils;

public class InvalidSchemaException extends Exception {

    public InvalidSchemaException(String errorMessage) {
        super(errorMessage);
    }

    public InvalidSchemaException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
