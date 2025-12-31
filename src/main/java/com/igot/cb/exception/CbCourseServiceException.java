package com.igot.cb.exception;

public class CbCourseServiceException extends RuntimeException {

    public CbCourseServiceException(String message) {
        super(message);
    }

    public CbCourseServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public CbCourseServiceException(Throwable cause) {
        super(cause);
    }
}
