package com.igot.cb.exceptions;

import com.igot.cb.transactional.util.exceptions.CustomException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CustomExceptionTest {

    @Test
    void testNoArgsConstructor() {
        CustomException ex = new CustomException();
        assertNull(ex.getCode());
        assertNull(ex.getMessage());
        assertNull(ex.getHttpStatusCode());
    }

    @Test
    void testAllArgsConstructor() {
        CustomException ex = new CustomException("ERR001", "Error occurred", HttpStatus.BAD_REQUEST);
        assertEquals("ERR001", ex.getCode());
        assertEquals("Error occurred", ex.getMessage());
        assertEquals(HttpStatus.BAD_REQUEST, ex.getHttpStatusCode());
    }

    @Test
    void testSettersAndGetters() {
        CustomException ex = new CustomException();
        ex.setCode("ERR002");
        ex.setMessage("Another error");
        ex.setHttpStatusCode(HttpStatus.NOT_FOUND);

        assertEquals("ERR002", ex.getCode());
        assertEquals("Another error", ex.getMessage());
        assertEquals(HttpStatus.NOT_FOUND, ex.getHttpStatusCode());
    }

}
