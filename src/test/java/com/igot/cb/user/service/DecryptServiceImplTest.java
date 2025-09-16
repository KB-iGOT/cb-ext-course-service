package com.igot.cb.user.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class DecryptServiceImplTest {

    private DecryptServiceImpl decryptService;

    @BeforeEach
    void setUp() {
        decryptService = new DecryptServiceImpl();
        ReflectionTestUtils.setField(decryptService, "sbChiperPassword", "testPassword");
    }

    @Test
    void testDecryptStringWithNullInput() {
        String result = decryptService.decryptString(null);
        assertNull(result);
    }

    @Test
    void testDecryptStringWithEmptyInput() {
        String result = decryptService.decryptString("");
        assertNull(result);
    }

    @Test
    void testDecryptStringWithInvalidInput() {
        String result = decryptService.decryptString("invalid_encrypted_string");
        assertNull(result);
    }

    @Test
    void testDecryptStringException() {
        String result = decryptService.decryptString("test@#$%");
        assertNull(result);
    }
}