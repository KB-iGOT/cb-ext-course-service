package com.igot.cb.util;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Constructor;
import static org.junit.jupiter.api.Assertions.*;

class ConstantsTest {

    @Test
    void testPrivateConstructor() throws Exception {
        Constructor<Constants> constructor = Constants.class.getDeclaredConstructor();
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        Constants instance = constructor.newInstance();
        assertNotNull(instance);
    }

    @Test
    void testConstantValues() {
        assertEquals("sunbird", Constants.KEYSPACE_SUNBIRD);
        assertEquals(".", Constants.DOT);
        assertEquals("success", Constants.SUCCESS);
        assertEquals("Failed", Constants.FAILED);
        assertEquals("asc", Constants.ASC);
        assertEquals("must", Constants.MUST);
        assertEquals("filter", Constants.FILTER);
        assertEquals("bool", Constants.BOOL);
        assertEquals("term", Constants.TERM);
        assertEquals("match", Constants.MATCH);
        assertEquals("range", Constants.RANGE);
        assertEquals("active", Constants.ACTIVE);
        assertEquals("inactive", Constants.INACTIVE);
        assertEquals("1.0", Constants.API_VERSION_1);
        assertEquals("number", Constants.NUMBER);
        assertEquals("long", Constants.LONG);
        assertEquals("date", Constants.DATE);
        assertEquals("AES", Constants.CIPHER_ALGORITHM);
        assertNotNull(Constants.CIPHER_KEY);
        assertEquals(16, Constants.CIPHER_KEY.length);
        assertEquals("True", Constants.TRUE);
        assertEquals("null", Constants.NULL_STRING);
        assertEquals("VERIFIED", Constants.VERIFIED);
    }
}