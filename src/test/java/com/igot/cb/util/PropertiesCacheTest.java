package com.igot.cb.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.igot.cb.util.PropertiesCache;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class PropertiesCacheTest {

    private PropertiesCache cache;

    @BeforeEach
    void setUp() throws Exception {
        cache = PropertiesCache.getInstance();
        // Inject test properties using reflection
        Field configPropField = PropertiesCache.class.getDeclaredField("configProp");
        configPropField.setAccessible(true);
        Properties testProps = new Properties();
        testProps.setProperty("test.key", "test.value");
        testProps.setProperty("blank.key", "");
        configPropField.set(cache, testProps);
    }

    @Test
    void testSingletonInstance() {
        PropertiesCache another = PropertiesCache.getInstance();
        assertSame(cache, another);
    }

    @Test
    void testGetPropertyReturnsValue() {
        assertEquals("test.value", cache.getProperty("test.key"));
    }

    @Test
    void testGetPropertyReturnsKeyIfNotFound() {
        assertEquals("missing.key", cache.getProperty("missing.key"));
    }

    @Test
    void testReadPropertyReturnsValue() {
        assertEquals("test.value", cache.readProperty("test.key"));
    }

    @Test
    void testReadPropertyReturnsNullIfNotFound() {
        assertNull(cache.readProperty("missing.key"));
    }

}
