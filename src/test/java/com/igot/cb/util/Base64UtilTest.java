package com.igot.cb.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class Base64UtilTest {

    @Test
    void testEncodeDecodeBasic() {
        String original = "Hello World";
        byte[] encoded = Base64Util.encode(original.getBytes(), Base64Util.DEFAULT);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        
        assertEquals(original, new String(decoded));
    }

    @Test
    void testEncodeDecodeWithFlags() {
        String original = "Test String";
        byte[] encoded = Base64Util.encode(original.getBytes(), Base64Util.NO_WRAP);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        
        assertEquals(original, new String(decoded));
    }

    @Test
    void testDecodeString() {
        String encoded = "SGVsbG8gV29ybGQ="; // "Hello World" in Base64
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        
        assertEquals("Hello World", new String(decoded));
    }

    @Test
    void testEncodeWithOffset() {
        byte[] input = "Hello World Test".getBytes();
        byte[] encoded = Base64Util.encode(input, 6, 5, Base64Util.DEFAULT); // "World"
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        
        assertEquals("World", new String(decoded));
    }

    @Test
    void testDecodeWithOffset() {
        String encoded = "SGVsbG8gV29ybGQ="; // "Hello World" in Base64
        byte[] encodedBytes = encoded.getBytes();
        byte[] decoded = Base64Util.decode(encodedBytes, 0, encodedBytes.length, Base64Util.DEFAULT);
        
        assertEquals("Hello World", new String(decoded));
    }

    @Test
    void testUrlSafeEncoding() {
        String original = "Test?String&With=Special+Chars";
        byte[] encoded = Base64Util.encode(original.getBytes(), Base64Util.URL_SAFE);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.URL_SAFE);
        
        assertEquals(original, new String(decoded));
    }

    @Test
    void testNoPaddingFlag() {
        String original = "Test";
        byte[] encoded = Base64Util.encode(original.getBytes(), Base64Util.NO_PADDING);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.NO_PADDING);
        
        assertEquals(original, new String(decoded));
    }

    @Test
    void testInvalidBase64() {
        assertThrows(IllegalArgumentException.class, () -> {
            Base64Util.decode("Invalid@Base64!", Base64Util.DEFAULT);
        });
    }

    @Test
    void testEmptyInput() {
        byte[] encoded = Base64Util.encode(new byte[0], Base64Util.DEFAULT);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        
        assertEquals(0, decoded.length);
    }
}