package com.igot.cb.authentication.util;




import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;


public class Base64UtilTest {

    /**
     * Tests that the decode method throws an IllegalArgumentException when given input with incorrect padding.
     * This tests the explicitly handled edge case in the method's implementation where incorrect padding is detected.
     */
    @Test
    public void testDecodeWithIncorrectPadding() {
        String inputWithIncorrectPadding = "SGVsbG8gV29ybGQ====="; // Extra padding
        assertThrows(IllegalArgumentException.class, () -> {
            Base64Util.decode(inputWithIncorrectPadding, Base64Util.DEFAULT);
        });
    }

    /**
     * Tests that decode throws IllegalArgumentException when input contains incorrect padding.
     */

    @Test
    public void testDecodeWithIncorrectPadding_2() {
        byte[] input = "Invalid==Padding".getBytes();
        assertThrows(IllegalArgumentException.class, () -> {
            Base64Util.decode(input, Base64Util.DEFAULT);
        });
    }

    /**
     * Tests that the encode method handles an empty input byte array correctly.
     * This is an edge case where the input is valid but contains no data.
     */
    @Test
    public void testEncodeEmptyInput() {
        byte[] emptyInput = new byte[0];
        byte[] result = Base64Util.encode(emptyInput, Base64Util.DEFAULT);
        assertNotNull(result);
        assertEquals(0, result.length);
    }


    /**
     * Tests encoding with padding, non-multiple of 3 input length, and newline insertion.
     * 
     * Path constraints: 
     * - encoder.do_padding is true
     * - len % 3 > 0 
     * - encoder.do_newline && len > 0
     */
    @Test
    public void testEncodeWithPaddingAndNewline() {
        byte[] input = "Hello, World!".getBytes();
        int offset = 0;
        int len = input.length;
        int flags = Base64Util.DEFAULT;

        byte[] result = Base64Util.encode(input, offset, len, flags);

        String expected = "SGVsbG8sIFdvcmxkIQ==\n";
        assertEquals(expected, new String(result));
    }

    /**
     * Test encoding with zero-length input.
     * This tests the edge case of providing an empty byte array as input.
     */
    @Test
    public void testEncodeWithZeroLengthInput() {
        byte[] input = new byte[0];
        byte[] result = Base64Util.encode(input, Base64Util.DEFAULT);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    /**
     * Tests the decode method with a simple Base64 encoded string.
     * This test verifies that the method correctly decodes a standard Base64 string
     * using the default flags.
     */
    @Test
    public void test_decode_1() {
        String input = "SGVsbG8gV29ybGQ=";
        byte[] expected = "Hello World".getBytes();
        byte[] result = Base64Util.decode(input, Base64Util.DEFAULT);
        assertArrayEquals(expected, result);
    }

    /**
     * Test case for decoding a simple Base64 encoded string
     * This test verifies that the decode method correctly decodes a Base64 encoded input
     * using the default flags (Base64Util.DEFAULT)
     */
    @Test
    public void test_decode_1_2() {
        String input = "SGVsbG8gV29ybGQ="; // "Hello World" in Base64
        byte[] expectedOutput = "Hello World".getBytes();
        byte[] result = Base64Util.decode(input.getBytes(), Base64Util.DEFAULT);
        assertArrayEquals(expectedOutput, result);
    }

    /**
     * Test case for decoding an empty input array.
     * This test verifies that the decode method correctly handles an empty input
     * and returns an empty byte array without throwing an exception.
     */
    @Test
    public void test_decode_emptyInput() {
        byte[] input = new byte[0];
        byte[] result = Base64Util.decode(input, 0, 0, Base64Util.DEFAULT);
        assertArrayEquals(new byte[0], result);
    }

    /**
     * Test case for decoding a Base64 input that results in an output of exact length.
     * This test verifies that the decode method correctly processes the input and
     * returns the output array without needing to create a new array.
     */
    @Test
    public void test_decode_exactOutputLength() {
        // Input that will decode to an exact length output
        byte[] input = "SGVsbG8gV29ybGQ=".getBytes();
        int offset = 0;
        int len = input.length;
        int flags = Base64Util.DEFAULT;

        byte[] result = Base64Util.decode(input, offset, len, flags);

        // Expected output: "Hello World"
        byte[] expected = {72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100};
        assertArrayEquals(expected, result);
    }


    /**
     * Tests the encode method with no padding and newline insertion.
     * This test case covers the path where padding is disabled (!encoder.do_padding)
     * and newline insertion is enabled (encoder.do_newline && len > 0).
     */
    @Test
    public void test_encode_3() {
        byte[] input = "Hello, World!".getBytes();
        int offset = 0;
        int len = input.length;
        int flags = Base64Util.NO_PADDING | Base64Util.CRLF;

        byte[] result = Base64Util.encode(input, offset, len, flags);

        String expected = "SGVsbG8sIFdvcmxkIQ==\r\n".replace("==", "");
        String actual = new String(result);

        assertEquals(expected, actual);
    }

    /**
     * Testcase 4 for public static byte[] encode(byte[] input, int offset, int len, int flags)
     * Tests encoding with padding when input length is not a multiple of 3 and newlines are disabled.
     */
    @Test
    public void test_encode_4() {
        byte[] input = {1, 2, 3, 4, 5};
        int offset = 0;
        int len = 5;
        int flags = Base64Util.NO_WRAP; // Disable newlines

        byte[] result = Base64Util.encode(input, offset, len, flags);

        // Expected output: "AQIDBAU=" (Base64 encoded with padding)
        byte[] expected = {65, 81, 73, 68, 66, 65, 85, 61};
        assertArrayEquals(expected, result);
    }

}
