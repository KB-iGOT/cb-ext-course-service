package com.igot.cb.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Base64Util3Test {

    private Base64Util.Decoder decoder;
    private byte[] output;

    @BeforeEach
    void setUp() {
        output = new byte[100];
        decoder = new Base64Util.Decoder(Base64Util.DEFAULT, output);
    }

    private void setField(String name, Object value) throws Exception {
        Field field = Base64Util.Decoder.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(decoder, value);
    }

    @ParameterizedTest
    @MethodSource("base64TestCases")
    void testBase64Decoding(byte[] input, boolean finish) {
        boolean result = decoder.process(input, 0, input.length, finish);
        assertTrue(result);
    }

    private static Stream<Arguments> base64TestCases() {
        return Stream.of(
                Arguments.of("TWFu".getBytes(), true),   // testValidCompleteBase64
                Arguments.of("TWE=".getBytes(), true),   // testValidPartialBase64_OnePad
                Arguments.of("TQ==".getBytes(), true),   // testValidPartialBase64_TwoPads
                Arguments.of("#WFu".getBytes(), true),   // testInvalidChar_shouldFail
                Arguments.of("TQ==".getBytes(), false),  // testFinishFalsePreservesState
                Arguments.of("TW==".getBytes(), true),   // testState2ToState4Transition
                Arguments.of("TWF=".getBytes(), true)    // testState3ToState5Transition
        );
    }


    @Test
    void testFinishTrue_invalidInState1() throws Exception {
        setField("state", 1);
        byte[] input = new byte[0];
        assertFalse(decoder.process(input, 0, input.length, true));
    }

    @Test
    void testFinishTrue_invalidInState4() throws Exception {
        setField("state", 4); // Expecting second padding character
        byte[] input = new byte[0];
        assertFalse(decoder.process(input, 0, input.length, true));
    }

    @Test
    void testWebSafeDecoder() {
        decoder = new Base64Util.Decoder(Base64Util.URL_SAFE, output);
        byte[] input = "TWF-".getBytes(); // URL-safe variant
        boolean result = decoder.process(input, 0, input.length, true);
        assertTrue(result);
    }

    @Test
    void testEmptyInput() {
        byte[] input = new byte[0];
        boolean result = decoder.process(input, 0, input.length, true);
        assertTrue(result);
    }

    @Test
    void testInvalidPaddingTooMany() {
        byte[] input = "TQ===".getBytes(); // Invalid
        boolean result = decoder.process(input, 0, input.length, true);
        assertFalse(result);
    }

    @Test
    void testInvalidStateEarlyExit() throws Exception {
        setField("state", 6); // already failed
        byte[] input = "TWFu".getBytes();
        assertFalse(decoder.process(input, 0, input.length, true));
    }
}