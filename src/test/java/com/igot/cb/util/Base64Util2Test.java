package com.igot.cb.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class Base64Util2Test {

    private Base64Util.Encoder encoder;
    private byte[] output;

    @BeforeEach
    void setUp() {
        output = new byte[1024];
        encoder = new Base64Util.Encoder(Base64Util.DEFAULT, output);
    }

    private void setField(String fieldName, Object value) throws Exception {
        Field field = Base64Util.Encoder.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(encoder, value);
    }

    private byte[] invokeProcess(byte[] input, boolean finish) {
        encoder.process(input, 0, input.length, finish);
        return Arrays.copyOf(output, encoder.op);
    }

    @Test
    void testCompleteBlockFinish() {
        byte[] input = {0x01, 0x02, 0x03};
        byte[] encoded = invokeProcess(input, true);
        assertTrue(new String(encoded).startsWith("AQID"));
    }

    @Test
    void testOneByteTailFinish() throws Exception {
        setField("tailLen", 1);
        byte[] tail = new byte[2];
        tail[0] = 0x01;
        setField("tail", tail);
        byte[] input = {0x02, 0x03};
        byte[] encoded = invokeProcess(input, true);
        assertTrue(new String(encoded).contains("AQID"));
    }

    @Test
    void testTwoByteTailFinish() throws Exception {
        setField("tailLen", 2);
        byte[] tail = new byte[2];
        tail[0] = 0x01;
        tail[1] = 0x02;
        setField("tail", tail);
        byte[] input = {0x03};
        byte[] encoded = invokeProcess(input, true);
        assertTrue(new String(encoded).contains("AQID"));
    }

    @Test
    void testFinishOneRemainingByte() {
        byte[] input = {0x01};
        byte[] encoded = invokeProcess(input, true);
        assertTrue(new String(encoded).contains("AQ=="));
    }

    @Test
    void testFinishTwoRemainingBytes() {
        byte[] input = {0x01, 0x02};
        byte[] encoded = invokeProcess(input, true);
        assertTrue(new String(encoded).contains("AQI="));
    }

    @Test
    void testTailSavedIfFinishFalse_1Byte() {
        byte[] input = {0x11};
        invokeProcess(input, false);
        assertEquals(1, encoder.tailLen);
    }

    @Test
    void testTailSavedIfFinishFalse_2Bytes() {
        byte[] input = {0x11, 0x12};
        invokeProcess(input, false);
        assertEquals(2, encoder.tailLen);
    }

    @Test
    void testWithCRLF() throws Exception {
        encoder = new Base64Util.Encoder(Base64Util.CRLF, output);
        setField("count", 1); // Force newline
        byte[] input = new byte[6]; // Small input to trigger newline
        Arrays.fill(input, (byte) 0x01);
        byte[] encoded = invokeProcess(input, true);
        String str = new String(encoded);
        assertTrue(str.contains("\r\n"));
    }

    @Test
    void testNoNewlineIfDisabled() {
        encoder = new Base64Util.Encoder(Base64Util.NO_WRAP, output);
        byte[] input = {0x01, 0x02, 0x03};
        byte[] encoded = invokeProcess(input, true);
        assertFalse(new String(encoded).contains("\n"));
    }

}
