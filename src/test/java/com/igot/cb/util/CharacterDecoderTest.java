package com.igot.cb.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class CharacterDecoderTest {

    private static class TestCharacterDecoder extends CharacterDecoder {
        @Override
        protected int bytesPerAtom() {
            return 4;
        }

        @Override
        protected int bytesPerLine() {
            return 8;
        }

        @Override
        protected void decodeAtom(PushbackInputStream aStream, OutputStream bStream, int l) throws IOException {
            throw new IOException("Test decode atom");
        }

        @Override
        protected int decodeLinePrefix(PushbackInputStream aStream, OutputStream bStream) throws IOException {
            // Force an IOException to be thrown during decoding
            throw new IOException("Forced exception for testing");
        }
    }

    @Test
    void testBytesPerAtom() {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        assertEquals(4, decoder.bytesPerAtom());
    }

    @Test
    void testBytesPerLine() {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        assertEquals(8, decoder.bytesPerLine());
    }

    @Test
    void testDecodeLinePrefix() throws IOException {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        PushbackInputStream inputStream = new PushbackInputStream(new ByteArrayInputStream("test".getBytes()));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        assertThrows(IOException.class, () -> decoder.decodeLinePrefix(inputStream, outputStream));
    }

    @Test
    void testReadFully() throws IOException {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        ByteArrayInputStream inputStream = new ByteArrayInputStream("test".getBytes());
        byte[] buffer = new byte[10];
        
        int bytesRead = decoder.readFully(inputStream, buffer, 0, 4);
        assertEquals(4, bytesRead);
        assertEquals('t', buffer[0]);
        assertEquals('e', buffer[1]);
        assertEquals('s', buffer[2]);
        assertEquals('t', buffer[3]);
    }

    @Test
    void testReadFullyWithInsufficientData() throws IOException {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        ByteArrayInputStream inputStream = new ByteArrayInputStream("ab".getBytes());
        byte[] buffer = new byte[10];
        
        int bytesRead = decoder.readFully(inputStream, buffer, 0, 4);
        assertEquals(2, bytesRead);
    }

    @Test
    void testReadFullyWithEmptyStream() throws IOException {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        byte[] buffer = new byte[10];
        
        int bytesRead = decoder.readFully(inputStream, buffer, 0, 4);
        assertEquals(-1, bytesRead);
    }

    @Test
    void testDecodeBufferPrefix() throws IOException {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        PushbackInputStream inputStream = new PushbackInputStream(new ByteArrayInputStream("test".getBytes()));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        assertDoesNotThrow(() -> decoder.decodeBufferPrefix(inputStream, outputStream));
    }

    @Test
    void testDecodeBufferSuffix() throws IOException {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        PushbackInputStream inputStream = new PushbackInputStream(new ByteArrayInputStream("test".getBytes()));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        assertDoesNotThrow(() -> decoder.decodeBufferSuffix(inputStream, outputStream));
    }

    @Test
    void testDecodeLineSuffix() throws IOException {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        PushbackInputStream inputStream = new PushbackInputStream(new ByteArrayInputStream("test".getBytes()));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        assertDoesNotThrow(() -> decoder.decodeLineSuffix(inputStream, outputStream));
    }

    @Test
    void testDefaultDecodeAtom() {
        CharacterDecoder decoder = new CharacterDecoder() {
            @Override
            protected int bytesPerAtom() { return 1; }
            @Override
            protected int bytesPerLine() { return 1; }
        };
        
        PushbackInputStream inputStream = new PushbackInputStream(new ByteArrayInputStream("test".getBytes()));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        assertThrows(IOException.class, () -> decoder.decodeAtom(inputStream, outputStream, 1));
    }

    @Test
    void testDecodeBufferString() {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        assertThrows(IOException.class, () -> decoder.decodeBuffer("test"));
    }

    @Test
    void testDecodeBufferInputStream() {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        ByteArrayInputStream inputStream = new ByteArrayInputStream("test".getBytes());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        assertThrows(IOException.class, () -> decoder.decodeBuffer(inputStream, outputStream));
    }

    @Test
    void testDecodeBufferToByteBufferString() {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        assertThrows(IOException.class, () -> decoder.decodeBufferToByteBuffer("test"));
    }

    @Test
    void testDecodeBufferToByteBufferInputStream() {
        TestCharacterDecoder decoder = new TestCharacterDecoder();
        ByteArrayInputStream inputStream = new ByteArrayInputStream("test".getBytes());
        assertThrows(IOException.class, () -> decoder.decodeBufferToByteBuffer(inputStream));
    }

    @Test
    void testDecodeBufferWithEmptyInput() {
        // Use a decoder that doesn't throw exceptions for empty input
        CharacterDecoder decoder = new CharacterDecoder() {
            @Override
            protected int bytesPerAtom() { return 4; }
            @Override
            protected int bytesPerLine() { return 8; }
        };
        ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        // This should complete without throwing exception for empty input
        assertDoesNotThrow(() -> decoder.decodeBuffer(inputStream, outputStream));
    }
}