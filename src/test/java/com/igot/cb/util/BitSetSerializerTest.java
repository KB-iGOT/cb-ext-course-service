package com.igot.cb.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.io.StringWriter;
import java.util.BitSet;
import static org.junit.jupiter.api.Assertions.*;

class BitSetSerializerTest {

    @Test
    void testSerialize() throws IOException {
        BitSetSerializer serializer = new BitSetSerializer();
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        bitSet.set(3);
        bitSet.set(5);
        
        serializer.serialize(bitSet, generator, provider);
        generator.flush();
        
        String result = writer.toString();
        assertTrue(result.contains("1"));
        assertTrue(result.contains("3"));
        assertTrue(result.contains("5"));
    }

    @Test
    void testSerializeEmptyBitSet() throws IOException {
        BitSetSerializer serializer = new BitSetSerializer();
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        BitSet bitSet = new BitSet();
        
        serializer.serialize(bitSet, generator, provider);
        generator.flush();
        
        String result = writer.toString();
        assertEquals("[]", result);
    }

    @Test
    void testSerializeSingleBit() throws IOException {
        BitSetSerializer serializer = new BitSetSerializer();
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        BitSet bitSet = new BitSet();
        bitSet.set(0);
        
        serializer.serialize(bitSet, generator, provider);
        generator.flush();
        
        String result = writer.toString();
        assertEquals("[0]", result);
    }
}