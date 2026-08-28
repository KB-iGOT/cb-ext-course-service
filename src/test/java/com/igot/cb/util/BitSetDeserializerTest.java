package com.igot.cb.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.BitSet;
import static org.junit.jupiter.api.Assertions.*;

class BitSetDeserializerTest {

    @Test
    void testDeserialize() throws IOException {
        BitSetDeserializer deserializer = new BitSetDeserializer();
        ObjectMapper mapper = new ObjectMapper();
        String json = "[1, 3, 5]";
        
        JsonParser parser = mapper.getFactory().createParser(json);
        DeserializationContext context = mapper.getDeserializationContext();
        
        BitSet result = deserializer.deserialize(parser, context);
        
        assertNotNull(result);
        assertTrue(result.get(1));
        assertTrue(result.get(3));
        assertTrue(result.get(5));
        assertFalse(result.get(0));
        assertFalse(result.get(2));
        assertFalse(result.get(4));
    }

    @Test
    void testDeserializeEmptyList() throws IOException {
        BitSetDeserializer deserializer = new BitSetDeserializer();
        ObjectMapper mapper = new ObjectMapper();
        String json = "[]";
        
        JsonParser parser = mapper.getFactory().createParser(json);
        DeserializationContext context = mapper.getDeserializationContext();
        
        BitSet result = deserializer.deserialize(parser, context);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testDeserializeWithLargeAndNegativeValues() throws IOException {
        BitSetDeserializer deserializer = new BitSetDeserializer();
        ObjectMapper mapper = new ObjectMapper();
        String json = "[1, -1, 1145607574, 5]";
        
        JsonParser parser = mapper.getFactory().createParser(json);
        DeserializationContext context = mapper.getDeserializationContext();
        
        BitSet result = deserializer.deserialize(parser, context);
        
        assertNotNull(result);
        assertTrue(result.get(1));
        assertTrue(result.get(5));
        assertEquals(2, result.cardinality());
    }
}