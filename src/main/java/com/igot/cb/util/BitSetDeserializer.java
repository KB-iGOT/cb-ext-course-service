package com.igot.cb.util;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class BitSetDeserializer extends JsonDeserializer<BitSet> {
    @Override
    public BitSet deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        List<Integer> bits = p.readValueAs(new TypeReference<List<Integer>>() {
        });
        BitSet bitSet = new BitSet();
        if (bits != null) {
            for (Integer bit : bits) {
                if (bit != null && bit >= 0 && bit <= Constants.MAX_BITSET_INDEX) {
                    bitSet.set(bit);
                }
            }
        }
        return bitSet;
    }
}