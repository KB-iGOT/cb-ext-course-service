package com.igot.cb.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class BitSetSerializer extends JsonSerializer<BitSet> {
    @Override
    public void serialize(BitSet bitSet, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        List<Integer> bits = new ArrayList<>();
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            bits.add(i);
        }
        gen.writeObject(bits);
    }
}
