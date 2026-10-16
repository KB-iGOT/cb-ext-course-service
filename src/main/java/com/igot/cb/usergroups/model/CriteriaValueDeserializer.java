package com.igot.cb.usergroups.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Normalizes {@code criteriaValue} during JSON deserialization.
 * <p>
 * The field is typed {@code List<String>} in Cassandra, but callers may send a
 * bare scalar (e.g. {@code "isOnCentralDeputation": true}). This deserializer
 * coerces any scalar — boolean, number, or string — into a single-element
 * {@code List<String>}, while leaving arrays unchanged.
 * </p>
 */
public class CriteriaValueDeserializer extends StdDeserializer<List<String>> {

    public CriteriaValueDeserializer() {
        super(List.class);
    }

    @Override
    public List<String> deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        if (p.currentToken() == JsonToken.START_ARRAY) {
            List<String> values = new ArrayList<>();
            while (p.nextToken() != JsonToken.END_ARRAY) {
                values.add(p.getText());
            }
            return List.copyOf(values);
        }
        return List.of(p.getText());
    }
}
