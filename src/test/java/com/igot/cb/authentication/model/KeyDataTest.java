package com.igot.cb.authentication.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.PublicKey;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class KeyDataTest {

    @Test
    void testConstructorAndGetters() {
        String keyId = "testKeyId";
        PublicKey publicKey = mock(PublicKey.class);

        KeyData keyData = new KeyData(keyId, publicKey);

        assertEquals(keyId, keyData.getKeyId());
        assertEquals(publicKey, keyData.getPublicKey());
    }

    @Test
    void testSetters() {
        KeyData keyData = new KeyData("initialId", mock(PublicKey.class));

        String newKeyId = "newKeyId";
        PublicKey newPublicKey = mock(PublicKey.class);

        keyData.setKeyId(newKeyId);
        keyData.setPublicKey(newPublicKey);

        assertEquals(newKeyId, keyData.getKeyId());
        assertEquals(newPublicKey, keyData.getPublicKey());
    }
}

