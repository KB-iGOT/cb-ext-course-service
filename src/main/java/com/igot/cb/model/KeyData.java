package com.igot.cb.model;

import java.security.PublicKey;

/**
 * @author Mahesh RV
 */
public class KeyData {
    private String keyId;
    private PublicKey publicKey;

    public KeyData(String keyId, PublicKey publicKey) {
        this.keyId = keyId;
        this.publicKey = publicKey;
    }

    public String getKeyId() {
        return keyId;
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(PublicKey publicKey) {
        this.publicKey = publicKey;
    }
}