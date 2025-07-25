package com.igot.cb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.*;


/**
 * @author Mahesh RV
 */
public class CryptoUtil {
    private static final Charset US_ASCII = StandardCharsets.US_ASCII;
    private static final Logger logger = LoggerFactory.getLogger(CryptoUtil.class.getName());

    private CryptoUtil() {
    }

    /**
     * Verifies an RSA signature using the provided payload, signature, public key, and algorithm.
     *
     * @param payLoad   The payload to be verified.
     * @param signature The signature to be verified.
     * @param key       The public key used for verification.
     * @param algorithm The algorithm used for verification.
     * @return true if the signature is valid, false otherwise.
     */
    public static boolean verifyRSASign(String payLoad, byte[] signature, PublicKey key, String algorithm) {
        Signature sign;
        try {
            // Initialize a Signature instance with the provided algorithm
            sign = Signature.getInstance(algorithm);
            // Initialize the Signature instance with the public key for verification
            sign.initVerify(key);
            // Update the Signature instance with the payload bytes
            sign.update(payLoad.getBytes(US_ASCII));
            return sign.verify(signature);
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            logger.error("An error occurred during RSA signature verification: {}", e.getMessage(), e);
            return false;
        }
    }

}