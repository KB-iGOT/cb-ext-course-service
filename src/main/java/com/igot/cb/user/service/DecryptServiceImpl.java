package com.igot.cb.user.service;

import com.igot.cb.util.BASE64Decoder;
import com.igot.cb.util.Base64Util;
import com.igot.cb.util.Constants;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class DecryptServiceImpl {

    private static String sunbird_encryption = "";

    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    private int ITERATIONS = 3;

    private Cipher decryptCipher;

    private Cipher encryptCipher;

    @Value("${sb.env.chiper.password}")
    private String sbChiperPassword;

    private SecretKeySpec secretKeySpec;

    @PostConstruct
    private void postConstruct() {
        try {
            decryptCipher = Cipher.getInstance(Constants.CIPHER_ALGORITHM);
            decryptCipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
            encryptCipher = Cipher.getInstance(Constants.CIPHER_ALGORITHM);
            encryptCipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
        } catch (Exception e) {
            log.error("Failed to construct DecryptServiceImpl object.");
        }
    }

    public String decryptString(String encStr) {
        try {
            String dValue = null;
            String valueToDecrypt = encStr.trim();
            for (int i = 0; i < ITERATIONS; i++) {
                byte[] decodedValue = new BASE64Decoder().decodeBuffer(valueToDecrypt);
                byte[] decValue = decryptCipher.doFinal(decodedValue);
                dValue = new String(decValue, StandardCharsets.UTF_8).substring(sbChiperPassword.length());
                valueToDecrypt = dValue;
            }
            return dValue;
        } catch (Exception ex) {
            logger.error("Failed to decrypt value. Exception: ", ex);
        }
        return null;
    }
}
