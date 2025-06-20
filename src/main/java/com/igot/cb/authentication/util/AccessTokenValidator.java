package com.igot.cb.authentication.util;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.transactional.util.ApiResponse;
import com.igot.cb.transactional.util.Constants;
import com.igot.cb.transactional.util.PropertiesCache;
import org.apache.commons.lang3.StringUtils;
import org.keycloak.common.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;


@Component
public class AccessTokenValidator {

    @Autowired
    KeyManager keyManager;

    private static Logger logger = LoggerFactory.getLogger(AccessTokenValidator.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();
    private static PropertiesCache cache = PropertiesCache.getInstance();
    private static final String REALM_URL = cache.getProperty(Constants.SSO_URL) + "realms/" + cache.getProperty(Constants.SSO_REALM);


    /**
     * Validates the provided JWT token.
     *
     * @param token The JWT token to be validated.
     * @return A map containing the token body if the token is valid and not expired, otherwise an empty map.
     */
    public Map<String, Object> validateToken(String token) {
        try {
            //  Null or empty check before processing
            if (StringUtils.isBlank(token)) {
                throw new IllegalArgumentException("Token is null or empty");
            }
            // Split the token into its elements
            String[] tokenElements = token.split("\\.");
            // Check if the token has at least three elements
            if (tokenElements.length < 3) {
                throw new IllegalArgumentException("Invalid token format");
            }
            // Extract header, body, and signature from token elements
            String header = tokenElements[0];
            String body = tokenElements[1];
            String signature = tokenElements[2];
            // Concatenate header and body to form the payload
            String payload = header + Constants.DOT_SEPARATOR + body;
            // Parse header data from base64 encoded header
            Map<String, Object> headerData = mapper.readValue(new String(decodeFromBase64(header)), new TypeReference<Map<String, Object>>() {
            });
            Map<String, Object> emptyMap = processToken(token, headerData, payload, signature, body);
            if (emptyMap != null) return emptyMap;
        } catch (IOException | IllegalArgumentException e) {
            logger.error("Error validating token: {}", e.getMessage());
        } catch (Exception ex) {
            logger.error("Unexpected error validating token: {}", ex.getMessage());
        }
        return Collections.emptyMap();
    }

    public Map<String, Object> processToken(String token, Map<String, Object> headerData, String payload, String signature, String body) throws JsonProcessingException {
        String keyId = headerData.get("kid").toString();
        // Verify the token signature
        boolean isValid = CryptoUtil.verifyRSASign(payload, decodeFromBase64(signature), keyManager.getPublicKey(keyId).getPublicKey(), Constants.SHA_256_WITH_RSA);
        // If token signature is valid, parse token body and check expiration
        if (isValid) {
            Map<String, Object> tokenBody = mapper.readValue(new String(decodeFromBase64(body)), new TypeReference<Map<String, Object>>() {
            });
            if (isExpired((Integer) tokenBody.get("exp"))) {
                logger.error("Token expired: {}", token);
                return Collections.emptyMap();
            }
            return tokenBody;
        }
        return null;
    }


    /**
     * Verifies the user token and extracts the user ID from it.
     *
     * @param token The user token to be verified.
     * @return The user ID extracted from the token, or UNAUTHORIZED if verification fails or an exception occurs.
     */
    public String verifyUserToken(String token) {
        // Initialize user ID to UNAUTHORIZED
        String userId = Constants.UNAUTHORIZED;
        try {
            // Validate the token and obtain its payload
            Map<String, Object> payload = validateToken(token);
            // Check if payload is not empty and issuer is valid
            if (!payload.isEmpty() && checkIss((String) payload.get("iss"))) {
                // Extract user ID from payload
                userId = (String) payload.get(Constants.SUB);
                // If user ID is not blank, extract the actual user ID
                if (StringUtils.isNotBlank(userId)) {
                    userId = userId.substring(userId.lastIndexOf(":") + 1);
                }
            }
        } catch (Exception ex) {
            logger.error("Exception in verifyUserAccessToken: verify ", ex);
        }
        return userId;
    }

    /**
     * Checks if the issuer of the token matches the predefined realm URL.
     *
     * @param iss The issuer extracted from the token.
     * @return true if the issuer matches the realm URL, false otherwise.
     */
    public boolean checkIss(String iss) {
        // Check if the realm URL is blank or if the issuer does not match the realm URL
        if (StringUtils.isBlank(REALM_URL) || !REALM_URL.equalsIgnoreCase(iss)) {
            logger.warn("Issuer does not match the expected realm URL. Issuer: {}, Expected: {}", iss, REALM_URL);
            return false;
        }
        logger.info("Issuer validation successful. Issuer: {}", iss);
        return true;
    }


    private boolean isExpired(Integer expiration) {
        return (Time.currentTime() > expiration);
    }

    byte[] decodeFromBase64(String data) {
        return Base64Util.decode(data, 11);
    }

    /**
     * Fetches the user ID from the provided access token.
     *
     * @param accessToken The access token from which to fetch the user ID.
     * @return The user ID fetched from the access token, or null if the token is invalid or an exception occurs.
     */
    public String fetchUserIdFromAccessToken(String accessToken) {
        // Initialize clientAccessTokenId to null
        String clientAccessTokenId = null;
        // Check if the accessToken is not null
        if (accessToken != null) {
            try {
                // Verify the access token to fetch the user ID
                clientAccessTokenId = verifyUserToken(accessToken);
                // If the user ID is UNAUTHORIZED, set it to null
                if (Constants.UNAUTHORIZED.equalsIgnoreCase(clientAccessTokenId)) {
                    clientAccessTokenId = null;
                }
            } catch (Exception ex) {
                String errMsg = "Exception occurred while fetching the userid from the access token. Exception: " + ex.getMessage();
                logger.error(errMsg, ex);
                clientAccessTokenId = null;
            }
        }
        return clientAccessTokenId;
    }

    public String fetchUserIdFromAccessToken(String accessToken, ApiResponse response) {
        String clientAccessTokenId = null;
        if (accessToken != null) {
            try {
                clientAccessTokenId = verifyUserToken(accessToken);
                if (Constants.UNAUTHORIZED.equalsIgnoreCase(clientAccessTokenId)) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErrMsg(Constants.ACCESS_TOKEN_IS_EXPIRED);
                    response.setResponseCode(HttpStatus.UNAUTHORIZED);
                    clientAccessTokenId = null;
                }
            } catch (Exception ex) {
                String errMsg = "Exception occurred while fetching the userid from the access token. Exception: " + ex.getMessage();
                logger.error(errMsg, ex);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(Constants.ACCESS_TOKEN_VALIDATION_FAILED);
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                clientAccessTokenId = null;
            }
        }else{
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(Constants.ACCESS_TOKEN_VALIDATION_FAILED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
        }
        return clientAccessTokenId;
    }
}