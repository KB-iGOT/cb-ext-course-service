package com.igot.cb.util;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.keycloak.common.util.Time;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.model.ApiResponse;

import lombok.extern.slf4j.Slf4j;


@Component
@Slf4j
public class AccessTokenValidator {

    private final KeyManager keyManager;

    public AccessTokenValidator(KeyManager keyManager) {
        this.keyManager = keyManager;
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private static PropertiesCache cache = PropertiesCache.getInstance();

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
            String keyId = headerData.get("kid").toString();
            boolean isValid =
                    CryptoUtil.verifyRSASign(
                            payload,
                            decodeFromBase64(signature),
                            keyManager.getPublicKey(keyId).getPublicKey(),
                            Constants.SHA_256_WITH_RSA);
            if (isValid) {
                Map<String, Object> tokenBody =
                        mapper.readValue(new String(decodeFromBase64(body)), Map.class);
                boolean isExp = isExpired((Integer) tokenBody.get("exp"));
                if (isExp) {
                    throw new Exception("Expired auth token is received.");
                }
                return tokenBody;
            } else {
                throw new Exception("Invalid auth token is received.");
            }
        } catch (Exception e) {
            log.warn("Failed to validate the user token. Exception: ", e);
        }
        return Collections.emptyMap();
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
            log.error("Exception in verifyUserAccessToken: verify ", ex);
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
        String expectedRealmUrl = getRealmUrl();
        // Check if the realm URL is blank or if the issuer does not match the realm URL
        if (StringUtils.isBlank(expectedRealmUrl) || !expectedRealmUrl.equalsIgnoreCase(iss)) {
            log.warn("Issuer does not match the expected realm URL. Issuer: {}, Expected: {}", iss, expectedRealmUrl);
            return false;
        }
        log.info("Issuer validation successful. Issuer: {}", iss);
        return true;
    }

    private boolean isExpired(Integer expiration) {
        return (Time.currentTime() > expiration);
    }

    byte[] decodeFromBase64(String data) {
        return Base64Util.decode(data, 11);
    }

    private String getRealmUrl() {
        return cache.getProperty(Constants.SSO_URL) + "realms/" + cache.getProperty(Constants.SSO_REALM);
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
                log.error(errMsg, ex);
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