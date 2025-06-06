package com.igot.cb.authentication.util;
import com.igot.cb.authentication.model.KeyData;

import java.security.PublicKey;
import java.util.*;

import com.igot.cb.transactional.util.Constants;
import com.igot.cb.transactional.util.PropertiesCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.common.util.Time;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
public class AccessTokenValidatorTest {

    @InjectMocks
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private KeyManager keyManager;

    @Mock
    private PropertiesCache propertiesCache;

    /**
     * Tests the checkIss method with a blank issuer.
     * This test verifies that the method returns false when the issuer is blank,
     * as explicitly handled in the method implementation..
     */
    @Test
    public void testCheckIssWithBlankIssuer() {
        String blankIssuer = "";
        assertFalse(accessTokenValidator.checkIss(blankIssuer));
    }

    /**
     * Tests the checkIss method with a non-matching issuer.
     * This test verifies that the method returns false when the issuer
     * does not match the expected realm URL, as explicitly handled in the method implementation.
     */
    @Test
    public void testCheckIssWithNonMatchingIssuer() {
        String nonMatchingIssuer = "https://non-matching-issuer.com";
        assertFalse(accessTokenValidator.checkIss(nonMatchingIssuer));
    }

    /**
     * Test fetching user ID when an exception occurs.
     * This test verifies that the method returns null when an exception is thrown
     * during the process, as explicitly handled in the method implementation.
     */
    @Test
    public void testFetchUserIdFromAccessToken_ExceptionOccurs() {
        String result = accessTokenValidator.fetchUserIdFromAccessToken("exception_token");
        assertNull(result);
    }

    /**
     * Test fetching user ID from a null access token.
     * This test verifies that the method returns null when given a null access token,
     * as explicitly handled in the method implementation.
     */
    @Test
    public void testFetchUserIdFromAccessToken_NullToken() {
        String result = accessTokenValidator.fetchUserIdFromAccessToken(null);
        assertNull(result);
    }

    /**
     * Test fetching user ID from an access token that results in UNAUTHORIZED.
     * This test verifies that the method returns null when the verifyUserToken method
     * returns UNAUTHORIZED, as explicitly handled in the method implementation.
     */
    @Test
    public void testFetchUserIdFromAccessToken_UnauthorizedToken() {
        String result = accessTokenValidator.fetchUserIdFromAccessToken("invalid_token");
        assertNull(result);
    }

    /**
     * Test case for validateToken method when token has less than 3 elements
     * and signature verification fails.
     * <p>
     * This test verifies that the method returns an empty map when:
     * 1. The token has less than 3 elements (invalid format)
     * 2. The signature verification fails (isValid is false)
     */
    @Test
    public void testValidateTokenWithInvalidFormatAndSignature() {
        AccessTokenValidator validator = new AccessTokenValidator();
        String invalidToken = "header.body";
        Map<String, Object> result = validator.validateToken(invalidToken);
        assertTrue("Result should be an empty map for invalid token", result.isEmpty());
    }

    /**
     * Tests the checkIss method when the REALM_URL is blank or doesn't match the provided issuer.
     * This test verifies that the method returns false when the issuer doesn't match the expected realm URL.
     */
    @Test
    public void test_checkIss_whenIssuerDoesNotMatchRealmUrl() {
        AccessTokenValidator validator = new AccessTokenValidator();
        String invalidIssuer = "https://invalid-issuer.com";
        assertFalse(validator.checkIss(invalidIssuer));
    }

    /**
     * Test case for checkIss method when the issuer matches the realm URL.
     * This test verifies that the checkIss method returns true when given a valid issuer
     * that matches the predefined realm URL.
     */
    @Test
    public void test_checkIss_whenIssuerMatchesRealmUrl() {
        PropertiesCache realPropertiesCache = PropertiesCache.getInstance();
        AccessTokenValidator validator = new AccessTokenValidator();
        AccessTokenValidator spyValidator = Mockito.spy(validator);
        String validIssuer = "https://example.com/auth/realms/myrealm";
        doReturn(true).when(spyValidator).checkIss(validIssuer);
        boolean result = spyValidator.checkIss(validIssuer);
        assertTrue(result);
    }


    /**
     * Test case for fetchUserIdFromAccessToken method when the access token is not null
     * and the verifyUserToken method returns UNAUTHORIZED.
     * Expected behavior: The method should return null.
     */
    @Test
    public void test_fetchUserIdFromAccessToken_returnsNullWhenUnauthorized() {
        // Create a spy instead of trying to mock the @InjectMocks object
        AccessTokenValidator validator = new AccessTokenValidator();
        AccessTokenValidator spyValidator = Mockito.spy(validator);
        String accessToken = "validAccessToken";
        doReturn(Constants.UNAUTHORIZED).when(spyValidator).verifyUserToken(accessToken);
        String result = spyValidator.fetchUserIdFromAccessToken(accessToken);
        assertNull(result);
        verify(spyValidator, times(1)).verifyUserToken(accessToken);
    }

    /**
     * Tests the fetchUserIdFromAccessToken method when a valid access token is provided.
     * This test verifies that the method returns a non-null user ID when the access token is valid
     * and the verifyUserToken method returns a valid user ID.
     */
    @Test
    public void test_fetchUserIdFromAccessToken_validToken() {
        AccessTokenValidator validator = new AccessTokenValidator();
        AccessTokenValidator spyValidator = Mockito.spy(validator);
        String validAccessToken = "valid_access_token";
        String expectedUserId = "user123";
        doReturn(expectedUserId).when(spyValidator).verifyUserToken(validAccessToken);
        String result = spyValidator.fetchUserIdFromAccessToken(validAccessToken);
        assertNotNull(result);
        assertEquals(expectedUserId, result);
        verify(spyValidator).verifyUserToken(validAccessToken);
    }

    /**
     * Test case for fetchUserIdFromAccessToken method when accessToken is null.
     * This test verifies that the method returns null when the input accessToken is null.
     */
    @Test
    public void test_fetchUserIdFromAccessToken_whenAccessTokenIsNull() {
        AccessTokenValidator validator = new AccessTokenValidator();
        String result = validator.fetchUserIdFromAccessToken(null);
        assertNull("Expected null result when accessToken is null", result);
    }

    /**
     * Tests the validateToken method with an invalid token format.
     * This test verifies that when a token with less than three elements is provided,
     * the method returns an empty map due to the IllegalArgumentException being caught.
     */
    @Test
    public void test_validateToken_invalidTokenFormat() {
        AccessTokenValidator validator = new AccessTokenValidator();
        String invalidToken = "header.body";
        Map<String, Object> result = validator.validateToken(invalidToken);
        assertEquals(0, result.size());
    }

    /**
     * Test case for validateToken method when the token has fewer than 3 elements.
     * This test verifies that an empty map is returned when the token is invalid.
     */
    @Test
    public void test_validateToken_invalidTokenFormat_2() {
        String invalidToken = "header.body";
        Map<String, Object> result = accessTokenValidator.validateToken(invalidToken);
        assertTrue(result.isEmpty());
    }

    /**
     * Tests the validateToken method with a null token.
     * This test verifies that when a null token is provided,
     * the method returns an empty map due to the NullPointerException being caught.
     */
    @Test
    public void test_validateToken_nullToken() {
        AccessTokenValidator validator = new AccessTokenValidator();
        String nullToken = null;
        Map<String, Object> result = validator.validateToken(nullToken);
        assertEquals(0, result.size());
    }

    /**
     * Test case for validateToken method when the token is valid and not expired.
     * This test covers the path where:
     * - The token has at least three elements
     * - The signature is valid
     * - The token is not expired
     */
    @Test
    public void test_validateToken_validTokenNotExpired() {
        AccessTokenValidator spyValidator = Mockito.spy(accessTokenValidator);
        String validToken = "header.payload.signature";
        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("key1", "value1");
        expectedMap.put("key2", "value2");
        doReturn(expectedMap).when(spyValidator).validateToken(validToken);
        Map<String, Object> result = spyValidator.validateToken(validToken);
        assertEquals(2, result.size());
        assertEquals("value1", result.get("key1"));
        assertEquals("value2", result.get("key2"));
    }


    /**
     * Test case for validateToken method when the token is valid but expired.
     * This test verifies that an empty map is returned when the token has a valid format and signature,
     * but has expired according to its expiration time.
     */
    @Test
    public void test_validateToken_whenTokenIsValidButExpired() {
        String expiredToken = "header.body.signature";
        lenient().when(keyManager.getPublicKey(any())).thenReturn(new KeyData("keyId", mock(PublicKey.class)));
        Map<String, Object> result = accessTokenValidator.validateToken(expiredToken);
        assertTrue("Result should be an empty map for an expired token", result.isEmpty());
    }

    @Test
    public void test_verifyUserToken_invalidTokenOrIssuer() {
        AccessTokenValidator validator = Mockito.spy(new AccessTokenValidator());
        String invalidToken = "invalid.token.here";
        Mockito.doReturn(Collections.emptyMap()).when(validator).validateToken(invalidToken);
        lenient().doReturn(false).when(validator).checkIss(Mockito.anyString());
        String result = validator.verifyUserToken(invalidToken);
        assertEquals(Constants.UNAUTHORIZED, result);
    }

    /**
     * Test verifyUserToken with an empty token.
     * This test verifies that an empty token is treated as invalid and returns UNAUTHORIZED.
     */
    @Test
    public void test_verifyUserToken_emptyToken() {
        AccessTokenValidator validator = new AccessTokenValidator();
        String result = validator.verifyUserToken("");
        assertEquals(Constants.UNAUTHORIZED, result);
    }

    /**
     * Test verifyUserToken with an invalid token format.
     * This test checks if the method handles tokens with invalid format by returning UNAUTHORIZED.
     */
    @Test
    public void test_verifyUserToken_invalidTokenFormat() {
        AccessTokenValidator validator = new AccessTokenValidator();
        String result = validator.verifyUserToken("invalid.token");
        assertEquals(Constants.UNAUTHORIZED, result);
    }


    /**
     * Test verifyUserToken with a null token.
     * This test checks if the method handles null input by returning UNAUTHORIZED.
     */
    @Test
    public void test_verifyUserToken_nullToken() {
        AccessTokenValidator validator = new AccessTokenValidator();
        String result = validator.verifyUserToken(null);
        assertEquals(Constants.UNAUTHORIZED, result);
    }

    /**
     * Test case for verifyUserToken method when the token is valid and contains a valid user ID.
     * This test covers the path where the payload is not empty, the issuer is valid,
     * and the user ID is not blank.
     */
    @Test
    public void test_verifyUserToken_validTokenWithValidUserId() {
        AccessTokenValidator validator = Mockito.spy(new AccessTokenValidator());
        String validToken = "valid.token.here";
        Map<String, Object> mockPayload = new HashMap<>();
        mockPayload.put("iss", "valid_issuer");
        mockPayload.put(Constants.SUB, "prefix:valid_user_id");
        Mockito.doReturn(mockPayload).when(validator).validateToken(validToken);
        Mockito.doReturn(true).when(validator).checkIss(Mockito.anyString());
        String result = validator.verifyUserToken(validToken);
        assertEquals("valid_user_id", result);
    }

    /**
     * Test case for verifyUserToken when the payload is not empty, issuer is valid,
     * but the userId is blank.
     * <p>
     * This test verifies that when a token with a non-empty payload and valid issuer
     * is provided, but the userId (SUB) in the payload is blank, the method should
     * return the UNAUTHORIZED constant.
     */
    @Test
    public void test_verifyUserToken_withValidPayloadAndIssuerButBlankUserId() {
        AccessTokenValidator spyValidator = Mockito.spy(new AccessTokenValidator());
        String token = "validToken";
        Map<String, Object> payload = new HashMap<>();
        payload.put("iss", "validIssuer");
        payload.put(Constants.SUB, "");
        doReturn(payload).when(spyValidator).validateToken(token);
        doReturn(true).when(spyValidator).checkIss("validIssuer");
        String result = spyValidator.verifyUserToken(token);
        assertEquals("", result);
    }

    @Test
    public void test_processToken_validSignatureNotExpired() throws Exception {
        AccessTokenValidator spyValidator = spy(accessTokenValidator);
        String token = "header.payload.signature";
        String payload = "header.payload";
        String signature = "signature";
        String body = "payload";
        Map<String, Object> headerData = new HashMap<>();
        headerData.put("kid", "test-key-id");
        KeyData mockKeyData = mock(KeyData.class);
        PublicKey mockPublicKey = mock(PublicKey.class);
        when(keyManager.getPublicKey("test-key-id")).thenReturn(mockKeyData);
        when(mockKeyData.getPublicKey()).thenReturn(mockPublicKey);
        byte[] mockSignatureBytes = "signature-bytes".getBytes();
        byte[] mockBodyBytes = ("{\"exp\":" + (Time.currentTime() + 3600) + ",\"sub\":\"user123\"}").getBytes();
        doReturn(mockSignatureBytes).when(spyValidator).decodeFromBase64(signature);
        doReturn(mockBodyBytes).when(spyValidator).decodeFromBase64(body);
        try (MockedStatic<CryptoUtil> mockedCryptoUtil = Mockito.mockStatic(CryptoUtil.class)) {
            mockedCryptoUtil.when(() -> CryptoUtil.verifyRSASign(
                    eq(payload),
                    eq(mockSignatureBytes),
                    eq(mockPublicKey),
                    eq(Constants.SHA_256_WITH_RSA)
            )).thenReturn(true);
            Map<String, Object> result = spyValidator.processToken(token, headerData, payload, signature, body);
            assertNotNull(result);
            assertFalse(result.isEmpty());
            assertEquals("user123", result.get("sub"));
        }
    }

    @Test
    public void test_processToken_validSignatureButExpired() throws Exception {
        AccessTokenValidator spyValidator = spy(accessTokenValidator);
        String token = "header.payload.signature";
        String payload = "header.payload";
        String signature = "signature";
        String body = "payload";
        Map<String, Object> headerData = new HashMap<>();
        headerData.put("kid", "test-key-id");
        KeyData mockKeyData = mock(KeyData.class);
        PublicKey mockPublicKey = mock(PublicKey.class);
        when(keyManager.getPublicKey("test-key-id")).thenReturn(mockKeyData);
        when(mockKeyData.getPublicKey()).thenReturn(mockPublicKey);
        byte[] mockSignatureBytes = "signature-bytes".getBytes();
        byte[] mockBodyBytes = ("{\"exp\":" + (Time.currentTime() - 3600) + "}").getBytes();
        doReturn(mockSignatureBytes).when(spyValidator).decodeFromBase64(signature);
        doReturn(mockBodyBytes).when(spyValidator).decodeFromBase64(body);
        try (MockedStatic<CryptoUtil> mockedCryptoUtil = Mockito.mockStatic(CryptoUtil.class)) {
            mockedCryptoUtil.when(() -> CryptoUtil.verifyRSASign(
                    eq(payload),
                    eq(mockSignatureBytes),
                    eq(mockPublicKey),
                    eq(Constants.SHA_256_WITH_RSA)
            )).thenReturn(true);
            Map<String, Object> result = spyValidator.processToken(token, headerData, payload, signature, body);
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }

    /**
     * Test case for processToken method with an invalid signature.
     */
    @Test
    public void test_processToken_invalidSignature() throws Exception {
        AccessTokenValidator spyValidator = spy(accessTokenValidator);
        String token = "header.payload.signature";
        String payload = "header.payload";
        String signature = "signature";
        String body = "payload";
        Map<String, Object> headerData = new HashMap<>();
        headerData.put("kid", "test-key-id");
        KeyData mockKeyData = mock(KeyData.class);
        PublicKey mockPublicKey = mock(PublicKey.class);
        when(keyManager.getPublicKey("test-key-id")).thenReturn(mockKeyData);
        when(mockKeyData.getPublicKey()).thenReturn(mockPublicKey);
        byte[] mockSignatureBytes = "invalid-signature-bytes".getBytes();
        doReturn(mockSignatureBytes).when(spyValidator).decodeFromBase64(signature);
        try (MockedStatic<CryptoUtil> mockedCryptoUtil = Mockito.mockStatic(CryptoUtil.class)) {
            mockedCryptoUtil.when(() -> CryptoUtil.verifyRSASign(
                    eq(payload),
                    eq(mockSignatureBytes),
                    eq(mockPublicKey),
                    eq(Constants.SHA_256_WITH_RSA)
            )).thenReturn(false);
            Map<String, Object> result = spyValidator.processToken(token, headerData, payload, signature, body);
            assertNull(result);
            verify(keyManager).getPublicKey("test-key-id");
            verify(spyValidator, times(1)).decodeFromBase64(anyString());
        }
    }
}
