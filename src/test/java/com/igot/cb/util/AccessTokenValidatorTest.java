package com.igot.cb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.security.PublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.KeyData;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.CryptoUtil;
import com.igot.cb.util.KeyManager;
import com.igot.cb.util.PropertiesCache;

@ExtendWith(MockitoExtension.class)
class AccessTokenValidatorTest {

    @Mock
    private KeyManager keyManager;

    @InjectMocks
    private AccessTokenValidator validator;

    @Mock
    private PropertiesCache propertiesCache;

    private static PropertiesCache propertiesCacheMock;
    private static MockedStatic<PropertiesCache> mockedPropertiesCache;
    private final String ssoUrl = "https://sso.local/";
    private final String realm = "test-realm";

    private String base64Encode(String json) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(json.getBytes());
    }

    @BeforeAll
    static void setupStatic() {
        propertiesCacheMock = mock(PropertiesCache.class);
        mockedPropertiesCache = mockStatic(PropertiesCache.class);
        mockedPropertiesCache.when(PropertiesCache::getInstance).thenReturn(propertiesCacheMock);
    }

    @BeforeEach
    void setup() {
        when(propertiesCacheMock.getProperty(Constants.SSO_URL)).thenReturn(ssoUrl);
        when(propertiesCacheMock.getProperty(Constants.SSO_REALM)).thenReturn(realm);
    }

    @AfterAll
    static void tearDown() {
        mockedPropertiesCache.close(); // important
    }

    @Test
    void testValidateToken_Success() throws Exception {
        String headerJson = "{\"alg\":\"RS256\",\"kid\":\"test-key\"}";
        String bodyJson = "{\"exp\":" + (System.currentTimeMillis() / 1000 + 300) + ", \"iss\":\"" + ssoUrl + "realms/"
                + realm + "\", \"sub\":\"user:123\"}";

        String header = base64Encode(headerJson);
        String body = base64Encode(bodyJson);
        String payload = header + "." + body;
        String signature = base64Encode("fake-signature");

        String token = header + "." + body + "." + signature;

        // Mock KeyManager and CryptoUtil
        PublicKey pubKey = mock(PublicKey.class);
        KeyData keyData = mock(KeyData.class);
        when(keyData.getPublicKey()).thenReturn(pubKey);
        when(keyManager.getPublicKey("test-key")).thenReturn(keyData);

        try (MockedStatic<CryptoUtil> cryptoMock = mockStatic(CryptoUtil.class)) {
            cryptoMock.when(
                    () -> CryptoUtil.verifyRSASign(eq(payload), any(), eq(pubKey), eq(Constants.SHA_256_WITH_RSA)))
                    .thenReturn(true);

            Map<String, Object> result = validator.validateToken(token);

            assertFalse(result.isEmpty());
            assertEquals("user:123", result.get("sub"));
        }
    }

    @Test
    void testCheckIss_Match() {
        String iss = ssoUrl + "realms/" + realm;
        assertTrue(validator.checkIss(iss));
    }

    @Test
    void testCheckIss_Mismatch() {
        assertFalse(validator.checkIss("https://fake/issuer"));
    }

    @Test
    void testFetchUserIdFromAccessToken_Valid() {
        AccessTokenValidator spy = spy(validator);
        ApiResponse response = new ApiResponse();

        Map<String, Object> payload = new HashMap<>();
        payload.put("iss", "https://sso.local/realms/test-realm");
        payload.put("sub", "user:101");

        doReturn(payload).when(spy).validateToken(any());
        doReturn(true).when(spy).checkIss(any());
        String userId = spy.fetchUserIdFromAccessToken("valid-token", response);
        assertEquals("101", userId);
    }

    @Test
    void testFetchUserIdFromAccessToken_NullToken() {
        ApiResponse response = new ApiResponse();

        String result = validator.fetchUserIdFromAccessToken(null, response);

        assertNull(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ACCESS_TOKEN_VALIDATION_FAILED, response.getParams().getErrMsg());
    }
}
