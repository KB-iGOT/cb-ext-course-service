package com.igot.cb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.igot.cb.model.KeyData;

@ExtendWith(MockitoExtension.class)
class KeyManagerTest {

    @Mock
    PropertiesCache mockCache;

    private KeyManager keyManager;

    static MockedStatic<PropertiesCache> cacheStatic;

    @BeforeAll
    static void initStaticMock() {
        cacheStatic = mockStatic(PropertiesCache.class);
    }

    @AfterAll
    static void closeStaticMock() {
        cacheStatic.close();
    }

    @BeforeEach
    void setup() {
        keyManager = new KeyManager();

        cacheStatic.when(PropertiesCache::getInstance).thenReturn(mockCache);
    }

    @Test
    void testLoadPublicKey_validKey() throws Exception {
        String validKey = """
        -----BEGIN PUBLIC KEY-----
        MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsXQpHH5Wj9ce2j8skC/v
        fNH/4NgyHQq0BbsGdzrCeD3Q/nQhMx5RZxD0HNa79KRC+hdWNNyfBDkcf1Tfp+Ka
        oBjCXdc4U2ImrBaF+UUIqj07c5iRY25ZqtFdUXWEQ2f+Vgy+JhOdGVKYF9rTfIuJ
        1tn9mPfzZ0/yZzX6Vwr1C1RdsqgqHdGdr1xwZFcHXjUYw8VUsHRRbzX5v8yX7TLm
        TFJ9K0HTrYEm+lDkZkmU6iSlsyhr+3g4ph3KekA1UAX7wv3cgJfWLU1mVg9AVspK
        A4tZ7BFlUN+OtqDsTHYkthTy3dpGIp+nNB4ZpSgLmoGx9IMAtzH2H8+JEuGF1qQv
        WwIDAQAB
        -----END PUBLIC KEY-----""";

        PublicKey publicKey = KeyManager.loadPublicKey(validKey);

        assertNotNull(publicKey);
        assertEquals("RSA", publicKey.getAlgorithm());
    }

    @Test
    void testGetPublicKey_shouldReturnNullIfNotLoaded() {
        assertNull(keyManager.getPublicKey("non-existent-key"));
    }

    @Test
    void testInit_shouldLoadKeysSuccessfully() {
        keyManager = new KeyManager();

        Path fakeBasePath = mock(Path.class);
        Path fakeFilePath = Paths.get("/dummy/path/test-key.pub");

        String keyContent = String.join("\n",
                "-----BEGIN PUBLIC KEY-----",
                "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsXQpHH5Wj9ce2j8skC/v",
                "fNH/4NgyHQq0BbsGdzrCeD3Q/nQhMx5RZxD0HNa79KRC+hdWNNyfBDkcf1Tfp+Ka",
                "oBjCXdc4U2ImrBaF+UUIqj07c5iRY25ZqtFdUXWEQ2f+Vgy+JhOdGVKYF9rTfIuJ",
                "1tn9mPfzZ0/yZzX6Vwr1C1RdsqgqHdGdr1xwZFcHXjUYw8VUsHRRbzX5v8yX7TLm",
                "TFJ9K0HTrYEm+lDkZkmU6iSlsyhr+3g4ph3KekA1UAX7wv3cgJfWLU1mVg9AVspK",
                "A4tZ7BFlUN+OtqDsTHYkthTy3dpGIp+nNB4ZpSgLmoGx9IMAtzH2H8+JEuGF1qQv",
                "WwIDAQAB",
                "-----END PUBLIC KEY-----");

        try (
                MockedStatic<Files> filesStatic = mockStatic(Files.class);
                MockedStatic<Paths> pathsStatic = mockStatic(Paths.class)) {
            // Mock static: PropertiesCache.getInstance()
            cacheStatic.when(PropertiesCache::getInstance).thenReturn(mockCache);
            when(mockCache.getProperty(Constants.ACCESS_TOKEN_PUBLICKEY_BASEPATH)).thenReturn("/dummy/path");

            // Mock static: Paths.get
            pathsStatic.when(() -> Paths.get("/dummy/path")).thenReturn(fakeBasePath);
            pathsStatic.when(() -> Paths.get("/dummy/path/test-key.pub")).thenReturn(fakeFilePath);

            // Mock static: Files.walk
            filesStatic.when(() -> Files.walk(fakeBasePath)).thenReturn(Stream.of(fakeFilePath));
            filesStatic.when(() -> Files.isRegularFile(fakeFilePath)).thenReturn(true);

            // Mock static: Files.readAllLines
            filesStatic.when(() -> Files.readAllLines(eq(fakeFilePath), eq(StandardCharsets.UTF_8)))
                    .thenReturn(List.of(keyContent.split("\n")));

            // Run the init
            keyManager.init();

            // Validate the map is populated
            KeyData keyData = keyManager.getPublicKey("test-key.pub");
            assertNotNull(keyData, "KeyData should not be null");
            assertNotNull(keyData.getPublicKey(), "PublicKey should not be null");
            assertEquals("RSA", keyData.getPublicKey().getAlgorithm());
        }
    }
}
