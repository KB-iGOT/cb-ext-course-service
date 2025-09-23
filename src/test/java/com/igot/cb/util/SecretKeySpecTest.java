package com.igot.cb.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import javax.crypto.SecretKey;
import static org.junit.jupiter.api.Assertions.*;

class SecretKeySpecTest {

    private SecretKeySpec secretKeySpec;

    @BeforeEach
    void setUp() {
        secretKeySpec = new SecretKeySpec();
        secretKeySpec.postConstruct();
    }

    @Test
    void testGetAlgorithm() {
        assertEquals(Constants.CIPHER_ALGORITHM, secretKeySpec.getAlgorithm());
    }

    @Test
    void testGetFormat() {
        assertEquals("RAW", secretKeySpec.getFormat());
    }

    @Test
    void testGetEncoded() {
        byte[] encoded = secretKeySpec.getEncoded();
        assertNotNull(encoded);
        assertArrayEquals(Constants.CIPHER_KEY, encoded);
    }

    @Test
    void testHashCode() {
        int hashCode = secretKeySpec.hashCode();
        assertTrue(hashCode != 0);
    }

    @Test
    void testEquals() {
        SecretKeySpec other = new SecretKeySpec();
        other.postConstruct();
        
        assertTrue(secretKeySpec.equals(secretKeySpec));
        assertTrue(secretKeySpec.equals(other));
        assertFalse(secretKeySpec.equals(null));
        assertFalse(secretKeySpec.equals("not a secret key"));
    }

    @Test
    void testEqualsWithDifferentAlgorithm() {
        SecretKey mockKey = new SecretKey() {
            @Override
            public String getAlgorithm() {
                return "DifferentAlgorithm";
            }

            @Override
            public String getFormat() {
                return "RAW";
            }

            @Override
            public byte[] getEncoded() {
                return Constants.CIPHER_KEY;
            }
        };
        
        assertFalse(secretKeySpec.equals(mockKey));
    }

    @Test
    void testEqualsWithTripleDES() {
        // Create a custom SecretKeySpec with TripleDES algorithm
        SecretKeySpec tripleDESKey = new SecretKeySpec();
        tripleDESKey.postConstruct();
        // Override the algorithm field using reflection
        try {
            java.lang.reflect.Field algorithmField = SecretKeySpec.class.getDeclaredField("algorithm");
            algorithmField.setAccessible(true);
            algorithmField.set(tripleDESKey, "TripleDES");
        } catch (Exception e) {
            fail("Failed to set algorithm field");
        }
        
        SecretKey desedeKey = new SecretKey() {
            @Override
            public String getAlgorithm() {
                return "DESede";
            }

            @Override
            public String getFormat() {
                return "RAW";
            }

            @Override
            public byte[] getEncoded() {
                return Constants.CIPHER_KEY;
            }
        };
        
        assertTrue(tripleDESKey.equals(desedeKey));
    }

    @Test
    void testHashCodeWithTripleDES() {
        SecretKeySpec tripleDESKey = new SecretKeySpec();
        tripleDESKey.postConstruct();
        // Override the algorithm field using reflection
        try {
            java.lang.reflect.Field algorithmField = SecretKeySpec.class.getDeclaredField("algorithm");
            algorithmField.setAccessible(true);
            algorithmField.set(tripleDESKey, "TripleDES");
        } catch (Exception e) {
            fail("Failed to set algorithm field");
        }
        
        int hashCode = tripleDESKey.hashCode();
        assertTrue(hashCode != 0);
    }

    @Test
    void testEqualsWithDifferentKey() {
        SecretKey differentKey = new SecretKey() {
            @Override
            public String getAlgorithm() {
                return Constants.CIPHER_ALGORITHM;
            }

            @Override
            public String getFormat() {
                return "RAW";
            }

            @Override
            public byte[] getEncoded() {
                return new byte[]{1, 2, 3, 4};
            }
        };
        
        assertFalse(secretKeySpec.equals(differentKey));
    }
}