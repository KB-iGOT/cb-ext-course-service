package com.igot.cb.util;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class CbExtServerPropertiesTest {

    @Test
    void testGetCbPlanUpdatePublishAuthorizedRoles() {
        CbExtServerProperties properties = new CbExtServerProperties();
        ReflectionTestUtils.setField(properties, "cbPlanUpdatePublishAuthorizedRoles", "ADMIN,MANAGER,EDITOR");
        
        List<String> roles = properties.getCbPlanUpdatePublishAuthorizedRoles();
        
        assertEquals(3, roles.size());
        assertTrue(roles.contains("ADMIN"));
        assertTrue(roles.contains("MANAGER"));
        assertTrue(roles.contains("EDITOR"));
    }

    @Test
    void testGetCbPlanUpdatePublishAuthorizedRolesWithSingleRole() {
        CbExtServerProperties properties = new CbExtServerProperties();
        ReflectionTestUtils.setField(properties, "cbPlanUpdatePublishAuthorizedRoles", "ADMIN");
        
        List<String> roles = properties.getCbPlanUpdatePublishAuthorizedRoles();
        
        assertEquals(1, roles.size());
        assertEquals("ADMIN", roles.get(0));
    }

    @Test
    void testGetCbPlanUpdatePublishAuthorizedRolesWithEmptyString() {
        CbExtServerProperties properties = new CbExtServerProperties();
        ReflectionTestUtils.setField(properties, "cbPlanUpdatePublishAuthorizedRoles", "");
        
        List<String> roles = properties.getCbPlanUpdatePublishAuthorizedRoles();
        
        assertEquals(1, roles.size());
        assertEquals("", roles.get(0));
    }

    @Test
    void testSetCbPlanUpdatePublishAuthorizedRoles() {
        CbExtServerProperties properties = new CbExtServerProperties();
        
        properties.setCbPlanUpdatePublishAuthorizedRoles("USER,GUEST");
        
        List<String> roles = properties.getCbPlanUpdatePublishAuthorizedRoles();
        assertEquals(2, roles.size());
        assertTrue(roles.contains("USER"));
        assertTrue(roles.contains("GUEST"));
    }
}