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

    @Test
    void testGetCbPlanUpdateAllowedFields() {
        CbExtServerProperties properties = new CbExtServerProperties();
        ReflectionTestUtils.setField(properties, "cbPlanUpdateAllowedFields", "field1,field2,field3");
        List<String> fields = properties.getCbPlanUpdateAllowedFields();
        assertEquals(3, fields.size());
        assertTrue(fields.contains("field1"));
        assertTrue(fields.contains("field2"));
        assertTrue(fields.contains("field3"));
    }
    @Test
    void testPrivateVariablesViaGettersAndSetters() {
    CbExtServerProperties properties = new CbExtServerProperties();

    // Set values using setters
    properties.setCpPlanIndex("testIndex");
    properties.setElasticCbPlanJsonPath("testPath");
    properties.setMsgOnUserGroupRestrictionForAllOrg("testMsg");
    properties.setNonTextFields("fieldA,fieldB");
    properties.setNotificationSupportMail("support@test.com");
    properties.setSbUrl("http://test-sb-url");
    properties.setUserSearchEndPoint("http://test-user-search");
    properties.setNotificationServiceHost("http://test-host");
    properties.setNotificationAsyncPath("/async");
    properties.setCbWrapperNotificationHost("http://wrapper-host");
    properties.setCbWrapperNotificationPath("/wrapper-path");

    // Assert values using getters
    assertEquals("testIndex", properties.getCpPlanIndex());
    assertEquals("testPath", properties.getElasticCbPlanJsonPath());
    assertEquals("testMsg", properties.getMsgOnUserGroupRestrictionForAllOrg());
    assertEquals("fieldA,fieldB", properties.getNonTextFields());
    assertEquals("support@test.com", properties.getNotificationSupportMail());
    assertEquals("http://test-sb-url", properties.getSbUrl());
    assertEquals("http://test-user-search", properties.getUserSearchEndPoint());
    assertEquals("http://test-host", properties.getNotificationServiceHost());
    assertEquals("/async", properties.getNotificationAsyncPath());
    assertEquals("http://wrapper-host", properties.getCbWrapperNotificationHost());
    assertEquals("/wrapper-path", properties.getCbWrapperNotificationPath());
}
}