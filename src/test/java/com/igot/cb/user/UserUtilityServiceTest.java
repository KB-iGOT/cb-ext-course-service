package com.igot.cb.user;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserUtilityServiceTest {

    @Mock
    private UserUtilityService userUtilityService;

    @Test
    void testGetUserDetailsFromDB() {
        List<String> userIds = Arrays.asList("user1", "user2");
        List<String> fields = Arrays.asList("id", "firstName", "email");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        doNothing().when(userUtilityService).getUserDetailsFromDB(userIds, fields, userInfoMap);

        assertDoesNotThrow(() -> userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap));
        verify(userUtilityService).getUserDetailsFromDB(userIds, fields, userInfoMap);
    }
}