package com.igot.cb.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.igot.cb.model.ApiRespParam;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.AccessSettingsServiceImpl;

@ExtendWith(MockitoExtension.class)
class AccessSettingsControllerTest {

    @Mock
    private AccessSettingsServiceImpl accessSettingsService;

    @InjectMocks
    private AccessSettingsController accessSettingsController;

    private ApiResponse createApiResponse(String id, HttpStatus code, Map<String, Object> result) {
        ApiResponse response = new ApiResponse(id);
        ApiRespParam params = new ApiRespParam("mock-res-id");
        params.setStatus("successful");
        response.setParams(params);
        response.setResponseCode(code);
        response.setResult(result);
        return response;
    }

    @Test
    void testUpsert() {
        Map<String, Object> input = Map.of("groupId", "g1");
        ApiResponse mockResponse = createApiResponse("api.upsert", HttpStatus.OK, Map.of("status", "created"));

        when(accessSettingsService.upsert(eq(input), eq("mock-token"))).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = accessSettingsController.upsert(input, "mock-token");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("api.upsert", response.getBody().getId());
        assertEquals("created", response.getBody().getResult().get("status"));
    }

    @Test
    void testRead() {
        String contentId = "content-1";
        ApiResponse mockResponse = createApiResponse("api.read", HttpStatus.OK, Map.of("id", contentId));

        when(accessSettingsService.read(contentId)).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = accessSettingsController.read(contentId);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("api.read", response.getBody().getId());
        assertEquals("content-1", response.getBody().getResult().get("id"));
    }

    @Test
    void testDelete() {
        String contentId = "content-2";
        ApiResponse mockResponse = createApiResponse("api.delete", HttpStatus.NO_CONTENT, Map.of("deleted", true));

        when(accessSettingsService.delete(contentId)).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = accessSettingsController.delete(contentId);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        assertEquals("api.delete", response.getBody().getId());
        assertEquals(true, response.getBody().getResult().get("deleted"));
    }
}