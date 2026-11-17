package com.igot.cb.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ProviderAccessSettingsServiceImpl;

@ExtendWith(MockitoExtension.class)
class ProviderAccessSettingsControllerTest {

    private static final String PARTNER_ID = "partner-1";
    private static final Map<String, Object> REQUEST = Map.of("partnerId", PARTNER_ID);

    @Mock
    private ProviderAccessSettingsServiceImpl providerAccessSettingsService;

    @InjectMocks
    private ProviderAccessSettingsController controller;

    private ApiResponse response(HttpStatus status) {
        ApiResponse response = ApiResponse.createDefaultResponse("api.test");
        response.setResponseCode(status);
        return response;
    }

    @Test
    void upsert() {
        ApiResponse expected = response(HttpStatus.OK);
        when(providerAccessSettingsService.upsert(REQUEST, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.upsert(REQUEST, "token");

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertSame(expected, result.getBody());
    }

    @Test
    void read() {
        ApiResponse expected = response(HttpStatus.NOT_FOUND);
        when(providerAccessSettingsService.read(PARTNER_ID, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.read(PARTNER_ID, "token");

        assertEquals(HttpStatus.NOT_FOUND, result.getStatusCode());
        assertSame(expected, result.getBody());
    }

    @Test
    void notificationUpsert() {
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(providerAccessSettingsService.notificationUpsert(REQUEST, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.notificationUpsert(REQUEST, "token");

        assertEquals(HttpStatus.BAD_REQUEST, result.getStatusCode());
        assertSame(expected, result.getBody());
    }

    @Test
    void validate() {
        ApiResponse expected = response(HttpStatus.OK);
        when(providerAccessSettingsService.validate(REQUEST)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.validate(REQUEST);

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertSame(expected, result.getBody());
    }

    @Test
    void rebalance() {
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(providerAccessSettingsService.rebalance(REQUEST)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.rebalance(REQUEST);

        assertEquals(HttpStatus.BAD_REQUEST, result.getStatusCode());
        assertSame(expected, result.getBody());
    }

    @Test
    void notificationRead() {
        ApiResponse expected = response(HttpStatus.OK);
        when(providerAccessSettingsService.notificationRead(PARTNER_ID, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.notificationRead(PARTNER_ID, "token");

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertSame(expected, result.getBody());
    }
}
