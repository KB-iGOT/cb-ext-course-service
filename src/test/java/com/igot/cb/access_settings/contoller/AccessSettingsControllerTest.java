package com.igot.cb.access_settings.contoller;


import com.igot.cb.access_settings.service.AccessSettingsService;
import com.igot.cb.transactional.util.ApiResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;



import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AccessSettingsControllerTest {

  @InjectMocks
  private AccessSettingsController controller;

  @Mock
  private AccessSettingsService accessSettingsService;

  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (closeable != null) {
      closeable.close();
    }
  }

  @Test
  void testUpsert_ReturnsApiResponse() {
    Map<String, Object> userGroupDetails = new HashMap<>();
    String authToken = "dummy-token";
    ApiResponse mockResponse = new ApiResponse();
    mockResponse.setResponseCode(HttpStatus.OK);

    when(accessSettingsService.upsert(userGroupDetails, authToken)).thenReturn(mockResponse);

    ResponseEntity<ApiResponse> responseEntity = controller.upsert(userGroupDetails, authToken);

    assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
    assertEquals(mockResponse, responseEntity.getBody());
    verify(accessSettingsService, times(1)).upsert(userGroupDetails, authToken);
  }

  @Test
  void testRead_ReturnsApiResponse() {
    String doId = "do_12345";
    ApiResponse mockResponse = new ApiResponse();
    mockResponse.setResponseCode(HttpStatus.OK);

    when(accessSettingsService.read(doId)).thenReturn(mockResponse);

    ResponseEntity<ApiResponse> responseEntity = controller.read(doId);

    assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
    assertEquals(mockResponse, responseEntity.getBody());
    verify(accessSettingsService, times(1)).read(doId);
  }

  @Test
  void testDelete_ReturnsApiResponse() {
    String contentId = "cid-123";
    ApiResponse mockResponse = new ApiResponse();
    mockResponse.setResponseCode(HttpStatus.OK);

    when(accessSettingsService.delete(contentId)).thenReturn(mockResponse);

    ResponseEntity<ApiResponse> responseEntity = controller.delete(contentId);

    assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
    assertEquals(mockResponse, responseEntity.getBody());
    verify(accessSettingsService, times(1)).delete(contentId);
  }

}
