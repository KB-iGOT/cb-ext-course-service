package com.igot.cb.access_settings.contoller;

import com.igot.cb.access_settings.service.CourseService;
import com.igot.cb.transactional.util.ApiResponse;
import com.igot.cb.transactional.util.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/content/v2")
public class CourseController {
    @Autowired
    private CourseService courseService;

    @PostMapping("/state/read")
    public ResponseEntity<Object> readContentState(@RequestBody Map<String, Object> requestBody, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseService.readContentState(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PatchMapping("/state/update")
    public ResponseEntity<Object> updateContentState(@RequestBody Map<String, Object> requestBody, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseService.updateContentState(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
