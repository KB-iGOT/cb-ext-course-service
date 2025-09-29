package com.igot.cb.consentacknowledge;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/consent")
public class ConsentAcknowledgeController {

    private final IConsentAcknowledgeService acknowledgeService;

    public ConsentAcknowledgeController(IConsentAcknowledgeService acknowledgeService) {
        this.acknowledgeService = acknowledgeService;
    }

    @PostMapping("/acknowledge")
    public ResponseEntity<ApiResponse> acknowledgeDeclaration(
            @RequestHeader(value = Constants.X_AUTH_TOKEN) String authToken,
            @RequestBody Map<String, Object> request) {
        ApiResponse response = acknowledgeService.acknowledgeDeclaration(request, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }


    @GetMapping("/details/{consentId}")
    public ResponseEntity<ApiResponse> getConsentDetails(@PathVariable String consentId,
                                         @RequestHeader(value = Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = acknowledgeService.getConsentDetails(consentId, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

}
