package com.igot.cb.consentacknowledge;

import com.igot.cb.model.ApiResponse;

import java.util.Map;

public interface IConsentAcknowledgeService {

    /**
     * Acknowledge a declaration based on the provided request data and authentication token.
     *
     * @param request   A map containing the request data for acknowledging the declaration.
     * @param authToken The authentication token for validating the request.
     * @return An ApiResponse object containing the result of the acknowledgment operation.
     */
    ApiResponse acknowledgeDeclaration(Map<String, Object> request, String authToken);

    /**
     * Retrieve consent acknowledgement details based on the provided content ID, consent ID, and authentication token.
     *
     * @param contentId The ID of the content to retrieve details for.
     * @param consentId The ID of the consent to retrieve details for.
     * @param authToken The authentication token for validating the request.
     * @return An ApiResponse object containing the consent acknowledgement details.
     */
    ApiResponse getConsentAcknowledgementDetails(String contentId, String consentId, String authToken);
}
