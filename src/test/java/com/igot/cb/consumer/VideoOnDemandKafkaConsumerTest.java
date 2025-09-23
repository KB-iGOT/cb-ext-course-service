package com.igot.cb.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class VideoOnDemandKafkaConsumerTest {

    @Mock
    private ObjectMapper mapper;

    @Mock
    private ServerProperties serverProperties;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @InjectMocks
    private VideoOnDemandKafkaConsumer videoOnDemandKafkaConsumer;

    @Test
    void test_content_metadata_update_consumer_processes_valid_json_message() throws Exception {
        String validJsonMessage = "{\"identifier\":\"test-id-123\",\"streamUri\":\"https://example.com/stream\"}";
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0L, "key", validJsonMessage);
    
        Map<String, String> messageData = new HashMap<>();
        messageData.put(Constants.IDENTIFIER, "test-id-123");
        messageData.put(Constants.STREAMING_URI, "https://example.com/stream");
        
        Map<String, Object> mockResponse = new HashMap<>();
        mockResponse.put(Constants.RESPONSE_CODE, Constants.OK);
    
        when(mapper.readValue(eq(validJsonMessage), any(TypeReference.class))).thenReturn(messageData);
        when(serverProperties.getLearningServiceVmBaseUrl()).thenReturn("https://api.example.com");
        when(serverProperties.getSystemUpdateAPI()).thenReturn("/update/");
        when(serverProperties.getVodBucketPrefix()).thenReturn("bucket-prefix");
        when(serverProperties.getVodStreamUrlPrefix()).thenReturn("stream-prefix");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any())).thenReturn(mockResponse);
    
        videoOnDemandKafkaConsumer.contentMetadataUpdateConsumerForVOD(consumerRecord);
    
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        verify(mapper).readValue(eq(validJsonMessage), any(TypeReference.class));
    }

    @Test
    void test_content_metadata_update_consumer_handles_blank_message_gracefully() {
        ConsumerRecord<String, String> consumerRecordWithBlank = new ConsumerRecord<>("test-topic", 0, 0L, "key", "");
        ConsumerRecord<String, String> consumerRecordWithNull = new ConsumerRecord<>("test-topic", 0, 0L, "key", null);
    
        videoOnDemandKafkaConsumer.contentMetadataUpdateConsumerForVOD(consumerRecordWithBlank);
        videoOnDemandKafkaConsumer.contentMetadataUpdateConsumerForVOD(consumerRecordWithNull);

        try {
            verify(mapper, never()).readValue(anyString(), any(TypeReference.class));
        } catch (JsonProcessingException e) {
        }
        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), any(), any());
    }

    @Test
    void test_invalid_json_message_handling() throws Exception {
        String invalidJsonMessage = "{invalid json}";
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0L, "key", invalidJsonMessage);
    
        when(mapper.readValue(eq(invalidJsonMessage), any(TypeReference.class)))
            .thenThrow(new JsonProcessingException("Invalid JSON") {});
    
        videoOnDemandKafkaConsumer.contentMetadataUpdateConsumerForVOD(consumerRecord);
    
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        verify(mapper).readValue(eq(invalidJsonMessage), any(TypeReference.class));
        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), any(), any());
    }

    @Test
    void test_missing_required_fields() throws Exception {
        String messageWithMissingFields = "{\"identifier\":\"test-id-123\"}";
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0L, "key", messageWithMissingFields);
    
        Map<String, String> messageData = new HashMap<>();
        messageData.put(Constants.IDENTIFIER, "test-id-123");
    
        when(mapper.readValue(eq(messageWithMissingFields), any(TypeReference.class))).thenReturn(messageData);
    
        videoOnDemandKafkaConsumer.contentMetadataUpdateConsumerForVOD(consumerRecord);
    
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        verify(mapper).readValue(eq(messageWithMissingFields), any(TypeReference.class));
        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), any(), any());
    }

    @Test
    void test_api_call_failure() throws Exception {
        String validJsonMessage = "{\"identifier\":\"test-id-123\",\"streamUri\":\"https://example.com/stream\"}";
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0L, "key", validJsonMessage);
    
        Map<String, String> messageData = new HashMap<>();
        messageData.put(Constants.IDENTIFIER, "test-id-123");
        messageData.put(Constants.STREAMING_URI, "https://example.com/stream");
        
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put(Constants.RESPONSE_CODE, "FAILED");
    
        when(mapper.readValue(eq(validJsonMessage), any(TypeReference.class))).thenReturn(messageData);
        when(serverProperties.getLearningServiceVmBaseUrl()).thenReturn("https://api.example.com");
        when(serverProperties.getSystemUpdateAPI()).thenReturn("/update/");
        when(serverProperties.getVodBucketPrefix()).thenReturn("bucket-prefix");
        when(serverProperties.getVodStreamUrlPrefix()).thenReturn("stream-prefix");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any())).thenReturn(errorResponse);
    
        videoOnDemandKafkaConsumer.contentMetadataUpdateConsumerForVOD(consumerRecord);
    
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), any(), any());
    }

    @Test
    void test_url_transformation() throws Exception {
        String validJsonMessage = "{\"identifier\":\"test-id-123\",\"streamUri\":\"https://bucket-prefix/video.mp4\"}";
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0L, "key", validJsonMessage);
    
        Map<String, String> messageData = new HashMap<>();
        messageData.put(Constants.IDENTIFIER, "test-id-123");
        messageData.put(Constants.STREAMING_URI, "https://bucket-prefix/video.mp4");
        
        Map<String, Object> mockResponse = new HashMap<>();
        mockResponse.put(Constants.RESPONSE_CODE, Constants.OK);
    
        when(mapper.readValue(eq(validJsonMessage), any(TypeReference.class))).thenReturn(messageData);
        when(serverProperties.getLearningServiceVmBaseUrl()).thenReturn("https://api.example.com");
        when(serverProperties.getSystemUpdateAPI()).thenReturn("/update/");
        when(serverProperties.getVodBucketPrefix()).thenReturn("bucket-prefix");
        when(serverProperties.getVodStreamUrlPrefix()).thenReturn("stream-prefix");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any())).thenReturn(mockResponse);
    
        videoOnDemandKafkaConsumer.contentMetadataUpdateConsumerForVOD(consumerRecord);
    
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        verify(outboundRequestHandlerService).fetchResultUsingPatch(
            eq("https://api.example.com/update/test-id-123"),
            any(Map.class),
            any(Map.class)
        );
    }
}