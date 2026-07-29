package com.igot.cb.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.service.impl.ProgramCoordinatorSyncService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@AllArgsConstructor
public class ProgramCoordinatorSyncConsumer {


    private final ProgramCoordinatorSyncService syncService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${program.coordinator.sync.topic}", groupId = "${program.coordinator.sync.topic.group}")
    public void processProgramCoordinatorSyncMessage(ConsumerRecord<String, String> data) {
        log.info("ProgramCoordinatorSyncConsumer::processMessage: Received event to sync program coordinators to ES...");
        log.info("Received message:: " + data.value());
        try {
            if (StringUtils.isNotBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    try {
                        processSyncMessage(data.value());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                log.error("Error in Program Coordinator Sync Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            log.error(String.format("Error in Program Coordinator Sync Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }

    private void processSyncMessage(String message) throws Exception {
        Map<String, Object> event = objectMapper.readValue(message, Map.class);

        String programId = (String) event.get("programId");
        List<Map<String, String>> coordinators = (List<Map<String, String>>) event.get("coordinators");

        if (StringUtils.isBlank(programId) || coordinators == null) {
            log.error("Invalid coordinator sync event, missing programId or coordinators: {}", message);
            return;
        }

        syncService.syncFullCoordinatorIndex(programId, coordinators);
        log.info("Synced coordinator list to ES for programId: {}, count: {}", programId, coordinators.size());
    }
}
