package com.igot.cb.service.impl;

import com.igot.cb.elasticsearch.service.EsUtilService;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class ProgramCoordinatorSyncService {

    private final EsUtilService esUtilService;

    @Value("${elastic.required.field.program.coordinator.json.path}")
    private String coordinatorIndexSchemaPath;

    @Value("${program.coordinator.es.index:program_coordinator_label_v1}")
    private String coordinatorIndex;

    public void syncFullCoordinatorIndex(String programId, List<Map<String, String>> coordinators) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("programId", programId);
        doc.put("coordinators", coordinators);

        String result = esUtilService.addDocument(
                coordinatorIndex,
                "_doc",
                programId,
                doc,
                coordinatorIndexSchemaPath
        );

        if (result == null) {
            throw new RuntimeException("Failed to sync coordinator index for programId: " + programId);
        }
    }

}
