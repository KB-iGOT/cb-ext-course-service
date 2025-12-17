package com.igot.cb.schedular;

import com.igot.cb.service.ContentRetirementService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ContentRetirementSchedular {

    private final ContentRetirementService contentRetirementService;

    public ContentRetirementSchedular(ContentRetirementService contentRetirementService) {
        this.contentRetirementService = contentRetirementService;
    }

    @Scheduled(cron = "${content.retirement.scheduler.cron}")
    public void runDaily() {
        log.info("Starting nightly content retirement scheduler");
        contentRetirementService.processDueRetirements();
    }
}
