package com.igot.cb.cbplan.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents a CA (Content Association) event consumed from Kafka.
 * Supports ADD and REMOVE operations on a training plan's content list.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class CbPlanCaEvent {

    private String eventType;
    private String trainingPlanId;
    private String caIdentifier;
}
