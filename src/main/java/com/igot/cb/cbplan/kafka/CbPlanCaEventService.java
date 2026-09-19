package com.igot.cb.cbplan.kafka;

/**
 * Service interface for processing CB Plan CA (Content Association) Kafka events.
 */
public interface CbPlanCaEventService {

    /**
     * Processes an ADD or REMOVE CA event by updating the plan's content list
     * in Cassandra and syncing the change to ElasticSearch.
     *
     * @param event the CA event to process
     */
    void processEvent(CbPlanCaEvent event);
}
