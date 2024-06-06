package org.sunbird.workflow.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.sunbird.workflow.models.WfRequest;
import org.sunbird.workflow.service.impl.ApplicationProcessingServiceImpl;
import org.sunbird.workflow.service.impl.WorkflowAuditProcessingServiceImpl;

@Service
public class ApplicationProcessingConsumer {

    Logger logger = LogManager.getLogger(ApplicationProcessingConsumer.class);

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private ApplicationProcessingServiceImpl applicationProcessingServiceImpl;

    @Autowired
    private WorkflowAuditProcessingServiceImpl workflowAuditProcessingService;


    @KafkaListener(groupId = "workflowContentTopic-consumer", topics = "${kafka.topics.workflow.request}")
    public void processMessage(ConsumerRecord<String, String> data) {
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    processApplicationMessage(data.value());
                });
            } else {
                logger.error("Error in Application Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in Application Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }

    private void processApplicationMessage(String strData) {
        WfRequest wfRequest = null;
        try {
            wfRequest = mapper.readValue(strData, WfRequest.class);
            logger.info("Recevied data in user profile consumer : {}", mapper.writeValueAsString(wfRequest));
        } catch (Exception ex) {
            logger.error("Error while deserialization the object value", ex);
        }
        applicationProcessingServiceImpl.processWfApplicationRequest(wfRequest);
        workflowAuditProcessingService.createAudit(wfRequest);
        applicationProcessingServiceImpl.updateDepartmentToPortalDBs(wfRequest);
    }
}


