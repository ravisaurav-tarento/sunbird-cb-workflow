package org.sunbird.workflow.consumer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.sunbird.workflow.config.Configuration;
import org.sunbird.workflow.config.Constants;
import org.sunbird.workflow.models.WfRequest;
import org.sunbird.workflow.service.impl.NotificationServiceImpl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.workflow.utils.CassandraOperation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class NotificationConsumer {
	Logger logger = LogManager.getLogger(NotificationConsumer.class);

	@Autowired
	private ObjectMapper mapper;

	@Autowired
	private NotificationServiceImpl notificationService;

	@Autowired
	private CassandraOperation cassandraOperation;

	@Autowired
	private Configuration configuration;

	@KafkaListener(groupId = "workflowNotificationTopic-consumer", topics = "${kafka.topics.workflow.notification}")
	public void processMessage(ConsumerRecord<String, String> data) {
		try {
            if (StringUtils.isNoneBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    processNotificationRequest(data.value());
                });
            } else {
                logger.error("Error in Notification Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in Notification Consumer: Error Msg :%s", e.getMessage()), e);
        }
	}

	private void processNotificationRequest(String strData) {
		WfRequest wfRequest = null;
		try {
			wfRequest = mapper.readValue(strData, WfRequest.class);
			logger.info("Recevied data in notification consumer : {}", mapper.writeValueAsString(wfRequest));
			switch (wfRequest.getServiceName()) {
				case Constants.PROFILE_SERVICE_NAME:
					notificationService.sendNotification(wfRequest);
					notificationService.sendNotificationToMdoAdmin(wfRequest);
					break;
				case Constants.POSITION_SERVICE_NAME:
				case Constants.DOMAIN_SERVICE_NAME:
				case Constants.ORGANISATION_SERVICE_NAME:
					notificationService.sendEmailNotification(wfRequest);
					break;
				case Constants.BLENDED_PROGRAM_SERVICE_NAME:
				case Constants.ONE_STEP_MDO_APPROVAL:
				case Constants.ONE_STEP_PC_APPROVAL:
				case Constants.TWO_STEP_MDO_AND_PC_APPROVAL:
				case Constants.TWO_STEP_PC_AND_MDO_APPROVAL:
					Map<String, Object> courseAttributes = getCourseAttributes(wfRequest.getCourseId());
					wfRequest.setCourseName((String) courseAttributes.get(Constants.COURSE_NAME));
					notificationService.sendNotification(wfRequest);
					notificationService.sendNotificationToMdoAdminAndPC(wfRequest);
					break;
				case Constants.USER_REGISTRATION_SERVICE_NAME:
					// nothing to do
					break;
				default:
					logger.error("Unsupported ServiceName in WFRequest.");
					break;
			}
		} catch (Exception ex) {
			logger.error("Error while deserialization the object value", ex);
		}
	}

	public Map<String, Object> getCourseAttributes(String courseId){
		Map<String, Object> propertiesMap = new HashMap<>();
		Map<String, Object> courseDetails = new HashMap<>();
		propertiesMap.put(Constants.IDENTIFIER, courseId);
		List<Map<String, Object>> coursesDataList = cassandraOperation.getRecordsByProperties(configuration.getEnvHierarchyKeystoreKeyspaceName(),
				Constants.CONTENT_HIERARCHY,
				propertiesMap,
				Arrays.asList(Constants.IDENTIFIER, Constants.HIERARCHY));
		Map<String, Object> hierarchy = new Gson().fromJson((String) coursesDataList.get(0).get("hierarchy"), new TypeToken<HashMap<String, Object>>(){}.getType());
		courseDetails.put(Constants.COURSE_NAME, hierarchy.get(Constants.NAME));
		return courseDetails;
	}

}
