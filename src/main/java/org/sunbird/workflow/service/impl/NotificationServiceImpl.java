package org.sunbird.workflow.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.sunbird.workflow.config.Configuration;
import org.sunbird.workflow.config.Constants;
import org.sunbird.workflow.consumer.ApplicationProcessingConsumer;
import org.sunbird.workflow.models.WfRequest;
import org.sunbird.workflow.models.WfStatus;
import org.sunbird.workflow.models.notification.Config;
import org.sunbird.workflow.models.notification.NotificationRequest;
import org.sunbird.workflow.models.notification.Template;
import org.sunbird.workflow.postgres.entity.WfStatusEntity;
import org.sunbird.workflow.postgres.repo.WfStatusRepo;
import org.sunbird.workflow.service.ContentReadService;
import org.sunbird.workflow.service.Workflowservice;
import org.sunbird.workflow.utils.CassandraOperation;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class NotificationServiceImpl {

	public static final String EMAILTEMPLATE = "emailtemplate";
	Logger logger = LogManager.getLogger(ApplicationProcessingConsumer.class);

	@Autowired
	private WfStatusRepo wfStatusRepo;

	@Autowired
	private Configuration configuration;

	@Autowired
	private RequestServiceImpl requestService;

	@Autowired
	private ObjectMapper mapper;

	@Autowired
	private Workflowservice workflowservice;

	@Autowired
	private UserProfileWfServiceImpl userProfileWfService;

	@Autowired
	private CassandraOperation cassandraOperation;

	@Autowired
	private ContentReadService contentReadService;

	private static final String WORK_FLOW_EVENT_NAME = "workflow_service_notification";

	private static final String USER_NAME_CONSTANT = "user";

	private static final String USER_NAME_TAG = "#userName";

	private static final String STATE_NAME_TAG = "#state";

	private static final String FIELD_KEY_TAG = "#fieldKey";

	private static final String TO_VALUE_TAG = "#toValue";

	private static final String COMMENT_TAG = "#comment";

	private static final String TO_VALUE_CONST = "toValue";

	private static final String MAIL_SUBJECT = "Your request is #state";

	private static final String MDO_MAIL_SUBJECT = "Request for approval";

	private static final String MAIL_BODY = "Your request to update #fieldKey to #toValue is #state ";

	private static final String BP_MAIL_BODY = "Your request for batch enrollment is #state.";

	private static final String COURSE_NAME_TAG = "#course_name";

	private static final String BATCH_NAME_TAG = "#batch_name";

	private static final String Role_TAG = "#role";

	private static final String ACT_TAG = "#action";

	private static final String BATCH_START_DATE_TAG = "#batch_start_date";

	private static final String ENROLMENT_ACTION_SUBJECT = "Enrollment #action";

	private static final String BP_MDO_PC_SUBJECT_LINE = "Enrollment Request Forwarded to #role";

	private static final String USERNAMAE_TAG = "#username";

	private static final String BLENDED_PROGRAME_NAME_TAG = "#blended_programme_name";

	private static final String ROLE_TAG = "#role";

	/**
	 * Send notification to the user based on state of application
	 *
	 * @param wfRequest workflow request
	 */
	public void sendNotification(WfRequest wfRequest) {
		boolean sendEmailnotification = true;
		WfStatusEntity wfStatusEntity = wfStatusRepo.findByApplicationIdAndWfId(wfRequest.getApplicationId(),
				wfRequest.getWfId());
		WfStatus wfStatus = workflowservice.getWorkflowStates(wfStatusEntity.getRootOrg(), wfStatusEntity.getOrg(),
				wfStatusEntity.getServiceName(), wfStatusEntity.getCurrentStatus());
		try {
			logger.info("Notification workflow status entity, {}", mapper.writeValueAsString(wfStatusEntity));
			logger.info("Notification workflow status model, {}", mapper.writeValueAsString(wfStatus));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		if (!ObjectUtils.isEmpty(wfStatus.getNotificationEnable()) && wfStatus.getNotificationEnable()) {
			logger.info("Enter's in the notification block");
            Set<String> usersId = new HashSet<>();
            usersId.add(wfRequest.getUserId());
            Set<String> blendedProgrammeServiceNames = new HashSet<>(Arrays.asList(Constants.BLENDED_PROGRAM_SERVICE_NAME,
                    Constants.ONE_STEP_MDO_APPROVAL,
                    Constants.ONE_STEP_PC_APPROVAL,
                    Constants.TWO_STEP_MDO_AND_PC_APPROVAL,
                    Constants.TWO_STEP_PC_AND_MDO_APPROVAL));
			if(!blendedProgrammeServiceNames.contains(wfRequest.getServiceName())){
				usersId.add(wfStatusEntity.getApplicationId());
			}
			HashMap<String, Object> usersObj = userProfileWfService.getUsersResult(usersId);
			Map<String, Object> recipientInfo;
			if (Constants.BLENDED_PROGRAM_SERVICE_NAME.equalsIgnoreCase(wfRequest.getServiceName())) {
				recipientInfo = (Map<String, Object>)usersObj.get(wfRequest.getUserId());
			} else {
				recipientInfo = (Map<String, Object>)usersObj.get(wfStatusEntity.getApplicationId());
			}
			Map<String, Object> senderInfo = (Map<String, Object>)usersObj.get(wfRequest.getUserId());
			Optional<HashMap<String, Object>> updatedFieldValue = wfRequest.getUpdateFieldValues().stream().findFirst();
			String subjectLine = "";
			String body = "";
			if (updatedFieldValue.isPresent()) {
				if (blendedProgrammeServiceNames.contains(wfRequest.getServiceName())) {
					String forwardedMailBody = configuration.getLearnerForwardedMailBody()
							.replace(COURSE_NAME_TAG, wfRequest.getCourseName())
							.replace(BATCH_NAME_TAG, wfRequest.getBatchName())
							.replace(BATCH_START_DATE_TAG, wfRequest.getBatchStartDate().toString());
					switch (wfStatusEntity.getCurrentStatus()){
						case Constants.SEND_FOR_PC_APPROVAL:
							subjectLine = BP_MDO_PC_SUBJECT_LINE.replace(Role_TAG,Constants.PROGRAM_COORDINATOR.replace("_"," "));
							body = forwardedMailBody.replace(Role_TAG,Constants.PROGRAM_COORDINATOR.replace("_"," "));
							break;
						case Constants.SEND_FOR_MDO_APPROVAL:
							subjectLine = BP_MDO_PC_SUBJECT_LINE.replace(Role_TAG,Constants.MDO_ADMIN.split("_")[0]);
							body = forwardedMailBody.replace(Role_TAG,Constants.MDO_ADMIN.split("_")[0]);
							break;
						case Constants.APPROVED:
							subjectLine = ENROLMENT_ACTION_SUBJECT.replace(ACT_TAG, wfStatusEntity.getCurrentStatus());
							body = configuration.getApprovedMailBody()
									.replace(BATCH_NAME_TAG, wfRequest.getBatchName())
									.replace(COURSE_NAME_TAG, wfRequest.getCourseName())
									.replace(BATCH_START_DATE_TAG, wfRequest.getBatchStartDate().toString());
							break;
						case Constants.REJECTED:
						case Constants.REMOVED:
							subjectLine = ENROLMENT_ACTION_SUBJECT.replace(ACT_TAG, wfStatusEntity.getCurrentStatus());
							body = configuration.getRejectedOrRemovedMailBody()
									.replace(BATCH_NAME_TAG, wfRequest.getBatchName())
									.replace(COURSE_NAME_TAG, wfRequest.getCourseName())
									.replace(ACT_TAG,wfStatusEntity.getCurrentStatus())
									.replace(BATCH_START_DATE_TAG, wfRequest.getBatchStartDate().toString());
							break;
						default:
							subjectLine = MAIL_SUBJECT.replace(STATE_NAME_TAG, wfStatusEntity.getCurrentStatus());
							body = BP_MAIL_BODY.replace(STATE_NAME_TAG, wfStatusEntity.getCurrentStatus());
							break;
					}
				} else {
					String fieldKey;
					List<HashMap<String, Object>> values = wfRequest.getUpdateFieldValues();
					for (Map<String, Object> fieldMap : values) {
						if (!fieldMap.containsKey(Constants.TO_VALUE))
							continue;
						Map<String, Object> toValueMap = (Map<String, Object>) fieldMap.get(Constants.TO_VALUE);
						fieldKey = toValueMap.entrySet().stream().findFirst().get().getKey();
						if (Constants.APPROVE_STATE.equalsIgnoreCase(wfRequest.getAction()) && Constants.NAME.equalsIgnoreCase(fieldKey)) {
							sendEmailnotification = false;
						}
						HashMap<String, Object> toValue = (HashMap<String, Object>) updatedFieldValue.get().get(TO_VALUE_CONST);
						body = MAIL_BODY.replace(STATE_NAME_TAG, wfStatusEntity.getCurrentStatus())
								.replace(FIELD_KEY_TAG, toValue.entrySet().iterator().next().getKey()).replace(TO_VALUE_TAG, (String) toValue.entrySet().iterator().next().getValue());
						subjectLine = MAIL_SUBJECT.replace(STATE_NAME_TAG, wfStatusEntity.getCurrentStatus());

					}
				}
			}
			if (StringUtils.isNotBlank(wfRequest.getComment())) {
				body = body.substring(0, body.length() - 1) + ", due to <b>" + wfRequest.getComment() + "</b>.";
			}
			Map<String, Object> mailNotificationDetails = new HashMap<>();
			mailNotificationDetails.put(Constants.EMAIL_TO, senderInfo.get(Constants.FIRST_NAME));
			mailNotificationDetails.put(Constants.BODY, body);
			mailNotificationDetails.put(Constants.SUBJECT, subjectLine);
			mailNotificationDetails.put(Constants.EMAIL_LIST, Collections.singletonList(senderInfo.get(Constants.EMAIL)));
			if(sendEmailnotification){
				sendNotificationEmail(mailNotificationDetails);
			}
		}
	}

	public void sendEmailNotification(WfRequest wfRequest) {
		WfStatusEntity wfStatusEntity = wfStatusRepo.findByApplicationIdAndWfId(wfRequest.getApplicationId(),
				wfRequest.getWfId());
		WfStatus wfStatus = workflowservice.getWorkflowStates(wfStatusEntity.getRootOrg(), wfStatusEntity.getOrg(),
				wfStatusEntity.getServiceName(), wfStatusEntity.getCurrentStatus());
		try {
			logger.info("Notification workflow status entity, {}", mapper.writeValueAsString(wfStatusEntity));
			logger.info("Notification workflow status model, {}", mapper.writeValueAsString(wfStatus));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		if (!ObjectUtils.isEmpty(wfStatus.getNotificationEnable()) && wfStatus.getNotificationEnable()) {
			logger.info("Enters in the email notification block");
			List<HashMap<String, Object>> updatedFieldValues = wfRequest.getUpdateFieldValues();
			Set<String> emailSet = new HashSet<>();
			// Find the email address from the updated field values
			for (Map<String, Object> fieldValue : updatedFieldValues) {
				if (fieldValue.containsKey("email")) {
					String email = (String) fieldValue.get("email");
					emailSet.add(email);
					break;
				}
			}
			if (!emailSet.isEmpty()) {
				HashMap<String, Object> params = new HashMap<>();
				NotificationRequest request = new NotificationRequest();
				request.setDeliveryType("message");
				request.setIds(new ArrayList<>(emailSet));
				request.setMode("email");
				Template template = new Template();
				template.setId(EMAILTEMPLATE);
				Optional<HashMap<String, Object>> updatedFieldValue = wfRequest.getUpdateFieldValues().stream().findFirst();
				if (updatedFieldValue.isPresent()) {
					HashMap<String, Object> toValue = (HashMap<String, Object>) updatedFieldValue.get().get(TO_VALUE_CONST);
					params.put("body", MAIL_BODY.replace(STATE_NAME_TAG, wfStatusEntity.getCurrentStatus())
							.replace(FIELD_KEY_TAG, toValue.entrySet().iterator().next().getKey()).replace(TO_VALUE_TAG, (String) toValue.entrySet().iterator().next().getValue()));
				}
				if (StringUtils.isNotBlank(wfRequest.getComment())) {
					String body = (String) params.get("body");
					body = body.substring(0, body.length() - 1);
					body = body + ", due to <b>" + wfRequest.getComment() + "</b>.";
					params.put("body", body);
				}
				params.put("orgImageUrl", null);
				template.setParams(params);
				Config config = new Config();
				config.setSubject(MAIL_SUBJECT.replace(STATE_NAME_TAG, wfStatusEntity.getCurrentStatus()));
				config.setSender(configuration.getSenderMail());
				Map<String, Object> req = new HashMap<>();
				request.setTemplate(template);
				request.setConfig(config);
				Map<String, List<NotificationRequest>> notificationMap = new HashMap<>();
				notificationMap.put("notifications", Arrays.asList(request));
				req.put("request", notificationMap);
				sendNotification(req);
			} else {
				logger.warn("Email address not found in the update field values.");
			}
		}
	}

	public void sendNotificationToMdoAdmin(WfRequest wfRequest) throws IOException {
		WfStatusEntity wfStatusEntity = wfStatusRepo.findByApplicationIdAndWfId(wfRequest.getApplicationId(),
				wfRequest.getWfId());
		String fieldKey = null;
		List<HashMap<String, Object>> values =  wfRequest.getUpdateFieldValues();
		for(Map<String, Object> fieldMap : values){
			if(!fieldMap.containsKey(Constants.TO_VALUE))
				continue;
			Map<String, Object> toValueMap= (Map<String, Object>) fieldMap.get(Constants.TO_VALUE);
			fieldKey = toValueMap.entrySet().stream().findFirst().get().getKey();

		}
		if(Constants.APPROVE_STATE.equalsIgnoreCase(wfRequest.getAction()) && Constants.NAME.equalsIgnoreCase(fieldKey)){
			this.sendMailToMDOForOrgChange(wfRequest);
		} else{
			WfStatus wfStatus = workflowservice.getWorkflowStates(wfStatusEntity.getRootOrg(), wfStatusEntity.getOrg(),
					wfStatusEntity.getServiceName(), wfStatusEntity.getCurrentStatus());
			if (!ObjectUtils.isEmpty(wfStatus.getNotificationEnable()) && wfStatus.getNotificationEnable()
					&& !Arrays.asList(Constants.REJECTED, Constants.APPROVED).contains(wfStatus.getState())) {
				logger.info("Enter in the notification block");
				List<String> mdoAdminList = userProfileWfService.getMdoAdminAndPCDetails(wfRequest.getRootOrgId(), Collections.singletonList(Constants.MDO_ADMIN));
				Map<String, Object> params = new HashMap<>();
				NotificationRequest request = new NotificationRequest();
				request.setDeliveryType("message");
				List<String> mdoMailList = mdoAdminList.stream().collect(Collectors.toList());
				if (!CollectionUtils.isEmpty(mdoMailList)) {
					request.setIds(mdoMailList);
					request.setMode("email");
					Template template = new Template();
					template.setId(configuration.getMdoEmailTemplate());
					HashMap<String, Object> usersObj = userProfileWfService.getUsersResult(Collections.singleton(wfRequest.getUserId()));
					Map<String, Object> recipientInfo = (Map<String, Object>) usersObj.get(wfStatusEntity.getUserId());
					params.put(Constants.USER_NAME, recipientInfo.get(Constants.FIRST_NAME));
					Optional<HashMap<String, Object>> updatedFieldValue = wfRequest.getUpdateFieldValues().stream().findFirst();
					if (updatedFieldValue.isPresent()) {
						HashMap<String, Object> toValue = (HashMap<String, Object>) updatedFieldValue.get().get(TO_VALUE_CONST);
						List<String> fieldNames = toValue.keySet().stream().collect(Collectors.toList());
						String approvalUrl = configuration.getDomainHost() + configuration.getMdoBaseUrl().replace("{id}", wfRequest.getApplicationId());
						params.put(Constants.LINK, approvalUrl);
						params.put(Constants.FIELDS, fieldNames);
						params.put(Constants.SUPPORT_EMAIL, configuration.getSenderMail());
					}
					String constructedEmailTemplate = constructEmailTemplate(configuration.getMdoEmailTemplate(), params);
					if (StringUtils.isNotEmpty(constructedEmailTemplate)) {
						template.setData(constructedEmailTemplate);
					}
					template.setParams(params);
					Config config = new Config();
					config.setSubject(MDO_MAIL_SUBJECT);
					config.setSender(configuration.getSenderMail());
					Map<String, Object> req = new HashMap<>();
					request.setTemplate(template);
					request.setConfig(config);
					Map<String, List<NotificationRequest>> notificationMap = new HashMap<>();
					notificationMap.put("notifications", Arrays.asList(request));
					req.put("request", notificationMap);
					sendNotification(req);
				}
			}
		}
	}

	public void sendNotificationToMdoAdminAndPC(WfRequest wfRequest) {
		WfStatusEntity wfStatusEntity = wfStatusRepo.findByApplicationIdAndWfId(wfRequest.getApplicationId(),
				wfRequest.getWfId());
		WfStatus wfStatus = workflowservice.getWorkflowStates(wfStatusEntity.getRootOrg(), wfStatusEntity.getOrg(),
				wfStatusEntity.getServiceName(), wfStatusEntity.getCurrentStatus());

		HashMap<String, Object> usersObj = userProfileWfService.getUsersResult(Collections.singleton(wfRequest.getUserId()));
		Map<String, Object> recipientInfo = (Map<String, Object>) usersObj.get(wfStatusEntity.getUserId());
		String userName = (String) recipientInfo.get(Constants.FIRST_NAME);
		String userRootOrgId = (String) recipientInfo.get(Constants.ROOT_ORG_ID);

		String rootOrgId = contentReadService.getRootOrgId(wfRequest.getCourseId());

		List<String> pcEmailList = new ArrayList<>();
		List<String> mdoEmailList = new ArrayList<>();

		Map<String, Object> mailNotificationDetails = new HashMap<>();
		String subjectLine = "";
		String body = "";
		String enrolmentStateSubject = "Enrolment #state";
		String approvalRequestSubject = "Enrollment Approval Request";
		String requestForwardedSubject = "Enrollment Request Forwarded to #role";
		String date = wfRequest.getBatchStartDate().toString();
		String nominationRequestMailBody = configuration.getNominationRequestMailBody().replace(USERNAMAE_TAG, userName)
				.replace(BATCH_NAME_TAG, wfRequest.getBatchName())
				.replace(BLENDED_PROGRAME_NAME_TAG, wfRequest.getCourseName());
		nominationRequestMailBody = nominationRequestMailBody.replace(BATCH_START_DATE_TAG, wfRequest.getBatchStartDate().toString());
		String approvalRequestMailBody = configuration.getApprovalRequetMailBody().replace(USERNAMAE_TAG, userName)
				.replace(BATCH_NAME_TAG, wfRequest.getBatchName())
				.replace(BLENDED_PROGRAME_NAME_TAG, wfRequest.getCourseName());
		approvalRequestMailBody = approvalRequestMailBody.replace(BATCH_START_DATE_TAG, date);
		String requestForwardedMailBody = configuration.getRequestForwardedMailBody().replace(USERNAMAE_TAG, userName)
				.replace(BATCH_NAME_TAG, wfRequest.getBatchName())
				.replace(BLENDED_PROGRAME_NAME_TAG, wfRequest.getCourseName())
				.replace(BATCH_START_DATE_TAG, wfRequest.getBatchStartDate().toString());
		mailNotificationDetails.put("subject", approvalRequestSubject);
		mailNotificationDetails.put("body", approvalRequestMailBody);

		if (!ObjectUtils.isEmpty(wfStatus.getNotificationEnable()) && wfStatus.getNotificationEnable()
				&& !Arrays.asList(Constants.REJECTED, Constants.APPROVED).contains(wfStatus.getState())) {
			logger.info("Enter in the sendNotificationToMdoAdminAndPC block");
			while (true) {
				if (Constants.INITIATE.equalsIgnoreCase(wfRequest.getState()) && Constants.SEND_FOR_PC_APPROVAL.equalsIgnoreCase(wfStatusEntity.getCurrentStatus())) {
					pcEmailList = userProfileWfService.getMdoAdminAndPCDetails(rootOrgId, Collections.singletonList(Constants.PROGRAM_COORDINATOR));
					mailNotificationDetails.put("emailList", pcEmailList);
					mailNotificationDetails.put("emailTo", Constants.TO_PROGRAMME_COORDINATOR);
					break;
				}
				if (Constants.INITIATE.equalsIgnoreCase(wfRequest.getState()) && Constants.SEND_FOR_MDO_APPROVAL.equalsIgnoreCase(wfStatusEntity.getCurrentStatus())) {
					mdoEmailList = userProfileWfService.getMdoAdminAndPCDetails(userRootOrgId, Collections.singletonList(Constants.MDO_ADMIN));
					mailNotificationDetails.put("emailList", mdoEmailList);
					mailNotificationDetails.put("emailTo", Constants.TO_MDO_ADMIN);
					break;
				}
				pcEmailList = userProfileWfService.getMdoAdminAndPCDetails(rootOrgId, Collections.singletonList(Constants.PROGRAM_COORDINATOR));
				mdoEmailList = userProfileWfService.getMdoAdminAndPCDetails(userRootOrgId, Collections.singletonList(Constants.MDO_ADMIN));
				if (Constants.SEND_FOR_MDO_APPROVAL.equalsIgnoreCase(wfRequest.getState()) && Constants.SEND_FOR_PC_APPROVAL.equalsIgnoreCase(wfStatusEntity.getCurrentStatus())) {
					mailNotificationDetails.put("emailList", pcEmailList);
					mailNotificationDetails.put("emailTo", Constants.TO_PROGRAMME_COORDINATOR);
					sendNotificationEmail(mailNotificationDetails);

					subjectLine = requestForwardedSubject.replace(ROLE_TAG, Constants.PROGRAM_COORDINATOR.replace("_", " "));
					body = requestForwardedMailBody.replace(ROLE_TAG, Constants.PROGRAM_COORDINATOR.replace("_", " "));
					mailNotificationDetails.put("subject", subjectLine);
					mailNotificationDetails.put("body", body);
					mailNotificationDetails.put("emailList", mdoEmailList);
					mailNotificationDetails.put("emailTo", Constants.TO_MDO_ADMIN);
					break;
				}
				if (Constants.SEND_FOR_PC_APPROVAL.equalsIgnoreCase(wfRequest.getState()) && Constants.SEND_FOR_MDO_APPROVAL.equalsIgnoreCase(wfStatusEntity.getCurrentStatus())) {
					mailNotificationDetails.put("emailList", mdoEmailList);
					mailNotificationDetails.put("emailTo", Constants.TO_MDO_ADMIN);
					sendNotificationEmail(mailNotificationDetails);

					subjectLine = requestForwardedSubject.replace(ROLE_TAG, Constants.MDO_ADMIN.split("_")[0]);
					body = requestForwardedMailBody.replace(ROLE_TAG, Constants.MDO_ADMIN.split("_")[0]);
					mailNotificationDetails.put("subject", subjectLine);
					mailNotificationDetails.put("body", body);
					mailNotificationDetails.put("emailList", pcEmailList);
					mailNotificationDetails.put("emailTo", Constants.TO_PROGRAMME_COORDINATOR);
					break;
				}
				break;
			}
			sendNotificationEmail(mailNotificationDetails);
		} else {
			if (Constants.SEND_FOR_PC_APPROVAL.equalsIgnoreCase(wfRequest.getState()) && (null != wfStatusEntity.getAdditionalProperties() && wfStatusEntity.getAdditionalProperties().contains("isNominatedByMdo:true"))) {
				mdoEmailList = userProfileWfService.getMdoAdminAndPCDetails(userRootOrgId, Collections.singletonList(Constants.MDO_ADMIN));
				subjectLine = enrolmentStateSubject.replace("#state", wfStatusEntity.getCurrentStatus());
				body = nominationRequestMailBody.replace("#state", wfStatusEntity.getCurrentStatus());
				if (StringUtils.isNotBlank(wfRequest.getComment())) {
					body = body.substring(0, body.length() - 1) + ", due to <b>" + wfRequest.getComment() + "</b>.";
				}
				mailNotificationDetails.put("emailList", mdoEmailList);
				mailNotificationDetails.put("subject", subjectLine);
				mailNotificationDetails.put("body", body);
				mailNotificationDetails.put("emailTo", Constants.TO_MDO_ADMIN);
				sendNotificationEmail(mailNotificationDetails);
				logger.info("current role to send notification ");
			}
		}
	}

	private String constructEmailTemplate(String templateName, Map<String, Object> params) {
		String replacedHTML = new String();
		try {
			Map<String, Object> propertyMap = new HashMap<>();
			propertyMap.put(Constants.NAME, templateName);
			List<Map<String, Object>> templateMap = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_EMAIL_TEMPLATE, propertyMap, Collections.singletonList(Constants.TEMPLATE));
			String htmlTemplate = templateMap.stream()
					.findFirst()
					.map(template -> (String) template.get(Constants.TEMPLATE))
					.orElse(null);
			VelocityEngine velocityEngine = new VelocityEngine();
			velocityEngine.init();
			VelocityContext context = new VelocityContext();
			for (Map.Entry<String, Object> entry : params.entrySet()) {
				context.put(entry.getKey(), entry.getValue());
			}
			StringWriter writer = new StringWriter();
			velocityEngine.evaluate(context, writer, "HTMLTemplate", htmlTemplate);
			replacedHTML = writer.toString();
		} catch (Exception e) {
			logger.error("Unable to create template "+e);
		}
		return replacedHTML;
	}

		/**
         * Post to the Notification service
         * @param request
         */
	public void sendNotification(Map<String, Object> request) {
		StringBuilder builder = new StringBuilder();
		builder.append(configuration.getNotifyServiceHost()).append(configuration.getNotifyServicePath());
		try {
			Object response = requestService.fetchResultUsingPost(builder, request, Map.class, null);
			logger.info("Notification sent successfully, response is: " + response);
		} catch (Exception e) {
			logger.error("Exception while posting the data in notification service: ", e);
		}
	}

	private void sendNotificationEmail(Map<String, Object> mailNotificationDetails){
		logger.info("Mail Notification Details: " + mailNotificationDetails);
		Map<String, Object> params = new HashMap<>();
		NotificationRequest request = new NotificationRequest();
		List<String> emailList = (List<String>) mailNotificationDetails.get("emailList");
		request.setDeliveryType("message");
		if (!CollectionUtils.isEmpty(emailList)) {
			request.setIds(emailList);
			request.setMode("email");
			Template template = new Template();
			template.setId(configuration.getNotificationEmailTemplate());
			params.put(Constants.NAME, mailNotificationDetails.get("emailTo"));
			params.put("body", mailNotificationDetails.get("body"));
			params.put(Constants.ORG_NAME, Constants.KARMYOGI_BHARAT);
			params.put(Constants.FROM_EMAIL, configuration.getSenderMail());
			String constructedEmailTemplate = constructEmailTemplate(configuration.getNotificationEmailTemplate(), params);
			if (StringUtils.isNotEmpty(constructedEmailTemplate)) {
				template.setData(constructedEmailTemplate);
			}
			template.setParams(params);
			Config config = new Config();
			config.setSubject((String) mailNotificationDetails.get("subject"));
			config.setSender(configuration.getSenderMail());
			Map<String, Object> req = new HashMap<>();
			request.setTemplate(template);
			request.setConfig(config);
			Map<String, List<NotificationRequest>> notificationMap = new HashMap<>();
			notificationMap.put("notifications", Arrays.asList(request));
			req.put("request", notificationMap);
			sendNotification(req);
		}
	}

	public void sendMailToMDOForOrgChange(WfRequest wfRequest){
		List<String> mdoAdminList = userProfileWfService.getMdoAdminAndPCDetails(wfRequest.getRootOrgId(), Collections.singletonList(Constants.MDO_ADMIN));
		Map<String, Object> params = new HashMap<>();
		NotificationRequest request = new NotificationRequest();
		request.setDeliveryType("message");
		List<String> mdoMailList = mdoAdminList.stream().collect(Collectors.toList());
		if (!CollectionUtils.isEmpty(mdoMailList)) {
			request.setIds(mdoMailList);
			request.setMode("email");
			Template template = new Template();
			template.setId(configuration.getOrgChangeNotificationTemplate());
			HashMap<String, Object> usersObj = userProfileWfService.getUsersResult(Collections.singleton(wfRequest.getUserId()));
			Map<String, Object> recipientInfo = (Map<String, Object>) usersObj.get(wfRequest.getUserId());
			String username = (String) recipientInfo.get(Constants.FIRST_NAME);
			params.put(Constants.USER_NAME, username);
			params.put(Constants.SENDER , Constants.KARMYOGI_BHARAT);
			params.put(Constants.SUPPORT_EMAIL, configuration.getSenderMail());
			String constructedEmailTemplate = constructEmailTemplate(configuration.getOrgChangeNotificationTemplate(), params);
			if (StringUtils.isNotEmpty(constructedEmailTemplate)) {
				template.setData(constructedEmailTemplate);
			}
			template.setParams(params);
			Config config = new Config();
			config.setSubject(Constants.TRANSFER_SUBJECT_LINE.replace( USERNAMAE_TAG, username));
			config.setSender(configuration.getSenderMail());
			Map<String, Object> req = new HashMap<>();
			request.setTemplate(template);
			request.setConfig(config);
			Map<String, List<NotificationRequest>> notificationMap = new HashMap<>();
			notificationMap.put("notifications", Arrays.asList(request));
			req.put("request", notificationMap);
			sendNotification(req);
		}
	}

}
