package org.sunbird.workflow.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.workflow.config.Configuration;
import org.sunbird.workflow.config.Constants;
import org.sunbird.workflow.exception.ApplicationException;
import org.sunbird.workflow.exception.BadRequestException;
import org.sunbird.workflow.exception.InvalidDataInputException;
import org.sunbird.workflow.models.Response;
import org.sunbird.workflow.models.SBApiResponse;
import org.sunbird.workflow.models.SearchCriteria;
import org.sunbird.workflow.models.SearchCriteriaV2;
import org.sunbird.workflow.models.WfAction;
import org.sunbird.workflow.models.WfRequest;
import org.sunbird.workflow.models.WfStatus;
import org.sunbird.workflow.models.WorkFlowModel;
import org.sunbird.workflow.postgres.entity.WfAuditEntity;
import org.sunbird.workflow.postgres.entity.WfStatusCountDTO;
import org.sunbird.workflow.postgres.entity.WfStatusEntity;
import org.sunbird.workflow.postgres.repo.WfAuditRepo;
import org.sunbird.workflow.postgres.repo.WfStatusRepo;
import org.sunbird.workflow.producer.Producer;
import org.sunbird.workflow.service.StorageService;
import org.sunbird.workflow.service.UserProfileWfService;
import org.sunbird.workflow.service.Workflowservice;
import org.sunbird.workflow.utils.AccessTokenValidator;
import org.sunbird.workflow.utils.CassandraOperation;
import org.sunbird.workflow.utils.LRUCache;
import org.sunbird.workflow.utils.ProjectUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class WorkflowServiceImpl implements Workflowservice {

	@Autowired
	private WfStatusRepo wfStatusRepo;

	@Autowired
	private WfAuditRepo wfAuditRepo;

	@Autowired
	private ObjectMapper mapper;

	@Autowired
	private Configuration configuration;

	@Autowired
	private UserProfileWfService userProfileWfService;

	@Autowired
	private RequestServiceImpl requestServiceImpl;

	@Autowired
	private Producer producer;

	@Autowired
	private CassandraOperation cassandraOperation;

	@Autowired
	AccessTokenValidator accessTokenValidator;

	Logger log = LogManager.getLogger(WorkflowServiceImpl.class);

	@Autowired
	LRUCache<String, List<WfStatusCountDTO>> localCache ;

	@Autowired
	StorageService storageService;

	@Autowired
	Producer kafkaProducer;
	/**
	 * Change the status of workflow application
	 *
	 * @param rootOrg
	 * @param org
	 * @param wfRequest
	 * @return
	 */

	public Response workflowTransition(String rootOrg, String org, WfRequest wfRequest,String userId,String role) {
		HashMap<String, String> changeStatusResponse;
		List<String> wfIds = new ArrayList<>();
		String changedStatus = null;
		if (configuration.getMultipleWfCreationEnable() && !CollectionUtils.isEmpty(wfRequest.getUpdateFieldValues())) {
			String wfId = wfRequest.getWfId();
			for (HashMap<String, Object> updatedField : wfRequest.getUpdateFieldValues()) {
				wfRequest.setUpdateFieldValues(new ArrayList<>(Arrays.asList(updatedField)));
				wfRequest.setWfId(wfId);
				changeStatusResponse = changeStatus(rootOrg, org, wfRequest,userId,role);
				wfIds.add(changeStatusResponse.get(Constants.WF_ID_CONSTANT));
				changedStatus = changeStatusResponse.get(Constants.STATUS);
			}
		} else {
			changeStatusResponse = changeStatus(rootOrg, org, wfRequest,userId,role);
			wfIds.add(changeStatusResponse.get(Constants.WF_ID_CONSTANT));
			changedStatus = changeStatusResponse.get(Constants.STATUS);
		}
		Response response = new Response();
		HashMap<String, Object> data = new HashMap<>();
		data.put(Constants.STATUS, changedStatus);
		data.put(Constants.WF_IDS_CONSTANT, wfIds);
		response.put(Constants.MESSAGE, Constants.STATUS_CHANGE_MESSAGE + changedStatus);
		response.put(Constants.DATA, data);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	public Response workflowTransition(String rootOrg, String org, WfRequest wfRequest) {
		return workflowTransition( rootOrg,  org,  wfRequest,null,null);
	}

		private HashMap<String, String> changeStatus(String rootOrg, String org, WfRequest wfRequest,String userId,String role) {
		String wfId = wfRequest.getWfId();
		String nextState = null;
		HashMap<String, String> data = new HashMap<>();
		try {
			validateWfRequest(wfRequest);
			WfStatusEntity applicationStatus = wfStatusRepo.findByRootOrgAndOrgAndApplicationIdAndWfId(rootOrg, org,
					wfRequest.getApplicationId(), wfRequest.getWfId());
			String serviceName=wfRequest.getServiceName();
			if (Constants.BLENDED_PROGRAM_SERVICE_NAME.equalsIgnoreCase(wfRequest.getServiceName()) && !StringUtils.isEmpty(applicationStatus.getServiceName())) {
				serviceName = applicationStatus.getServiceName();
			}
			WorkFlowModel workFlowModel = getWorkFlowConfig(serviceName);
			WfStatus wfStatus = getWfStatus(wfRequest.getState(), workFlowModel);
			validateUserAndWfStatus(wfRequest, wfStatus, applicationStatus);
			WfAction wfAction = getWfAction(wfRequest.getAction(), wfStatus);

			// actor has proper role to take the workflow action

			nextState = wfAction.getNextState();
			if (ObjectUtils.isEmpty(applicationStatus)) {
				applicationStatus = new WfStatusEntity();
				wfId = UUID.randomUUID().toString();
				applicationStatus.setWfId(wfId);
				applicationStatus.setServiceName(serviceName);
				applicationStatus.setUserId(wfRequest.getUserId());
				applicationStatus.setApplicationId(wfRequest.getApplicationId());
				applicationStatus.setRootOrg(rootOrg);
				applicationStatus.setOrg(org);
				applicationStatus.setCreatedOn(new Date());
				wfRequest.setWfId(wfId);
			}

			WfStatus wfStatusCheckForNextState = getWfStatus(nextState, workFlowModel);

			applicationStatus.setLastUpdatedOn(new Date());
			applicationStatus.setCurrentStatus(nextState);
			applicationStatus.setActorUUID(wfRequest.getActorUserId());
			applicationStatus.setUpdateFieldValues(mapper.writeValueAsString(wfRequest.getUpdateFieldValues()));
			applicationStatus.setInWorkflow(!wfStatusCheckForNextState.getIsLastState());
			applicationStatus.setDeptName(wfRequest.getDeptName());
			applicationStatus.setComment(wfRequest.getComment());
			applicationStatus.setServiceName(serviceName);
			addModificationEntry(applicationStatus,userId,wfRequest.getAction(),role);
			wfStatusRepo.save(applicationStatus);
			producer.push(configuration.getWorkFlowNotificationTopic(), wfRequest);
			producer.push(configuration.getWorkflowApplicationTopic(), wfRequest);

		} catch (IOException e) {
			throw new ApplicationException(Constants.WORKFLOW_PARSING_ERROR_MESSAGE, e);
		}
		data.put(Constants.STATUS, nextState);
		data.put(Constants.WF_ID_CONSTANT, wfId);
		return data;
	}

	private void addModificationEntry(WfStatusEntity applicationStatus,String userId,String action,String role) throws IOException {
		if(!StringUtils.isEmpty(userId) &&
				Arrays.asList(configuration.getModificationRecordAllowActions().split(Constants.COMMA)).contains(action)) {
			List<Map<String, Object>> historyMap = null;
			String history = applicationStatus.getModificationHistory();
			if (!StringUtils.isEmpty(history))
				historyMap = mapper.readValue(history, List.class);
			else
				historyMap = new ArrayList<>();
			Map<String, Object> newModification = new HashMap<>();
			newModification.put("modifiedDate", new Date());
			newModification.put("modifiedBy", userId);
			newModification.put("action", action);
			newModification.put("role", role);
			historyMap.add(newModification);
			applicationStatus.setModificationHistory(mapper.writeValueAsString(historyMap));
		}

	}

	/**
	 * @param rootOrg        root Org
	 * @param org            org
	 * @param searchCriteria Search Criteria
	 * @return Response of Application Search
	 */
	public Response appsPCSearchV2(String rootOrg, String org, SearchCriteriaV2 searchCriteria) {
		return getResponse(rootOrg,  appsSearchV2(rootOrg,searchCriteria));
	}

	public Response applicationsSearch(String rootOrg, String org, SearchCriteria searchCriteria, boolean... isSearchEnabled) {
		Response response = null;
		Response wfApplicationSearchResponse = null;
		switch (searchCriteria.getServiceName()) {
			// Below statement will work as OR condition.
			case Constants.PROFILE_SERVICE_NAME:
			case Constants.USER_PROFILE_FLAG_SERVICE:
			case Constants.POSITION_SERVICE_NAME:
			case Constants.ORGANISATION_SERVICE_NAME:
			case Constants.DOMAIN_SERVICE_NAME:
				wfApplicationSearchResponse = applicationSearchOnApplicationIdGroup(rootOrg, searchCriteria, isSearchEnabled);
				response = getResponse(rootOrg, wfApplicationSearchResponse);
				response.put(Constants.COUNT,wfApplicationSearchResponse.get(Constants.COUNT));
				break;
			case Constants.BLENDED_PROGRAM_SERVICE_NAME: {
				if (searchCriteria.getApplicationStatus() != null) {
					wfApplicationSearchResponse = applicationSearchOnApplicationIdGroup(rootOrg, searchCriteria, isSearchEnabled);
				} else {
					wfApplicationSearchResponse = applicationUserSearchOnApplicationIdGroup(searchCriteria);
				}
				response = getResponse(rootOrg, wfApplicationSearchResponse);
			}
			break;
			default:
				response = applicationSearchOnApplicationIdGroup(rootOrg, searchCriteria);
				break;
		}
		return response;
	}


	private Response getResponse(String rootOrg, Response wfApplicationSearchResponse) {
		Response response;
		List<Map<String, Object>> userProfiles = userProfileWfService.enrichUserData(
				(Map<String, List<WfStatusEntity>>) wfApplicationSearchResponse.get(Constants.DATA), rootOrg);
		response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, userProfiles);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	/**
	 * @param rootOrg Root Org
	 * @param wfId    Wf Id
	 * @return Response of workflow process
	 */
	@Override
	public Response getWorkflowProcess(String rootOrg, String wfId) {
		Response response = new Response();
		WfStatusEntity wfStatusEntity = wfStatusRepo.findByRootOrgAndWfId(rootOrg, wfId);
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, wfStatusEntity);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	/**
	 * Validate application against workflow state
	 *
	 * @param wfRequest
	 * @param wfStatus
	 * @param applicationStatus
	 */
	private void validateUserAndWfStatus(WfRequest wfRequest, WfStatus wfStatus, WfStatusEntity applicationStatus) {

		if (StringUtils.isEmpty(wfRequest.getWfId()) && !wfStatus.getStartState()) {
			throw new ApplicationException(Constants.WORKFLOW_ID_ERROR_MESSAGE);
		}
		if ((!ObjectUtils.isEmpty(applicationStatus))
				&& (!wfRequest.getState().equalsIgnoreCase(applicationStatus.getCurrentStatus()))) {
			throw new BadRequestException("Application is in " + applicationStatus.getCurrentStatus()
					+ " State but trying to be move in " + wfRequest.getState() + " state!");
		}
	}

	/**
	 * @param nextActions
	 * @return String of next actions
	 */
	private String getNextApplicableActions(List<WfAction> nextActions) {
		String applicableAction = null;
		List<HashMap<String, Object>> nextActionArray = new ArrayList<>();
		try {
			if (CollectionUtils.isEmpty(nextActions)) {
				applicableAction = mapper.writeValueAsString(nextActions);
			} else {
				HashMap<String, Object> actionMap = null;
				for (WfAction action : nextActions) {
					actionMap = new HashMap<>();
					actionMap.put(Constants.ACTION_CONSTANT, action.getAction());
					actionMap.put(Constants.ROLES_CONSTANT, action.getRoles());
					nextActionArray.add(actionMap);
				}
				applicableAction = mapper.writeValueAsString(nextActionArray);
			}
		} catch (IOException e) {
			throw new ApplicationException(Constants.JSON_PARSING_ERROR, e);
		}
		return applicableAction;
	}

	/**
	 * Validate roles of actor with action role
	 *
	 * @param actorRoles
	 * @param actionRoles
	 */
	private void validateRoles(List<String> actorRoles, List<String> actionRoles) {
		if ((CollectionUtils.isEmpty(actionRoles))
				|| (CollectionUtils.isEmpty(actorRoles) && CollectionUtils.isEmpty(actionRoles)))
			return;
		if (CollectionUtils.isEmpty(actorRoles)) {
			throw new ApplicationException(Constants.WORKFLOW_ROLE_ERROR);
		}
		boolean roleFound = actorRoles.stream().anyMatch(actionRoles::contains);
		if (!roleFound) {
			throw new BadRequestException(Constants.WORKFLOW_ROLE_CHECK_ERROR);
		}
	}

	/**
	 * Get Workflow Action based on given action
	 *
	 * @param action
	 * @param wfStatus
	 * @return Work flow Action
	 */
	private WfAction getWfAction(String action, WfStatus wfStatus) {
		WfAction wfAction = null;
		if (ObjectUtils.isEmpty(wfStatus.getActions())) {
			throw new BadRequestException(Constants.WORKFLOW_ACTION_ERROR);
		}
		for (WfAction filterAction : wfStatus.getActions()) {
			if (action.equals(filterAction.getAction())) {
				wfAction = filterAction;
			}
		}
		if (ObjectUtils.isEmpty(wfAction)) {
			throw new BadRequestException(Constants.WORKFLOW_ACTION_ERROR);
		}
		return wfAction;
	}

	/**
	 * Get the workflow State based on given state
	 *
	 * @param state
	 * @param workFlowModel
	 * @return Workflow State
	 */
	private WfStatus getWfStatus(String state, WorkFlowModel workFlowModel) {
		WfStatus wfStatus = null;
		for (WfStatus status : workFlowModel.getWfstates()) {
			if (status.getState().equals(state)) {
				wfStatus = status;
				break;
			}
		}
		if (ObjectUtils.isEmpty(wfStatus)) {
			throw new BadRequestException(Constants.WORKFLOW_STATE_CHECK_ERROR);
		}
		return wfStatus;
	}

	/**
	 * Validate the workflow request
	 *
	 * @param wfRequest
	 */
	private void validateWfRequest(WfRequest wfRequest) {

		if (StringUtils.isEmpty(wfRequest.getState())) {
			throw new InvalidDataInputException(Constants.STATE_VALIDATION_ERROR);
		}

		if (StringUtils.isEmpty(wfRequest.getApplicationId())) {
			throw new InvalidDataInputException(Constants.APPLICATION_ID_VALIDATION_ERROR);
		}

		if (StringUtils.isEmpty(wfRequest.getActorUserId())) {
			throw new InvalidDataInputException(Constants.ACTOR_UUID_VALIDATION_ERROR);
		}

		if (StringUtils.isEmpty(wfRequest.getUserId())) {
			throw new InvalidDataInputException(Constants.USER_UUID_VALIDATION_ERROR);
		}

		if (StringUtils.isEmpty(wfRequest.getAction())) {
			throw new InvalidDataInputException(Constants.ACTION_VALIDATION_ERROR);
		}

		if (!Constants.WITHDRAW.equalsIgnoreCase(wfRequest.getAction()) && CollectionUtils.isEmpty(wfRequest.getUpdateFieldValues())) {
			throw new InvalidDataInputException(Constants.FIELD_VALUE_VALIDATION_ERROR);
		}

		if (StringUtils.isEmpty(wfRequest.getServiceName())) {
			throw new InvalidDataInputException(Constants.WORKFLOW_SERVICENAME_VALIDATION_ERROR);
		}

	}

	/**
	 * Get the application based on application id
	 *
	 * @param rootOrg
	 * @param org
	 * @param wfId
	 * @param applicationId
	 * @return Wf Application based on Id
	 */
	public Response getWfApplication(String rootOrg, String org, String wfId, String applicationId) {
		WfStatusEntity applicationStatus = wfStatusRepo.findByRootOrgAndOrgAndApplicationIdAndWfId(rootOrg, org,
				applicationId, wfId);
		List<WfStatusEntity> applicationList = applicationStatus == null ? new ArrayList<>()
				: new ArrayList<>(Arrays.asList(applicationStatus));
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, applicationList);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	/**
	 * Get workflow applications based on status
	 *
	 * @param rootOrg
	 * @param org
	 * @param criteria
	 * @return workflow applications
	 */
	public Response wfApplicationSearch(String rootOrg, String org, SearchCriteria criteria) {
		Pageable pageable = getPageReqForApplicationSearch(criteria);
		Page<WfStatusEntity> statePage = wfStatusRepo.findByRootOrgAndOrgAndServiceNameAndCurrentStatus(rootOrg, org,
				criteria.getServiceName(), criteria.getApplicationStatus(), pageable);
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, statePage.getContent());
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	/**
	 * @param rootOrg
	 * @param wfId
	 * @param applicationId
	 * @return
	 */
	public Response getApplicationHistoryOnWfId(String rootOrg, String wfId, String applicationId) {
		Response response = new Response();
		List<WfAuditEntity> wfAuditEntityList = wfAuditRepo
				.findByRootOrgAndApplicationIdAndWfIdOrderByCreatedOnDesc(rootOrg, applicationId, wfId);
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, wfAuditEntityList);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	/**
	 * @param rootOrg
	 * @param org
	 * @param serviceName
	 * @param state
	 * @return
	 */
	public Response getNextActionForState(String rootOrg, String org, String serviceName, String state) {
		Response response = new Response();
		try {
			WorkFlowModel workFlowModel = getWorkFlowConfig(serviceName);
			WfStatus wfStatus = getWfStatus(state, workFlowModel);
			List<HashMap<String, Object>> nextActionArray = new ArrayList<>();
			HashMap<String, Object> actionMap = null;
			if (!CollectionUtils.isEmpty(wfStatus.getActions())) {
				for (WfAction action : wfStatus.getActions()) {
					actionMap = new HashMap<>();
					actionMap.put(Constants.ACTION_CONSTANT, action.getAction());
					actionMap.put(Constants.ROLES_CONSTANT, action.getRoles());
					actionMap.put(Constants.IS_WORKFLOW_TERMINATED, !wfStatus.getIsLastState());
					nextActionArray.add(actionMap);
				}
			}
			response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
			response.put(Constants.DATA, nextActionArray);
			response.put(Constants.STATUS, HttpStatus.OK);
		} catch (Exception e) {
			throw new ApplicationException(Constants.JSON_PARSING_ERROR, e);
		}
		return response;
	}

	public WfStatus getWorkflowStates(String rootOrg, String org, String serviceName, String state) {
		WfStatus wfStatus = null;
		try {
			WorkFlowModel workFlowModel = getWorkFlowConfig(serviceName);
			wfStatus = getWfStatus(state, workFlowModel);
		} catch (Exception e) {
			throw new ApplicationException(Constants.JSON_PARSING_ERROR, e);
		}
		return wfStatus;
	}

	/**
	 * @param rootOrg
	 * @param applicationId
	 * @return
	 */
	public Response getApplicationWfHistory(String rootOrg, String applicationId) {
		Response response = new Response();
		List<WfAuditEntity> wfAuditEntityList = wfAuditRepo.findByRootOrgAndApplicationIdOrderByCreatedOnDesc(rootOrg,
				applicationId);
		HashMap<String, List<WfAuditEntity>> history = new HashMap<>();

		for (WfAuditEntity audit : wfAuditEntityList) {
			if (StringUtils.isEmpty(history.get(audit.getWfId()))) {
				List<WfAuditEntity> wfAuditEntities = new ArrayList<>(Arrays.asList(audit));
				history.put(audit.getWfId(), wfAuditEntities);
			} else {
				history.get(audit.getWfId()).add(audit);
			}
		}
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, history);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	/**
	 * @param userId userId
	 * @return list of roles for user
	 */
	private List<String> getUserRoles(String userId) {
		List<String> roleList;
		StringBuilder builder = new StringBuilder();
		String endPoint = configuration.getUserRoleSearchEndpoint().replace("{user_id}", userId);
		builder.append(configuration.getLexCoreServiceHost()).append(endPoint);
		Map<String, Object> response = (Map<String, Object>) requestServiceImpl.fetchResultUsingGet(builder);
		List<String> defaultRoles = new ArrayList<>();
		List<String> userRoles = new ArrayList<>();
		if (!ObjectUtils.isEmpty(response.get("default_roles"))) {
			defaultRoles = (List<String>) response.get("default_roles");
		}
		if (!ObjectUtils.isEmpty(response.get("user_roles"))) {
			userRoles = (List<String>) response.get("user_roles");
		}
		roleList = Stream.concat(userRoles.stream(), defaultRoles.stream()).distinct().collect(Collectors.toList());
		return roleList;
	}

	public Response applicationSerachOnApplicationIdGrup(String rootOrg, SearchCriteria criteria) {
		Pageable pageable = getPageReqForApplicationSearch(criteria);

		List<String> applicationIds = criteria.getApplicationIds();
		if (CollectionUtils.isEmpty(applicationIds)) {
			applicationIds = wfStatusRepo.getListOfDistinctApplication(rootOrg, criteria.getServiceName(),
					criteria.getApplicationStatus(), pageable);
		}
		List<WfStatusEntity> wfStatusEntities = wfStatusRepo.findByServiceNameAndCurrentStatusAndApplicationIdIn(
				criteria.getServiceName(), criteria.getApplicationStatus(), applicationIds);
		Map<String, List<WfStatusEntity>> infos = wfStatusEntities.stream()
				.collect(Collectors.groupingBy(WfStatusEntity::getApplicationId));
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, infos);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	private Pageable getPageReqForApplicationSearch(SearchCriteria criteria) {
		Pageable pageable;
		if (criteria.isEmpty()) {
			throw new BadRequestException(Constants.SEARCH_CRITERIA_VALIDATION);
		}

		Integer limit = criteria.getLimit();
		Integer offset = criteria.getOffset();
		if (limit == null) {
			limit = configuration.getDefaultLimit();
		} else {
			if (limit > configuration.getMaxLimit()) {
				limit = configuration.getMaxLimit();
			}
		}
		if (offset == null) {
			offset = configuration.getDefaultOffset();
			limit = configuration.getMaxLimit();
		}
		pageable = PageRequest.of(offset, limit);
		return pageable;
	}

	@Override
	public Response updateUserProfileWF(String rootOrg, String org, WfRequest wfRequest) {
		String wfId;
		WfStatusEntity applicationStatus = new WfStatusEntity();
		wfId = UUID.randomUUID().toString();
		applicationStatus.setWfId(wfId);
		applicationStatus.setServiceName(wfRequest.getServiceName());
		applicationStatus.setUserId(wfRequest.getUserId());
		applicationStatus.setApplicationId(wfRequest.getApplicationId());
		applicationStatus.setRootOrg(rootOrg);
		applicationStatus.setOrg(org);
		applicationStatus.setCreatedOn(new Date());
		applicationStatus.setLastUpdatedOn(new Date());
		applicationStatus.setCurrentStatus(Constants.APPROVED_STATE);
		applicationStatus.setActorUUID(wfRequest.getActorUserId());
		applicationStatus
				.setDeptName(getDepartmentDetails(wfRequest.getUpdateFieldValues().stream().findFirst().get()));
		wfRequest.setWfId(wfId);
		wfRequest.setAction(Constants.APPROVE_STATE);
		wfRequest.setState(Constants.APPROVED_STATE);
		try {
			applicationStatus.setUpdateFieldValues(mapper.writeValueAsString(wfRequest.getUpdateFieldValues()));
		} catch (JsonProcessingException e) {
			throw new ApplicationException(Constants.WORKFLOW_PARSING_ERROR_MESSAGE, e);
		}
		applicationStatus.setInWorkflow(false);
		wfStatusRepo.save(applicationStatus);
		producer.push(configuration.getWorkFlowNotificationTopic(), wfRequest);
		producer.push(configuration.getWorkflowApplicationTopic(), wfRequest);
		Response response = new Response();
		HashMap<String, Object> data = new HashMap<>();
		data.put(Constants.STATUS, Constants.APPROVED_STATE);
		data.put(Constants.WF_IDS_CONSTANT, Arrays.asList(wfId));
		response.put(Constants.MESSAGE, Constants.STATUS_CHANGE_MESSAGE + Constants.APPROVED_STATE);
		response.put(Constants.DATA, data);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

    /*@Override
	public Response getUserWf(String rootOrg, String org, String wid, SearchCriteria criteria) {
		List<WfStatusEntity> wfStatusEntities = wfStatusRepo.findByRootOrgAndOrgAndServiceNameAndCurrentStatusAndUserId(
				rootOrg, org, criteria.getServiceName(), criteria.getApplicationStatus(), wid);
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, wfStatusEntities);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}*/

	public Response appsSearchV2(String rootOrg, SearchCriteriaV2 criteria) {
		Map<String, List<WfStatusEntity>> infos =null;
		List<WfStatusEntity> wfStatusEntities = null;
	    if(CollectionUtils.isEmpty(criteria.getDeptName())) {
			wfStatusEntities = wfStatusRepo.findByStatusAndAppIds(
					criteria.getApplicationStatus(),
					criteria.getApplicationIds());
		}
		else{
			wfStatusEntities = wfStatusRepo.findByStatusAndDeptAndAppIds(
					criteria.getApplicationStatus(),
					criteria.getApplicationIds(),
					criteria.getDeptName());
		}

		infos =	wfStatusEntities.stream().collect(Collectors.groupingBy(WfStatusEntity::getUserId));
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, infos);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	public Response applicationSearchOnApplicationIdGroup(String rootOrg, SearchCriteria criteria, boolean... isSearchEnabled) {
		boolean searchEnabled = (isSearchEnabled.length < 1)?false:isSearchEnabled[0];
		Pageable pageable = getPageReqForApplicationSearch(criteria);
		List<String> applicationIds = criteria.getApplicationIds();
		Map<String, List<WfStatusEntity>> infos = null;
		long totalRequestCount = 0;
		if (CollectionUtils.isEmpty(applicationIds)) {
			Page<String> applicationIdsPage = wfStatusRepo.getListOfDistinctApplicationUsingDept(criteria.getServiceName(),
					criteria.getApplicationStatus(), criteria.getDeptName(), pageable);
			applicationIds = applicationIdsPage.getContent();
			totalRequestCount = applicationIdsPage.getTotalElements();
		}
		List<WfStatusEntity> wfStatusEntities = null;
		if (!StringUtils.isEmpty(criteria.getDeptName())) {
			if (searchEnabled==true) {
				if (Constants.BLENDED_PROGRAM_SERVICE_NAME.equalsIgnoreCase(criteria.getServiceName())) {
					List<String> eligibleServiceNames = new ArrayList<>();
					eligibleServiceNames.add(Constants.ONE_STEP_MDO_APPROVAL);
					eligibleServiceNames.add(Constants.ONE_STEP_PC_APPROVAL);
					eligibleServiceNames.add(Constants.TWO_STEP_MDO_AND_PC_APPROVAL);
					eligibleServiceNames.add(Constants.TWO_STEP_PC_AND_MDO_APPROVAL);
					eligibleServiceNames.add(Constants.BLENDED_PROGRAM_SERVICE_NAME);
					wfStatusEntities = wfStatusRepo.findByServiceNameAndCurrentStatusAndDeptNameAndApplicationId(eligibleServiceNames, criteria.getApplicationStatus(), applicationIds,criteria.getDeptName());
				} else {
					wfStatusEntities = wfStatusRepo.findByServiceNameAndCurrentStatusAndDeptName(criteria.getServiceName(), criteria.getApplicationStatus(), criteria.getDeptName());
				}
			} else {
				wfStatusEntities = wfStatusRepo.findByServiceNameAndCurrentStatusAndDeptNameAndApplicationIdIn(
						criteria.getServiceName(), criteria.getApplicationStatus(), criteria.getDeptName(), applicationIds);
			}
		} else {
			if (Constants.BLENDED_PROGRAM_SERVICE_NAME.equalsIgnoreCase(criteria.getServiceName())) {
				List<String> eligibleServiceNames = new ArrayList<>();
				eligibleServiceNames.add(Constants.ONE_STEP_MDO_APPROVAL);
				eligibleServiceNames.add(Constants.ONE_STEP_PC_APPROVAL);
				eligibleServiceNames.add(Constants.TWO_STEP_MDO_AND_PC_APPROVAL);
				eligibleServiceNames.add(Constants.TWO_STEP_PC_AND_MDO_APPROVAL);
				eligibleServiceNames.add(Constants.BLENDED_PROGRAM_SERVICE_NAME);
				wfStatusEntities = wfStatusRepo.findByServiceNameAndCurrentStatusAndApplicationId(eligibleServiceNames, criteria.getApplicationStatus(), applicationIds);
			}else {
				wfStatusEntities = wfStatusRepo.findByServiceNameAndCurrentStatusAndApplicationIdIn(
						criteria.getServiceName(), criteria.getApplicationStatus(), applicationIds);
			}
		}
		if (criteria.getServiceName().equalsIgnoreCase(Constants.BLENDED_PROGRAM_SERVICE_NAME)) {
			infos = wfStatusEntities.stream().collect(Collectors.groupingBy(WfStatusEntity::getUserId));
		} else {
			infos = wfStatusEntities.stream().collect(Collectors.groupingBy(WfStatusEntity::getApplicationId));
		}
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, infos);
		response.put(Constants.STATUS, HttpStatus.OK);
		response.put(Constants.COUNT,totalRequestCount);
		return response;
	}

	private String getDepartmentDetails(HashMap<String, Object> updatedDeptvalue) {
		HashMap<String, String> toValue = (HashMap<String, String>) updatedDeptvalue.get("toValue");
		return toValue.get("departmentName").isEmpty() ? "" : toValue.get("departmentName");
	}

	@Override
	public Response getUserWf(String rootOrg, String org, String wid, SearchCriteria criteria) {
		List<WfStatusEntity> wfStatusEntities = wfStatusRepo.findByRootOrgAndOrgAndServiceNameAndCurrentStatusAndUserId(
				rootOrg, org, criteria.getServiceName(), criteria.getApplicationStatus(), wid);
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, wfStatusEntities);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	@Override
	public Response getUserWFApplicationFields(String rootOrg, String org, String wid, SearchCriteria criteria) {
		List<String> updatedFieldValues = wfStatusRepo.findWfFieldsForUser(rootOrg, org, criteria.getServiceName(), criteria.getApplicationStatus(), wid);
		TypeReference<List<HashMap<String, Object>>> typeRef = new TypeReference<List<HashMap<String, Object>>>() {
		};
		Map<String,Object> toValuesMap = new HashMap<>();
		for (String fields : updatedFieldValues) {
			if (!StringUtils.isEmpty(fields)) {
				try {
					List<HashMap<String, Object>> values = mapper.readValue(fields, typeRef);
					for (HashMap<String, Object> wffieldReq : values) {
						HashMap<String, Object> toValueMap = (HashMap<String, Object>) wffieldReq.get("toValue");
						toValuesMap.put(toValueMap.entrySet().iterator().next().getKey(),toValueMap.entrySet().iterator().next().getValue());
					}
				} catch (IOException e) {
					log.error("Exception occurred while parsing wf fields!");
                    log.error(e.toString());
				}
			}
		}
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, toValuesMap);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	public WorkFlowModel getWorkFlowConfig(String serviceName) {
		try {
			Map<String, Object> wfConfig = new HashMap<>();
			StringBuilder uri = new StringBuilder();
			switch (serviceName) {
				case Constants.PROFILE_SERVICE_NAME:
					uri.append(configuration.getLmsServiceHost() + configuration.getProfileServiceConfigPath());
					break;
				case Constants.USER_REGISTRATION_SERVICE_NAME:
					uri.append(configuration.getLmsServiceHost() + configuration.getUserRegistrationServiceConfigPath());
					break;
				case Constants.POSITION_SERVICE_NAME:
					uri.append(configuration.getLmsServiceHost() + configuration.getPositionServiceConfigPath());
					break;
				case Constants.ORGANISATION_SERVICE_NAME:
					uri.append(configuration.getLmsServiceHost() + configuration.getOrgServiceConfigPath());
					break;
				case Constants.DOMAIN_SERVICE_NAME:
					uri.append(configuration.getLmsServiceHost() + configuration.getDomainServiceConfigPath());
					break;
				case Constants.BLENDED_PROGRAM_SERVICE_NAME:
					uri.append(configuration.getLmsServiceHost() + configuration.getBlendedProgramServicePath());
					break;
				case Constants.ONE_STEP_PC_APPROVAL:
					uri.append(configuration.getLmsServiceHost()).append(configuration.getMultilevelBPEnrolEndPoint()).append(Constants.ONE_STEP_PC_APPROVAL);
					break;
				case Constants.ONE_STEP_MDO_APPROVAL:
					uri.append(configuration.getLmsServiceHost()).append(configuration.getMultilevelBPEnrolEndPoint()).append(Constants.ONE_STEP_MDO_APPROVAL);
					break;
				case Constants.TWO_STEP_MDO_AND_PC_APPROVAL:
					uri.append(configuration.getLmsServiceHost()).append(configuration.getMultilevelBPEnrolEndPoint()).append(Constants.TWO_STEP_MDO_AND_PC_APPROVAL);
					break;
				case Constants.TWO_STEP_PC_AND_MDO_APPROVAL:
					uri.append(configuration.getLmsServiceHost()).append(configuration.getMultilevelBPEnrolEndPoint()).append(Constants.TWO_STEP_PC_AND_MDO_APPROVAL);
					break;
				default:
					break;
			}
			wfConfig = (Map<String, Object>) requestServiceImpl.fetchResultUsingGet(uri);
			Map<String, Object> result = (Map<String, Object>) wfConfig.get(Constants.RESULT);
			Map<String, Object> response = (Map<String, Object>) result.get(Constants.RESPONSE);
			Map<String,Object> wfStates = mapper.readValue((String) response.get(Constants.VALUE),Map.class);
			WorkFlowModel workFlowModel = mapper.convertValue(wfStates, new TypeReference<WorkFlowModel>(){});
			return workFlowModel;
		} catch (Exception e) {
			log.error("Exception occurred while getting work flow config details!");
			throw new ApplicationException(Constants.WORKFLOW_PARSING_ERROR_MESSAGE, e);
		}
	}

	public Response applicationUserSearchOnApplicationIdGroup(SearchCriteria criteria) {
		List<String> applicationIds = criteria.getApplicationIds();
		List<String> servicesName = new ArrayList<>();
		if (Constants.BLENDED_PROGRAM_SERVICE_NAME.equalsIgnoreCase(criteria.getServiceName())){
			servicesName.add(Constants.ONE_STEP_MDO_APPROVAL);
			servicesName.add(Constants.ONE_STEP_PC_APPROVAL);
			servicesName.add(Constants.TWO_STEP_MDO_AND_PC_APPROVAL);
			servicesName.add(Constants.TWO_STEP_PC_AND_MDO_APPROVAL);
			servicesName.add(Constants.BLENDED_PROGRAM_SERVICE_NAME);
		}else {
			servicesName.add(criteria.getServiceName());
		}
		Map<String, List<WfStatusEntity>> infos = null;
		if (CollectionUtils.isEmpty(applicationIds)) {
			throw new ApplicationException(Constants.WORKFLOW_PARSING_ERROR_MESSAGE);
		}
		List<WfStatusEntity> wfStatusEntities = wfStatusRepo.findByServiceNameAndUserIdAndApplicationIdIn(
				servicesName, criteria.getUserId(), applicationIds);
		infos = wfStatusEntities.stream().collect(Collectors.groupingBy(WfStatusEntity::getUserId));
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, infos);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	public Response statusCountOnApplicationId(SearchCriteria criteria) {

		String applicationId = criteria.getApplicationIds().get(0);
		List<WfStatusCountDTO> statusCountDTOs;
		if(localCache.get(applicationId) != null){
		 statusCountDTOs = (List<WfStatusCountDTO>)localCache.get(applicationId);
		}
	    else {
			List<Object[]> resultSet = wfStatusRepo.findStatusCountByApplicationId(criteria.getApplicationIds());
			statusCountDTOs = new ArrayList<>();
			for (Object[] result : resultSet) {
				WfStatusCountDTO dto = new WfStatusCountDTO();
				dto.setCurrentStatus((String) result[0]);
				dto.setStatusCount(((BigInteger) result[1]).longValue());
				statusCountDTOs.add(dto);
			}
			localCache.put(applicationId,statusCountDTOs);
		}
		Response response = new Response();
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		response.put(Constants.DATA, statusCountDTOs);
		response.put(Constants.STATUS, HttpStatus.OK);
		return response;
	}

	public Response updatePendingRequestsToNewMDO(Map<String, Object> request) {
		Response response = new Response();
		try {
			Map<String, Object> requestBody = (Map<String, Object>) request.get(Constants.REQUEST);
			String userId = (String) requestBody.get(Constants.USER_ID);
			String newDeptName = (String) requestBody.get(Constants.DEPARTMENT_NAME);
			String serviceName = Constants.PROFILE_SERVICE_NAME;
			String currentStatus = Constants.SEND_FOR_APPROVAL;
			if(requestBody.containsKey(Constants.FORCE_MIGRATION) && requestBody.get(Constants.FORCE_MIGRATION).equals(true)){
				List<WfStatusEntity> wfStatusEntities = wfStatusRepo.getPendingRequests(userId,serviceName,currentStatus);
				for(WfStatusEntity wfStatusEntity: wfStatusEntities){
					List<Map<String,Object>> updatedFieldValues = mapper.readValue(wfStatusEntity.getUpdateFieldValues(), new TypeReference<List<Map<String,Object>>>() {
					});
					Map<String,Object> toValue = (Map<String, Object>) updatedFieldValues.get(0).get(Constants.TO_VALUE);
					if (toValue.containsKey(Constants.NAME)){
						wfStatusEntity.setCurrentStatus(Constants.REJECTED);
						wfStatusEntity.setInWorkflow(false);
						wfStatusRepo.save(wfStatusEntity);
					}
				}
			}
			Integer numOfUpdatedRecords = wfStatusRepo.updatePendingRequestsToNewMDO(userId, serviceName, currentStatus, newDeptName);
			log.info(String.format("The number of records updated for user: %s is %d", userId, numOfUpdatedRecords));
			response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
			response.put(Constants.STATUS, HttpStatus.OK);
		} catch (Exception e) {
			String errMsg = String.format("Exception occurred while updating pending approval requests. Exception: %s ", e.getMessage());
			response.put(Constants.ERROR_MESSAGE, errMsg);
			response.put(Constants.STATUS, HttpStatus.INTERNAL_SERVER_ERROR);
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
			log.error(errMsg);
			return response;
		}
		return response;
	}

	public SBApiResponse getBulkUpdateStatus(String userAuthToken) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_BULK_UPDATE_STATUS);
		try {
			String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
			if (StringUtils.isEmpty(userId)) {
				setErrorData(response, "Invalid User Token");
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				log.error("Failed to get uploaded file status. Error: Invalid user token");
				return response;
			}
			Map<String, Object> propertyMap = new HashMap<>();
			propertyMap.put(Constants.ID, userId);
			List<Map<String, Object>> userDetails = cassandraOperation.getRecordsByProperties(
					Constants.KEYSPACE_SUNBIRD, Constants.USER_TABLE, propertyMap, Arrays.asList(Constants.ROOT_ORG_ID));
			
			if (userDetails.isEmpty()) {
				setErrorData(response, String.format("Failed to get uploaded file status. Error: User not found for Id: %s", userId));
				log.error("Record not found in :" + Constants.USER_TABLE + Constants.DB_TABLE_NAME);
				return response;
			}
			String rootOrgId = (String) userDetails.get(0).get(Constants.USER_ROOT_ORG_ID);
			propertyMap.clear();
			propertyMap.put(Constants.ROOT_ORG_ID, rootOrgId);
			List<Map<String, Object>> bulkUpdateDetails = cassandraOperation.getRecordsByProperties(
					Constants.KEYSPACE_SUNBIRD,
					Constants.TABLE_USER_BULK_UPDATE,
					propertyMap,
					new ArrayList<>());
			response.getParams().setStatus(Constants.SUCCESSFUL);
			response.setResponseCode(HttpStatus.OK);
			response.getResult().put(Constants.CONTENT, bulkUpdateDetails);
			response.getResult().put(Constants.COUNT, bulkUpdateDetails != null ? bulkUpdateDetails.size() : 0);
		} catch (Exception e) {
			log.error("An Exception Occurred", e);
			setErrorData(response,
					String.format("Failed to get records from user_bulk_update table. Error: ", e.getMessage()));
		}
		return response;
	}

	public SBApiResponse workflowBulkUpdateTransition(String userAuthToken, MultipartFile mFile) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_BULK_UPDATE);
		try {
			String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
			if (StringUtils.isEmpty(userId)) {
				setErrorData(response, "Invalid User Token");
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				log.error("Failed to process bulk update. Error: Invalid user token");
				return response;
			}
			Map<String, Object> propertyMap = new HashMap<>();
			propertyMap.put(Constants.ID, userId);
			List<Map<String, Object>> userDetails = cassandraOperation.getRecordsByProperties(
					Constants.KEYSPACE_SUNBIRD, Constants.USER_TABLE, propertyMap, Arrays.asList(Constants.ROOT_ORG_ID));
			
			if (userDetails.isEmpty()) {
				setErrorData(response, String.format("Failed to upload file. Error: User not found for Id: %s", userId));
				log.error("Record not found in :" + Constants.USER_TABLE + Constants.DB_TABLE_NAME);
				return response;
			}

			String rootOrgId = (String) userDetails.get(0).get(Constants.USER_ROOT_ORG_ID);

			SBApiResponse uploadResponse = storageService.uploadFile(mFile, configuration.getUserBulkUpdateFolderName(), configuration.getWorkflowCloudContainerName());
			if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
				setErrorData(response, String.format("Failed to upload file. Error: %s",
						(String) uploadResponse.getParams().getErrmsg()));
				log.error("Failed to upload the given file.");
				return response;
			}

			Map<String, Object> uploadedFileDetails = new HashMap<>();
			uploadedFileDetails.put(Constants.ROOT_ORG_ID, rootOrgId);
			uploadedFileDetails.put(Constants.IDENTIFIER, UUID.randomUUID().toString());
			uploadedFileDetails.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
			uploadedFileDetails.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
			uploadedFileDetails.put(Constants.CREATED_BY, userId);
			uploadedFileDetails.put(Constants.DATE_CREATED_ON, new Timestamp(System.currentTimeMillis()));
			uploadedFileDetails.put(Constants.STATUS, Constants.INITIATED_CAPITAL);
			uploadedFileDetails.put(Constants.COMMENT, "");
			uploadedFileDetails.put(Constants.CREATED_BY, userId);

			Response insertionResponse = cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_BULK_UPDATE, uploadedFileDetails);
			if (!Constants.SUCCESS.equalsIgnoreCase((String)insertionResponse.get("STATUS"))) {
				setErrorData(uploadResponse, "Failed to insert the upload file details.");
				log.error("Failed to update database with user bulk upload file details.");
				return response;
			}
			kafkaProducer.push(configuration.getUserBulkUpdateTopic(), uploadedFileDetails);
			response.getParams().setStatus(Constants.SUCCESSFUL);
			response.setResponseCode(HttpStatus.OK);
			response.getResult().putAll(uploadedFileDetails);
		} catch(Exception e){
			log.error("Failed to process bulk upload request. Exception: ", e);
			setErrorData(response,
					String.format("Failed to process user bulk upload request. Error: ", e.getMessage()));
		}
		return response;
	}

	public boolean verifyUserRecordExists(String field, String fieldValue, Map<String, Object> userRecordDetails) {

		HashMap<String, String> headersValue = new HashMap<>();
		headersValue.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);

		Map<String, Object> filters = new HashMap<>();
		filters.put(field, fieldValue);

		Map<String, Object> request = new HashMap<>();
		request.put("filters", filters);
		request.put(Constants.FIELDS, Arrays.asList(Constants.USER_ID, Constants.STATUS, Constants.CHANNEL, Constants.ROOT_ORG_ID, Constants.PHONE, Constants.EMAIL));

		Map<String, Object> requestObject = new HashMap<>();
		requestObject.put("request", request);
		try {
			StringBuilder builder = new StringBuilder(configuration.getLmsServiceHost());
			builder.append(configuration.getLmsUserSearchEndPoint());
			Map<String, Object> userSearchResult = (Map<String, Object>) requestServiceImpl
					.fetchResultUsingPost(builder, requestObject, Map.class, headersValue);
			if (userSearchResult != null
					&& "OK".equalsIgnoreCase((String) userSearchResult.get(Constants.RESPONSE_CODE))) {
				Map<String, Object> map = (Map<String, Object>) userSearchResult.get(Constants.RESULT);
				Map<String, Object> response = (Map<String, Object>) map.get(Constants.RESPONSE);
				List<Map<String, Object>> contents = (List<Map<String, Object>>) response.get(Constants.CONTENT);
				if (!CollectionUtils.isEmpty(contents)) {
					for (Map<String, Object> content : contents) {
						userRecordDetails.put(Constants.USER_ID, content.get(Constants.USER_ID));
						userRecordDetails.put(Constants.DEPARTMENT_NAME, content.get(Constants.CHANNEL));
					}
					return true;
				}
			}
		} catch (Exception e) {
			log.error("Exception while fetching user setails : ",e);
			throw new ApplicationException("Hub Service ERROR: ", e);
		}
		return false;
	}

	public ResponseEntity<InputStreamResource> downloadBulkUploadFile(String fileName){
		HttpHeaders headers = new HttpHeaders();
		try{
			storageService.downloadFile(fileName);
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
			Path filePath = Paths.get(String.format("%s/%s", Constants.LOCAL_BASE_PATH, fileName));
			String strFilePath = Constants.LOCAL_BASE_PATH + fileName;
			File file = new File(strFilePath);
			InputStreamResource fileStream = new InputStreamResource(new FileInputStream(file));
			return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(Files.size(filePath))
                    .body(fileStream);
		}catch(Exception e){
			log.error("An error occured while downloading file", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}

	private void setErrorData(SBApiResponse response, String errMsg) {
		response.getParams().setStatus(Constants.FAILED);
		response.getParams().setErrmsg(errMsg);
		response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
	}

	@Override
	public Response getUserWFApplicationFieldsV2(String rootOrg, String org, String wid, SearchCriteria criteria) {
		Response response = new Response();
		response.put(Constants.STATUS, HttpStatus.OK);
		response.put(Constants.MESSAGE, Constants.SUCCESSFUL);
		try {
			List<Object[]> updatedFieldValues = wfStatusRepo.findWfFieldsForUserV2(criteria.getServiceName(), criteria.getApplicationStatus(), wid);
			TypeReference<List<HashMap<String, Object>>> typeRef = new TypeReference<List<HashMap<String, Object>>>() {
			};
			List<Map<String, Object>> result = new ArrayList<>();
			response.put(Constants.DATA, result);
			for (Object[] fields : updatedFieldValues) {
				if (!ObjectUtils.isEmpty(fields)) {
					List<HashMap<String, Object>> values = mapper.readValue((String) fields[0], typeRef);
					for (HashMap<String, Object> wffieldReq : values) {
						Map<String, Object> resultData = new LinkedHashMap<>();
						HashMap<String, Object> toValueMap = (HashMap<String, Object>) wffieldReq.get("toValue");
						resultData.put("wfId", fields[1]);
						resultData.put(toValueMap.entrySet().iterator().next().getKey(), toValueMap.entrySet().iterator().next().getValue());
						resultData.put(Constants.COMMENT, fields[2]);
						result.add(resultData);
					}
				}
			}
			response.put(Constants.DATA, result);
		} catch (Exception e) {
			log.error("Exception occurred while parsing wf fields!", e);
			response.put(Constants.DATA, new ArrayList<>());
			response.put(Constants.STATUS, HttpStatus.INTERNAL_SERVER_ERROR);
			response.put(Constants.MESSAGE, "Exception occurred while fetching requested fields for approval");
		}
		return response;
	}

}
