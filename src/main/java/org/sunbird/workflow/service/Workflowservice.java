package org.sunbird.workflow.service;

import java.util.Map;

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.workflow.models.Response;
import org.sunbird.workflow.models.SBApiResponse;
import org.sunbird.workflow.models.SearchCriteria;
import org.sunbird.workflow.models.SearchCriteriaV2;
import org.sunbird.workflow.models.WfRequest;
import org.sunbird.workflow.models.WfStatus;

public interface Workflowservice {

    public Response workflowTransition(String rootOrg, String org, WfRequest wfRequest);

    public Response workflowTransition(String rootOrg, String org, WfRequest wfRequest,String userId,String role);

    public Response getWfApplication(String rootOrg, String org, String wfId, String applicationId);

    public Response wfApplicationSearch(String rootOrg, String org, SearchCriteria criteria);

    public Response getApplicationHistoryOnWfId(String rootOrg, String wfId, String userId);

    public Response getNextActionForState(String rootOrg, String org, String serviceName, String state);

    public WfStatus getWorkflowStates(String rootOrg, String org, String serviceName, String state);

    public Response getApplicationWfHistory(String rootOrg, String applicationId);

    public Response applicationsSearch(String rootOrg, String org, SearchCriteria criteria, boolean... isSearchEnabled);

    public Response getWorkflowProcess(String rootOrg, String wfId);

    public Response updateUserProfileWF(String rootOrg, String org, WfRequest wfRequest);
    
    public Response getUserWf(String rootOrg, String org, String wid, SearchCriteria criteria);
    
    public Response getUserWFApplicationFields(String rootOrg, String org, String wid, SearchCriteria criteria);

    public Response statusCountOnApplicationId(SearchCriteria criteria);

    public Response appsPCSearchV2(String rootOrg, String org, SearchCriteriaV2 criteria);

    public Response updatePendingRequestsToNewMDO(Map<String, Object> request);

    public SBApiResponse workflowBulkUpdateTransition(String userAuthToken, MultipartFile file);

    public SBApiResponse getBulkUpdateStatus(String userAuthToken);

    public ResponseEntity<InputStreamResource> downloadBulkUploadFile(String fileName);

    public Response profileChangeRequestWithdraw(String wid, WfRequest wfRequest);
}
