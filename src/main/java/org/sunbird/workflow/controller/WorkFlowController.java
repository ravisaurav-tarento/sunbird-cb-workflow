package org.sunbird.workflow.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.workflow.config.Constants;
import org.sunbird.workflow.models.Response;
import org.sunbird.workflow.models.SBApiResponse;
import org.sunbird.workflow.models.SearchCriteria;
import org.sunbird.workflow.models.WfRequest;
import org.sunbird.workflow.service.Workflowservice;

@RestController
@RequestMapping("/v1/workflow")
public class WorkFlowController {

	@Autowired
	private Workflowservice workflowService;

	@PostMapping("/transition")
	public ResponseEntity<Response> wfTransition(@RequestHeader String rootOrg, @RequestHeader String org,
		    @RequestBody WfRequest wfRequest) {
		Response response = workflowService.workflowTransition(rootOrg, org, wfRequest);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping(path = "/{wfId}/{applicationId}/status", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getWfApplication(@RequestHeader String rootOrg, @RequestHeader String org,
			@PathVariable("wfId") String wfId, @PathVariable("applicationId") String applicationId) {
		Response response = workflowService.getWfApplication(rootOrg, org, wfId, applicationId);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(path = "/applications/search", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> wfApplicationSearch(@RequestHeader String rootOrg, @RequestHeader String org,
			@RequestBody SearchCriteria searchCriteria) {
		Response response = workflowService.applicationsSearch(rootOrg, org, searchCriteria);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping(path = "/nextAction/{serviceName}/{state}", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getNextActionForState(@RequestHeader String rootOrg, @RequestHeader String org,
			@PathVariable("serviceName") String serviceName, @PathVariable("state") String state) {
		Response response = workflowService.getNextActionForState(rootOrg, org, serviceName, state);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping(path = "/{wfId}/{applicationId}/history", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getApplicationHistoryOnWfId(@RequestHeader String rootOrg,
			@PathVariable("wfId") String wfId, @PathVariable("applicationId") String applicationId) {
		Response response = workflowService.getApplicationHistoryOnWfId(rootOrg, wfId, applicationId);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping(path = "/{applicationId}/history", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getApplicationWfHistory(@RequestHeader String rootOrg,
			@PathVariable("applicationId") String applicationId) {
		Response response = workflowService.getApplicationWfHistory(rootOrg, applicationId);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping(path = "/workflowProcess/{wfId}", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getWorkflowProcess(@RequestHeader String rootOrg,
			@PathVariable("wfId") String wfId) {
		Response response = workflowService.getWorkflowProcess(rootOrg, wfId);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(path = "/updateUserProfileWF", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getWorkflowProcess(@RequestHeader String rootOrg, @RequestHeader String org,
			@RequestBody WfRequest wfRequest) {
		Response response = workflowService.updateUserProfileWF(rootOrg, org, wfRequest);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(path = "/getUserWF", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getUserWF(@RequestHeader String rootOrg, @RequestHeader String org,
			@RequestHeader String wid, @RequestBody SearchCriteria searchCriteria) {
		Response response = workflowService.getUserWf(rootOrg, org, wid, searchCriteria);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(path = "/getUserWFApplicationFields", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getUserWFApplicationFields(@RequestHeader String rootOrg, @RequestHeader String org,
			@RequestHeader String wid, @RequestBody SearchCriteria searchCriteria) {
		Response response = workflowService.getUserWFApplicationFields(rootOrg, org, wid, searchCriteria);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(path = "/update/pendingRequestsToNewMDO", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> updatePendingRequestsToNewMDO(@RequestBody Map<String, Object> request){
		Response response = workflowService.updatePendingRequestsToNewMDO(request);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(path = "/admin/bulkupdate/transition", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<SBApiResponse> wfBulkUpdateTransition(@RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken,
														   @RequestParam("file")MultipartFile file) {
		SBApiResponse response = workflowService.workflowBulkUpdateTransition(userAuthToken, file);
		return new ResponseEntity<>(response, response.getResponseCode());
	}

	@GetMapping(path = "/admin/bulkupdate/getstatus", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<SBApiResponse> getBulkUpdateStatus(@RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {
		SBApiResponse response = workflowService.getBulkUpdateStatus(userAuthToken);
		return new ResponseEntity<>(response, response.getResponseCode());
	}

	@GetMapping(path = "/admin/bulkbuplodfile/download/{fileName}")
	public ResponseEntity<?> downloadBulkuplodFile(@PathVariable("fileName") String fileName) {
		return workflowService.downloadBulkUploadFile(fileName);
	}
}
