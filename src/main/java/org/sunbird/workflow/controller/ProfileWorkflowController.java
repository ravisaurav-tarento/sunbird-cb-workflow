package org.sunbird.workflow.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.workflow.models.Response;
import org.sunbird.workflow.models.SearchCriteria;
import org.sunbird.workflow.service.Workflowservice;

@RestController
@RequestMapping("/v2/workflow")
public class ProfileWorkflowController {

    @Autowired
    private Workflowservice workflowService;

    @PostMapping(path = "/getUserWFApplicationFields", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Response> getUserWFApplicationFieldsV2(@RequestHeader String rootOrg, @RequestHeader String org,
                                                                 @RequestHeader String wid, @RequestBody SearchCriteria searchCriteria) {
        Response response = workflowService.getUserWFApplicationFieldsV2(rootOrg, org, wid, searchCriteria);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
