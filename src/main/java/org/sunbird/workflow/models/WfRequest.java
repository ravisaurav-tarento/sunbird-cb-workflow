package org.sunbird.workflow.models;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class WfRequest {

	private String state;

	private String action;

	private String applicationId;

	private String userId;

	private String actorUserId;

	private String wfId;

	private List<HashMap<String, Object>> updateFieldValues;

	private String comment;

	private String serviceName;

	private String deptName;

	private String rootOrgId;
	private String courseId;
	private String courseName;
	private String batchName;
	private Date batchStartDate;
	private boolean isNominatedByMdo;

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getActorUserId() {
		return actorUserId;
	}

	public void setActorUserId(String actorUserId) {
		this.actorUserId = actorUserId;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getWfId() {
		return wfId;
	}

	public void setWfId(String wfId) {
		this.wfId = wfId;
	}

	public List<HashMap<String, Object>> getUpdateFieldValues() {
		return updateFieldValues;
	}

	public void setUpdateFieldValues(List<HashMap<String, Object>> updateFieldValues) {
		this.updateFieldValues = updateFieldValues;
	}

	public String getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getDeptName() {
		return deptName;
	}

	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}

	public String getRootOrgId() {
		return rootOrgId;
	}

	public void setRootOrgId(String rootOrgId) {
		this.rootOrgId = rootOrgId;
	}

	public String getCourseId() {
		return courseId;
	}

	public void setCourseId(String courseId) {
		this.courseId = courseId;
	}

	public String getCourseName() {
		return courseName;
	}

	public void setCourseName(String courseName) {
		this.courseName = courseName;
	}

	public String getBatchName() {
		return batchName;
	}

	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}

	public Date getBatchStartDate() {
		return batchStartDate;
	}

	public void setBatchStartDate(Date batchStartDate) {
		this.batchStartDate = batchStartDate;
	}

	public boolean isNominatedByMdo() {
		return isNominatedByMdo;
	}

	public void setNominatedByMdo(boolean nominatedByMdo) {
		isNominatedByMdo = nominatedByMdo;
	}

}
