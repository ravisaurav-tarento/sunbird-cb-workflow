package org.sunbird.workflow.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class Configuration {

    @Value("${workflow.pagination.default.limit}")
    private Integer defaultLimit;

    @Value("${workflow.pagination.default.offset}")
    private Integer defaultOffset;

    @Value("${workflow.pagination.max.limit}")
    private Integer maxLimit;

    @Value("${hub.service.host}")
    private String hubServiceHost;

    @Value("${hub.profile.update}")
    private String hubProfileUpdateEndPoint;

    @Value("${lms.user.read}")
    private String userProfileReadEndPoint;

    @Value("${lms.user.update}")
    private String userProfileUpdateEndPoint;

    @Value("${lms.user.migrate}")
    private String userProfileMigrateEndPoint;

    @Value("${lms.service.host}")
    private String lmsServiceHost;

    @Value("${lms.user.search}")
    private String lmsUserSearchEndPoint;

    @Value("${lms.org.search.path}")
    private String lmsOrgSearchEndPoint;

    @Value("${lms.assign.role}")
    private String lmsAssignRoleEndPoint;

    @Value("${pid.service.host}")
    private String pidServiceHost;

    @Value("${pid.multiplesearch.endpoint}")
    private String multipleSearchEndPoint;

    @Value("${lexcore.service.host}")
    private String lexCoreServiceHost;

    @Value("${userrole.search.endpoint}")
    private String userRoleSearchEndpoint;

    @Value("${kafka.topics.workflow.request}")
    private String workflowApplicationTopic;

    @Value("${multiple-workflow-creation}")
    private boolean multipleWfCreationEnable;

    @Value("${notify.service.host}")
    private String notifyServiceHost;

    @Value("${notify.service.path}")
    private String notifyServicePath;

    @Value("${hub.notification.rootOrg}")
    private String hubRootOrg;

    @Value("${portal.departmentupdate.path}")
    private String departmentUpdatePath;

    @Value("${hub.profile.search}")
    private String hubProfileSearchEndPoint;

    @Value("${kafka.topics.workflow.notification}")
    private String workFlowNotificationTopic;
    @Value("${kafka.topics.user.registration.createUser}")
    private String workflowCreateUserTopic;

    @Value("${lms.system.settings.wfProfileService.path}")
    private String profileServiceConfigPath;

    @Value("${lms.system.settings.wfPositionService.path}")
    private String positionServiceConfigPath;

    @Value("${lms.system.settings.wfOrgService.path}")
    private String orgServiceConfigPath;

    @Value("${lms.system.settings.wfDomainService.path}")
    private String domainServiceConfigPath;

    @Value("${notification.sender.mail}")
    private String senderMail;

    @Value("${mdo.search.fields}")
    private String mdoAdminSearchFields;

    @Value("${mdo.approval.base.url}")
    private String mdoBaseUrl;

    @Value("${domain.host}")
    private String domainHost;

    @Value("${mdo.email.template}")
    private String mdoEmailTemplate;

    @Value("${notification.email.body}")
    private String mailBody;

    @Value("${lms.system.settings.wfUserRegistrationService.path}")
    private String userRegistrationServiceConfigPath;

    @Value("${lms.system.settings.verified.profile.fields.path}")
    private String verifiedProfileFieldsPath;

    @Value("${lms.system.settings.wfBlendedProgramService.path}")
    private String blendedProgramServicePath;

    @Value("${course.service.host}")
    private String courseServiceHost;

    @Value("${course.admin.enrol}")
    private String adminEnrolEndPoint;

    @Value("${bp.email.template}")
    private String bpAprroveAndRejectEmailTemplate;

    @Value("${bp.batch.full.validation.exclude.states}")
    private String bpBatchFullValidationExcludeStates;

    @Value("${bp.batch.enrol.limit.buffer.size}")
    private Integer bpBatchEnrolLimitBufferSize;

    @Value("${course.admin.unenrol}")
    private String adminUnEnrolEndPoint;

    @Value("${content.service.host}")
    private String contentServiceHost;

    @Value("${content.read}")
    private String contentReadEndPoint;

    @Value("${ms.system.settings.multilevelBPEnroll.path}")
    private String multilevelBPEnrolEndPoint;

    @Value("${enrol.status.count.local.cache.size}")
    private Integer enrolStatusCountLocalCacheSize;

    @Value("${blended.program.enrol.conflict.reject.reason}")
    private String conflictRejectReason;

    @Value("${blended.program.enrol.batch.full.message}")
    private String batchFullMesg;

    @Value("${wfstatus.allowed.action.for.modification.history.entry}")
    private String modificationRecordAllowActions;

    @Value("${blended.program.batch.in.progress.message}")
    private String batchInProgressMessage;

    @Value("${bp.mail.body.forwarded.to}")
    private String learnerForwardedMailBody;

    @Value("${bp.mail.body.rejected.or.remove}")
    private String rejectedOrRemovedMailBody;

    @Value("${bp.mail.body.approve}")
    private String approvedMailBody;

    @Value("${notify.email.template}")
    private String notificationEmailTemplate;

    @Value("${bp.approval.request.mail.body}")
    private String approvalRequetMailBody;

    @Value("${bp.request.forwarded.mail.body}")
    private String requestForwardedMailBody;

    @Value("${bp.nomination.request.mail.body}")
    private String nominationRequestMailBody;

    @Value("${domain.validation.regex}")
    private String domainValidationRegex;

    @Value("${env.hierarchy.keystore.keyspace}")
    private String envHierarchyKeystoreKeyspaceName;

    @Value("${cloud.storage.type.name}")
    private String cloudStorageTypeName;

    @Value("${cloud.storage.key}")
    private String cloudStorageKey;

    @Value("${cloud.storage.secret}")
    private String cloudStorageSecret;

    @Value("${cloud.storage.cephs3.endpoint}")
    private String cloudStorageCephs3Endpoint;

    @Value("${workflow.cloud.container.name}")
    private String workflowCloudContainerName;

    @Value("${user.bulk.update.folder.name}")
    private String userBulkUpdateFolderName;

    @Value("${kafka.topics.user.bulk.update}")
    private String userBulkUpdateTopic;

    @Value("${mdo.org.change.email.template}")
    private String orgChangeNotificationTemplate;

    @Value("${user.bulk.upload.gender.value}")
    private String bulkUploadGenderValue;

    @Value("${user.bulk.upload.category.value}")
    private String bulkUploadCategoryValue;

    @Value("${user.default.group.values}")
    private String groupValues;

    public String getModificationRecordAllowActions() {
        return modificationRecordAllowActions;
    }

    public void setModificationRecordAllowActions(String modificationRecordAllowActions) {
        this.modificationRecordAllowActions = modificationRecordAllowActions;
    }

    public String getBatchFullMesg() {
        return batchFullMesg;
    }

    public void setBatchFullMesg(String batchFullMesg) {
        this.batchFullMesg = batchFullMesg;
    }

    public String getConflictRejectReason() {
        return conflictRejectReason;
    }

    public void setConflictRejectReason(String conflictRejectReason) {
        this.conflictRejectReason = conflictRejectReason;
    }

    public Integer getEnrolStatusCountLocalCacheSize() {
        return enrolStatusCountLocalCacheSize;
    }

    public void setEnrolStatusCountLocalCacheSize(Integer enrolStatusCountLocalCacheSize) {
        this.enrolStatusCountLocalCacheSize = enrolStatusCountLocalCacheSize;
    }

    public Integer getEnrolStatusCountLocalTimeToLive() {
        return enrolStatusCountLocalTimeToLive;
    }

    public void setEnrolStatusCountLocalTimeToLive(Integer enrolStatusCountLocalTimeToLive) {
        this.enrolStatusCountLocalTimeToLive = enrolStatusCountLocalTimeToLive;
    }

    @Value("${enrol.status.count.local.cache.timetolive}")
    private Integer enrolStatusCountLocalTimeToLive;


    public Integer getDefaultLimit() {
        return defaultLimit;
    }

    public void setDefaultLimit(Integer defaultLimit) {
        this.defaultLimit = defaultLimit;
    }

    public Integer getDefaultOffset() {
        return defaultOffset;
    }

    public void setDefaultOffset(Integer defaultOffset) {
        this.defaultOffset = defaultOffset;
    }

    public Integer getMaxLimit() {
        return maxLimit;
    }
    public String getMailBody() {
        return mailBody;
    }

    public void setMailBody(String mailBody) {
        this.mailBody = mailBody;
    }
    public void setMaxLimit(Integer maxLimit) {
        this.maxLimit = maxLimit;
    }

    public String getHubServiceHost() {
        return hubServiceHost;
    }

    public void setHubServiceHost(String hubServiceHost) {
        this.hubServiceHost = hubServiceHost;
    }

    public String getUserProfileReadEndPoint() {
        return userProfileReadEndPoint;
    }

    public void setUserProfileReadEndPoint(String userProfileReadEndPoint) {
        this.userProfileReadEndPoint = userProfileReadEndPoint;
    }

    public String getUserProfileUpdateEndPoint() {
        return userProfileUpdateEndPoint;
    }

    public void setUserProfileUpdateEndPoint(String userProfileUpdateEndPoint) {
        this.userProfileUpdateEndPoint = userProfileUpdateEndPoint;
    }

    public String getUserProfileMigrateEndPoint() {
        return userProfileMigrateEndPoint;
    }

    public void setUserProfileMigrateEndPoint(String userProfileMigrateEndPoint) {
        this.userProfileMigrateEndPoint = userProfileMigrateEndPoint;
    }

    public String getLmsAssignRoleEndPoint() {
        return lmsAssignRoleEndPoint;
    }

    public void setLmsAssignRoleEndPoint(String lmsAssignRoleEndPoint) {
        this.lmsAssignRoleEndPoint = lmsAssignRoleEndPoint;
    }

    public String getHubProfileUpdateEndPoint() {
        return hubProfileUpdateEndPoint;
    }

    public void setHubProfileUpdateEndPoint(String hubProfileUpdateEndPoint) {
        this.hubProfileUpdateEndPoint = hubProfileUpdateEndPoint;
    }

    public String getPidServiceHost() {
        return pidServiceHost;
    }

    public void setPidServiceHost(String pidServiceHost) {
        this.pidServiceHost = pidServiceHost;
    }

    public String getMultipleSearchEndPoint() {
        return multipleSearchEndPoint;
    }

    public void setMultipleSearchEndPoint(String multipleSearchEndPoint) {
        this.multipleSearchEndPoint = multipleSearchEndPoint;
    }

    public String getLexCoreServiceHost() {
        return lexCoreServiceHost;
    }

    public void setLexCoreServiceHost(String lexCoreServiceHost) {
        this.lexCoreServiceHost = lexCoreServiceHost;
    }

    public String getUserRoleSearchEndpoint() {
        return userRoleSearchEndpoint;
    }

    public void setUserRoleSearchEndpoint(String userRoleSearchEndpoint) {
        this.userRoleSearchEndpoint = userRoleSearchEndpoint;
    }

    public String getWorkflowApplicationTopic() {
        return workflowApplicationTopic;
    }

    public void setWorkflowApplicationTopic(String workflowApplicationTopic) {
        this.workflowApplicationTopic = workflowApplicationTopic;
    }

    public boolean getMultipleWfCreationEnable() {
        return multipleWfCreationEnable;
    }

    public void setMultipleWfCreationEnable(boolean multipleWfCreationEnable) {
        this.multipleWfCreationEnable = multipleWfCreationEnable;
    }

    public boolean isMultipleWfCreationEnable() {
        return multipleWfCreationEnable;
    }

    public String getNotifyServiceHost() {
        return notifyServiceHost;
    }

    public void setNotifyServiceHost(String notifyServiceHost) {
        this.notifyServiceHost = notifyServiceHost;
    }

    public String getNotifyServicePath() {
        return notifyServicePath;
    }

    public void setNotifyServicePath(String notifyServicePath) {
        this.notifyServicePath = notifyServicePath;
    }

    public String getHubRootOrg() {
        return hubRootOrg;
    }

    public void setHubRootOrg(String hubRootOrg) {
        this.hubRootOrg = hubRootOrg;
    }

    public String getDepartmentUpdatePath() {
        return departmentUpdatePath;
    }

    public void setDepartmentUpdatePath(String departmentUpdatePath) {
        this.departmentUpdatePath = departmentUpdatePath;
    }

    public String getHubProfileSearchEndPoint() {
        return hubProfileSearchEndPoint;
    }

    public void setHubProfileSearchEndPoint(String hubProfileSearchEndPoint) {
        this.hubProfileSearchEndPoint = hubProfileSearchEndPoint;
    }

    public String getWorkFlowNotificationTopic() {
        return workFlowNotificationTopic;
    }

    public void setWorkFlowNotificationTopic(String workFlowNotificationTopic) {
        this.workFlowNotificationTopic = workFlowNotificationTopic;
    }

    public String getLmsServiceHost() {
        return lmsServiceHost;
    }

    public void setLmsServiceHost(String lmsServiceHost) {
        this.lmsServiceHost = lmsServiceHost;
    }

    public String getLmsUserSearchEndPoint() {
        return lmsUserSearchEndPoint;
    }

    public void setLmsUserSearchEndPoint(String lmsUserSearchEndPoint) {
        this.lmsUserSearchEndPoint = lmsUserSearchEndPoint;
    }

    public String getWorkflowCreateUserTopic() {
        return workflowCreateUserTopic;
    }

    public void setWorkflowCreateUserTopic(String workflowCreateUserTopic) {
        this.workflowCreateUserTopic = workflowCreateUserTopic;
    }

    public String getProfileServiceConfigPath() {
        return profileServiceConfigPath;
    }

    public void setProfileServiceConfigPath(String profileServiceConfigPath) {
        this.profileServiceConfigPath = profileServiceConfigPath;
    }

    public String getPositionServiceConfigPath() {
        return positionServiceConfigPath;
    }

    public void setPositionServiceConfigPath(String positionServiceConfigPath) {
        this.positionServiceConfigPath = positionServiceConfigPath;
    }

    public String getOrgServiceConfigPath() {
        return orgServiceConfigPath;
    }

    public void setOrgServiceConfigPath(String orgServiceConfigPath) {
        this.orgServiceConfigPath = orgServiceConfigPath;
    }

    public String getDomainServiceConfigPath() {
        return domainServiceConfigPath;
    }

    public void setDomainServiceConfigPath(String domainServiceConfigPath) {
        this.domainServiceConfigPath = domainServiceConfigPath;
    }

    public String getSenderMail() {
        return senderMail;
    }

    public void setSenderMail(String senderMail) {
        this.senderMail = senderMail;
    }

    public List<String> getMdoAdminSearchFields() {
        return Arrays.asList(mdoAdminSearchFields.split(",", -1));
    }

    public void setMdoAdminSearchFields(String getMdoAdminSearchFields) {
        this.mdoAdminSearchFields = getMdoAdminSearchFields;
    }

    public String getMdoBaseUrl() {
        return mdoBaseUrl;
    }

    public void setMdoBaseUrl(String mdoBaseUrl) {
        this.mdoBaseUrl = mdoBaseUrl;
    }

    public String getDomainHost() {
        return domainHost;
    }

    public void setDomainHost(String domainHost) {
        this.domainHost = domainHost;
    }

    public String getMdoEmailTemplate() {
        return mdoEmailTemplate;
    }

    public void setMdoEmailTemplate(String mdoEmailTemplate) {
        this.mdoEmailTemplate = mdoEmailTemplate;
    }

    public String getUserRegistrationServiceConfigPath() {
        return userRegistrationServiceConfigPath;
    }

    public void setUserRegistrationServiceConfigPath(String userRegistrationServiceConfigPath) {
        this.userRegistrationServiceConfigPath = userRegistrationServiceConfigPath;
    }


    public String getVerifiedProfileFieldsPath() {
        return verifiedProfileFieldsPath;
    }

    public void setVerifiedProfileFieldsPath(String verifiedProfileFieldsPath) {
        this.verifiedProfileFieldsPath = verifiedProfileFieldsPath;
    }

    public String getBlendedProgramServicePath() {
        return blendedProgramServicePath;
    }

    public void setBlendedProgramServicePath(String blendedProgramServicePath) {
        this.blendedProgramServicePath = blendedProgramServicePath;
    }

    public String getCourseServiceHost() {
        return courseServiceHost;
    }

    public void setCourseServiceHost(String courseServiceHost) {
        this.courseServiceHost = courseServiceHost;
    }

    public String getAdminEnrolEndPoint() {
        return adminEnrolEndPoint;
    }

    public void setAdminEnrolEndPoint(String adminEnrolEndPoint) {
        this.adminEnrolEndPoint = adminEnrolEndPoint;
    }

    public String getBpAprroveAndRejectEmailTemplate() {
        return bpAprroveAndRejectEmailTemplate;
    }

    public void setBpAprroveAndRejectEmailTemplate(String bpAprroveAndRejectEmailTemplate) {
        this.bpAprroveAndRejectEmailTemplate = bpAprroveAndRejectEmailTemplate;
    }

    public List<String> getBpBatchFullValidationExcludeStates() {
        return Arrays.asList(bpBatchFullValidationExcludeStates.split(",", -1));
    }

    public void setBpBatchFullValidationExcludeStates(String bpBatchFullValidationExcludeStates) {
        this.bpBatchFullValidationExcludeStates = bpBatchFullValidationExcludeStates;
    }

    public Integer getBpBatchEnrolLimitBufferSize() {
        return bpBatchEnrolLimitBufferSize;
    }

    public void setBpBatchEnrolLimitBufferSize(Integer bpBatchEnrolLimitBufferSize) {
        this.bpBatchEnrolLimitBufferSize = bpBatchEnrolLimitBufferSize;
    }

    public String getAdminUnEnrolEndPoint() {
        return adminUnEnrolEndPoint;
    }

    public void setAdminUnEnrolEndPoint(String adminUnEnrolEndPoint) {
        this.adminUnEnrolEndPoint = adminUnEnrolEndPoint;
    }

    public String getContentServiceHost() {
        return contentServiceHost;
    }

    public void setContentServiceHost(String contentServiceHost) {
        this.contentServiceHost = contentServiceHost;
    }

    public String getContentReadEndPoint() {
        return contentReadEndPoint;
    }

    public void setContentReadSearchEndPoint(String contentReadEndPoint) {
        this.contentReadEndPoint = contentReadEndPoint;
    }

    public String getMultilevelBPEnrolEndPoint() {
        return multilevelBPEnrolEndPoint;
    }

    public void setMultilevelBPEnrolEndPoint(String multilevelBPEnrolEndPoint) {
        this.multilevelBPEnrolEndPoint = multilevelBPEnrolEndPoint;
    }


    public String getLmsOrgSearchEndPoint() {
        return lmsOrgSearchEndPoint;
    }

    public void setLmsOrgSearchEndPoint(String lmsOrgSearchEndPoint) {
        this.lmsOrgSearchEndPoint = lmsOrgSearchEndPoint;
    }


    public String getBatchInProgressMessage() {
        return batchInProgressMessage;
    }

    public void setBatchInProgressMessage(String batchInProgressMessage) {
        this.batchInProgressMessage = batchInProgressMessage;
    }

    public String getLearnerForwardedMailBody() {
        return learnerForwardedMailBody;
    }

    public void setLearnerForwardedMailBody(String learnerForwardedMailBody) {
        this.learnerForwardedMailBody = learnerForwardedMailBody;
    }

    public String getRejectedOrRemovedMailBody() {
        return rejectedOrRemovedMailBody;
    }

    public void setRejectedOrRemovedMailBody(String rejectedOrRemovedMailBody) {
        this.rejectedOrRemovedMailBody = rejectedOrRemovedMailBody;
    }

    public String getApprovedMailBody() {
        return approvedMailBody;
    }

    public void setApprovedMailBody(String approvedMailBody) {
        this.approvedMailBody = approvedMailBody;
    }

    public String getNotificationEmailTemplate() {
        return notificationEmailTemplate;
    }

    public void setNotificationEmailTemplate(String notificationEmailTemplate) {
        this.notificationEmailTemplate = notificationEmailTemplate;
    }

    public String getApprovalRequetMailBody() {
        return approvalRequetMailBody;
    }

    public void setApprovalRequetMailBody(String approvalRequetMailBody) {
        this.approvalRequetMailBody = approvalRequetMailBody;
    }

    public String getRequestForwardedMailBody() {
        return requestForwardedMailBody;
    }

    public void setRequestForwardedMailBody(String requestForwardedMailBody) {
        this.requestForwardedMailBody = requestForwardedMailBody;
    }

    public String getNominationRequestMailBody() {
        return nominationRequestMailBody;
    }

    public void setNominationRequestMailBody(String nominationRequestMailBody) {
        this.nominationRequestMailBody = nominationRequestMailBody;
    }
    public String getDomainValidationRegex() {
        return domainValidationRegex;
    }

    public void setDomainValidationRegex(String domainValidationRegex) {
        this.domainValidationRegex = domainValidationRegex;
    }

    public String getEnvHierarchyKeystoreKeyspaceName() {
        return envHierarchyKeystoreKeyspaceName;
    }

    public void setEnvHierarchyKeystoreKeyspaceName(String envHierarchyKeystoreKeyspaceName) {
        this.envHierarchyKeystoreKeyspaceName = envHierarchyKeystoreKeyspaceName;
    }

    public String getCloudStorageTypeName() {
        return cloudStorageTypeName;
    }

    public void setCloudStorageTypeName(String cloudStorageTypeName) {
        this.cloudStorageTypeName = cloudStorageTypeName;
    }

    public String getCloudStorageKey() {
        return cloudStorageKey;
    }

    public void setCloudStorageKey(String cloudStorageKey) {
        this.cloudStorageKey = cloudStorageKey;
    }

    public String getCloudStorageSecret() {
        return cloudStorageSecret;
    }

    public void setCloudStorageSecret(String cloudStorageSecret) {
        this.cloudStorageSecret = cloudStorageSecret;
    }

    public String getCloudStorageCephs3Endpoint() {
        return cloudStorageCephs3Endpoint;
    }

    public void setCloudStorageCephs3Endpoint(String cloudStorageCephs3Endpoint) {
        this.cloudStorageCephs3Endpoint = cloudStorageCephs3Endpoint;
    }

    public String getWorkflowCloudContainerName() {
        return workflowCloudContainerName;
    }

    public void setWorkflowCloudContainerName(String workflowCloudContainerName) {
        this.workflowCloudContainerName = workflowCloudContainerName;
    }

    public String getUserBulkUpdateFolderName() {
        return userBulkUpdateFolderName;
    }

    public void setUserBulkUpdateFolderName(String userBulkUpdateFolderName) {
        this.userBulkUpdateFolderName = userBulkUpdateFolderName;
    }

    public String getUserBulkUpdateTopic() {
        return userBulkUpdateTopic;
    }

    public void setUserBulkUpdateTopic(String userBulkUpdateTopic) {
        this.userBulkUpdateTopic = userBulkUpdateTopic;
    }

    public String getOrgChangeNotificationTemplate() {
        return orgChangeNotificationTemplate;
    }

    public void setOrgChangeNotificationTemplate(String orgChangeNotificationTemplate) {
        this.orgChangeNotificationTemplate = orgChangeNotificationTemplate;
    }

    public List<String> getBulkUploadGenderValue() {
        return Arrays.asList(bulkUploadGenderValue.split(",", -1));
    }

    public void setBulkUploadGenderValue(String bulkUploadGenderValue) {
        this.bulkUploadGenderValue = bulkUploadGenderValue;
    }

    public List<String> getBulkUploadCategoryValue() {
        return Arrays.asList(bulkUploadCategoryValue.split(",", -1));
    }

    public void setBulkUploadCategoryValue(String bulkUploadCategoryValue) {
        this.bulkUploadCategoryValue = bulkUploadCategoryValue;
    }

    public List<String> getGroupValues() {
        return Arrays.asList(this.groupValues.split(",", -1));
    }

    public void setGroupValues(String groupValues) {
        this.groupValues = groupValues;
    }
}
