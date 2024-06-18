package org.sunbird.workflow.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Constants {

    private Constants() {
	}

	public static final String ROOT_ORG = "igot";

	public static final String ORG = "dopt";

	public static final String MESSAGE = "message";

	public static final String DATA = "data";

	public static final String STATUS = "status";

	public static final String WF_ID_CONSTANT = "wfId";

	public static final String WF_IDS_CONSTANT = "wfIds";

	public static final String SUCCESSFUL = "Successful";

	public static final String STATUS_CHANGE_MESSAGE = "Application status changed to ";

	public static final String WORKFLOW_PARSING_ERROR_MESSAGE = "Workflow parsing error occurred!";

	public static final String WORKFLOW_INITIATE_ERROR_MESSAGE = "Workflow is not initiated!";

	public static final String WORKFLOW_ROLE_ERROR = "No role found for the actor!";

	public static final String WORKFLOW_ROLE_CHECK_ERROR = "No proper role found for the actor!";

	public static final String WORKFLOW_ACTION_ERROR = "No action found on given action!";

	public static final String WORKFLOW_STATE_CHECK_ERROR = "No wf state found on given state!";

	public static final String STATE_VALIDATION_ERROR = "Work flow state can not be empty!";

	public static final String ACTOR_UUID_VALIDATION_ERROR = "actor uuid can not be empty!";

	public static final String APPLICATION_ID_VALIDATION_ERROR = "application id can not be empty!";

	public static final String USER_UUID_VALIDATION_ERROR = "user uuid can not be empty!";

	public static final String ACTION_VALIDATION_ERROR = "Work flow action can not be empty!";

	public static final String FIELD_VALUE_VALIDATION_ERROR = "Update Field values can not be empty!";

	public static final String WORKFLOW_SERVICENAME_VALIDATION_ERROR = "Workflow service name can not be empty!";

	public static final String JSON_PARSING_ERROR = "Json parsing error occured!";

	public static final String WORKFLOW_ID_ERROR_MESSAGE = "Workflow id can not be empty!";

	public static final String ACTION_CONSTANT = "action";

	public static final String ROLES_CONSTANT = "roles";

	public static final String IS_WORKFLOW_TERMINATED = "inWorkflow";

	public static final String SEARCH_CRITERIA_VALIDATION = "Provide the mandatory fields for search!";

	public static final String PROFILE_SERVICE_NAME = "profile";

	public static final String BLENDED_PROGRAM_SERVICE_NAME = "blendedprogram";
	public static final String POSITION_SERVICE_NAME = "position";
	public static final String ORGANISATION_SERVICE_NAME = "organisation";
	public static final String DOMAIN_SERVICE_NAME = "domain";

	public static final String SERVICE_NAME_EXCEPTION = "No configured service found!";

	public static final String USER_ID_VALUE = "userIdValue";

	public static final String APPROVED_STATE = "APPROVED";

	public static final String PROCESSED_STATE = "PROCESSED";

	public static final String EMAIL = "email";

	public static final String FIRST_NAME = "first_name";

	public static final String UUID = "wid";

	public static final List<String> USER_DEFAULT_FIELDS = Collections
			.unmodifiableList(Arrays.asList(UUID, FIRST_NAME, EMAIL));

	public static final String CBP_WF_SERVICE_NAME = "cbp-content";

	public static final String USER_PROFILE_FLAG_SERVICE = "userprofile_flag_service";

	public static final String APPROVE_STATE = "APPROVE";

	public static final String ROOT_ORG_CONSTANT = "rootOrg";

	public static final String ROOT_ORG_ID = "rootOrgId";

	public static final String USER_ID = "userId";

	public static final String FIRSTNAME = "firstname";

	public static final String PRIMARY_EMAIL = "primaryEmail";

	public static final String RESULT = "result";
	public static final String RESPONSE_CODE = "responseCode";
	public static final String RESPONSE = "response";
	public static final String CONTENT = "content";
	public static final String PROFILE_DETAILS = "profileDetails";
	public static final String REQUEST = "request";
	public static final String PERSONAL_DETAILS = "personalDetails";
	public static final String EMPLOYMENT_DETAILS = "employmentDetails";
	public static final String DEPARTMENT_NAME = "departmentName";
	public static final String USER_REGISTRATION_SERVICE_NAME = "user_registration";
	public static final String WF_APPROVED_STATE = "WF_APPROVED";
	public static final String OSID = "osid";
	public static final String FIELD_KEY = "fieldKey";
	public static final String TO_VALUE = "toValue";
	public static final String FROM_VALUE = "fromValue";
	public static final String PROFESSIONAL_DETAILS = "professionalDetails";
	public static final String CHANNEL = "channel";
	public static final String FORCE_MIGRATION = "forceMigration";
	public static final String SOFT_DELETE_OLD_GROUP = "softDeleteOldOrg";
	public static final String NOTIFY_MIGRATION = "notifyMigration";
	public static final String X_AUTH_USER_ID = "x-authenticated-userid";
	public static final String ORG_NAME = "orgName";
	public static final String OK = "OK";
	public static final String FAILED = "FAILED";
	public static final String PARAMS = "params";
	public static final String ERROR_MESSAGE = "errmsg";
	public static final String CONTENT_TYPE = "Content-Type";
	public static final String APPLICATION_JSON = "application/json";
	public static final String REJECTED = "REJECTED";
	public static final String HTTP_STATUS_CODE = "httpStatusCode";
	public static final String NAME = "name";
	public static final String ORGANIZATION_ID = "organisationId";
	public static final String ROLES = "roles";
	public static final String PUBLIC = "PUBLIC";
	public static final String VALUE = "value";
	public static final boolean POSITION_SEARCH_ENABLED = true;
	public static final boolean DOMAIN_SEARCH_ENABLED = true;
	public static final boolean ORG_SEARCH_ENABLED = true;

	public static final boolean BLENDED_PROGRAM_SEARCH_ENABLED = true;
	public static final String MDO_ADMIN = "MDO_ADMIN";
	public static final String FIELDS = "fields";
	public static final String APPROVED = "APPROVED";
	public static final String SUPPORT_EMAIL = "supportEmail";
	public static final String LINK = "link";
	public static final String USER_NAME = "userName";
	public static final String VERIFIED_KARMAYOGI = "verifiedKarmayogi";
	public static final String SUCCESS = "success";
	public static final String EXCEPTION_MSG_FETCH = "Exception occurred while fetching record from ";
	public static final String EXCEPTION_MSG_DELETE = "Exception occurred while deleting record from ";
	public static final String INSERT_INTO = "INSERT INTO ";
	public static final String DOT = ".";
	public static final String OPEN_BRACE = "(";
	public static final String VALUES_WITH_BRACE = ") VALUES (";
	public static final String QUE_MARK = "?";
	public static final String COMMA = ",";
	public static final String CLOSING_BRACE = ");";

	public static final String KEYSPACE_SUNBIRD = "sunbird";
	public static final String KEYSPACE_SUNBIRD_COURSES = "sunbird_courses";
	public static final String TABLE_COURSE_BATCH = "course_batch";
	public static final String TABLE_ENROLMENT_BATCH_LOOKUP = "enrollment_batch_lookup";
	public static final String BATCH_ATTRIBUTES = "batch_attributes";
	public static final String CURRENT_BATCH_SIZE = "currentBatchSize";
	public static final String BATCH_ID = "batchId";
	public static final String COURSE_ID = "courseId";
	public static final String ID = "id";
	public static final String ENROLLED_DATE = "enrolled_date";
	public static final String ENROLMENT_END_DATE = "enrollment_enddate";
	public static final String CORE_CONNECTIONS_PER_HOST_FOR_LOCAL = "coreConnectionsPerHostForLocal";
	public static final String CORE_CONNECTIONS_PER_HOST_FOR_REMOTE = "coreConnectionsPerHostForRemote";
	public static final String MAX_CONNECTIONS_PER_HOST_FOR_LOCAl = "maxConnectionsPerHostForLocal";
	public static final String MAX_CONNECTIONS_PER_HOST_FOR_REMOTE = "maxConnectionsPerHostForRemote";
	public static final String MAX_REQUEST_PER_CONNECTION = "maxRequestsPerConnection";
	public static final String HEARTBEAT_INTERVAL = "heartbeatIntervalSeconds";
	public static final String POOL_TIMEOUT = "poolTimeoutMillis";
	public static final String CASSANDRA_CONFIG_HOST = "cassandra.config.host";
	public static final String SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL = "sunbird_cassandra_consistency_level";
	public static final String UNAUTHORIZED = "unauthorized";
	public static final String SEND_FOR_MDO_APPROVAL = "SEND_FOR_MDO_APPROVAL";
	public static final String SEND_FOR_PC_APPROVAL = "SEND_FOR_PC_APPROVAL";
	public static final String PROGRAM_COORDINATOR = "PROGRAM_COORDINATOR";
	public static final String TEMPLATE = "template";
	public static final String TABLE_EMAIL_TEMPLATE = "email_template";
	public static final String MISSING_APPLICATION_IDS_ERROR_MESSAGE = "Application Ids are missing in the request";
	public static final String ADDITIONAL_PROPERTIES = "additionalProperties";
	public static final String TAG = "tag";

	public static final String BP_ENROLL_STATE = "ENROLL_STATE";
	public static final String BP_UPDATE_STATE = "UPDATE_STATE";
	public static final String COURSE_ID_LIST = "courseIdList";
	public static final String END_DATE = "end_date";
	public static final String BATCH_ID_KEY = "batchid";
	public static final String COURSE_ID_KEY = "courseid";
	public static final String BATCH_DETAILS_KEY = "batchDetails";
	public static final String NEW_REQUEST_COUNT = "newRequestCount";
	public static final String LEARNER_COUNT = "learnerCount";
	public static final String REJECTED_COUNT = "rejectedCount";
	public static final String TOTAL_NEW_REQUEST_COUNT = "totalNewRequestCount";
	public static final String TOTAL_LEARNER_COUNT = "totalLearnerCount";
	public static final String TOTAL_REJECTED_COUNT = "totalRejectedCount";
	public static final String COUNT = "count";
	public static final String USER_ENROLMENTS="user_enrolments";
	public static final String START_DATE="start_date";
	public static final String ENROLLMENT_TYPE= "enrollmenttype";
	public static final String INVITE_ONLY="invite-only";
	public static final String REMOVED = "REMOVED";

	public static final String MULTILEVEL_BP_ENROLL_FIELDS="primaryCategory,wfApprovalType";
	public static final String CREATED_FOR= "createdFor";
	public static final String BATCH_IS_FULL="BATCH_IS_FULL";
	public static final String ENROLL_IS_IN_PROGRESS="ENROLL_IS_IN_PROGRESS";
	public static final String ONE_STEP_MDO_APPROVAL="oneStepMDOApproval";
	public static final String ONE_STEP_PC_APPROVAL="oneStepPCApproval";
	public static final String TWO_STEP_MDO_AND_PC_APPROVAL="twoStepMDOAndPCApproval";
	public static final String TWO_STEP_PC_AND_MDO_APPROVAL="twoStepPCAndMDOApproval";

	public static final String ADMIN_ENROLL_IS_IN_PROGRESS="ADMIN_ENROLL_IS_IN_PROGRESS";

	public  static final String INITIATE ="INITIATE";
	public static final String CURRENT_STATUS="current_status";

	public static final String REJECT="REJECT";

	public static final String FILTERS = "filters";
	public static final String PHONE = "phone";
	public static final String EMAIL_EXIST_ERROR = "Email id already registered";
	public static final String PHONE_NUMBER_EXIST_ERROR = "Phone number is already registered.";
	public static final String ORGANIZATION_EXIST_ERROR = "Organization is already registered.";
	public static final String ORGANIZATION_NAME ="orgName";
	public static final String BATCH_START_DATE_ERROR = "Batch Start Date Error";
	public static final String BATCH_SIZE_ERROR = "Batch Size Error";;
	public static final String TRUE="True";
	public static final String ACTIVE = "active";
	public static final String DESCRIPTION = "description";
	public static final String COURSE_NAME = "courseName";
	public static final String BATCH_NAME = "batchName";
	public static final String BATCH_START_DATE = "batchStartDate";
	public static final String FROM_EMAIL = "fromEmail";
	public static final String DEV_HIERARCHY_STORE = "dev_hierarchy_store";
	public static final String CONTENT_HIERARCHY = "content_hierarchy";
	public static final String IDENTIFIER = "identifier";
	public static final String HIERARCHY = "hierarchy";
	public static final Object KARMYOGI_BHARAT = "Karmyogi Bharat";
	public static final String TO_PROGRAMME_COORDINATOR = "Programme Coordinator";
	public static final String TO_MDO_ADMIN = "MDO Admin";
	public static final String DOMAIN ="domain";
	public static final String TABLE_MASTER_DATA = "master_data";
	public static final String CONTEXT_TYPE = "contextType";
	public static final String CONTEXT_NAME = "contextName";
	public static final String USER_REGISTRATION_PRE_APPROVED_DOMAIN = "userRegistrationPreApprovedDomain";
	public static final String DOMAIN_NAME_REQUEST_EXIST_MSG = "Already a request is raised for domain";
	public static final String DOMAIN_NAME_APPROVED_ERROR_MSG = "Domain is already approved";
	public static final String DOMAIN_NOT_VALID_ERROR_MSG = "Not a valid domain";
	public static final String MSG_CODE = "msgCode";
	public static final String DOMAIN_REQUEST_ALREADY_RAISED = "DOMAIN_REQUEST_ALREADY_RAISED";
	public static final String DOMAIN_APPROVED = "DOMAIN_APPROVED";
	public static final String DOMAIN_REQUEST_REJECTED = "DOMAIN_REQUEST_REJECTED";
	public static final String DOMAIN_REQUEST_REJECTED_MSG = "Domain request is rejected by admin";
	public static final String DOMAIN_REQUEST_ALREADY_PRESENT = "DOMAIN_REQUEST_ALREADY_PRESENT";
	public static final String DOMAIN_REQUEST_CREATED = "DOMAIN_REQUEST_CREATED";
	public static final String SEND_FOR_APPROVAL = "SEND_FOR_APPROVAL";
	public static final String LOCAL_BASE_PATH= "/tmp/";
	public static final String FILE_NAME = "fileName";
	public static final String FILE_PATH = "filePath";
	public static final String DATE_CREATED_ON = "dateCreatedOn";
	public static final String DATE_UPDATE_ON = "dateUpdatedOn";
	public static final String INITIATED_CAPITAL = "INITIATED";
	public static final String COMMENT = "comment";
	public static final String CREATED_BY = "createdBy";
	public static final String URL = "url";
	public static final String STATUS_IN_PROGRESS_UPPERCASE = "IN-PROGRESS";
	public static final String ROOT_ORG_ID_LOWER = "rootorgid";
	public static final String TOTAL_RECORDS = "totalRecords";
	public static final String SUCCESSFUL_RECORDS_COUNT = "successfulRecordsCount";
	public static final String FAILED_RECORDS_COUNT = "failedRecordsCount";
	public static final String TABLE_USER_BULK_UPDATE = "user_bulk_update";
	public static final String SUCCESS_UPPERCASE = "SUCCESS";
	public static final String FAILED_UPPERCASE = "FAILED";
	public static final String EMPTY_FILE_FAILED = "The uploaded file is empty";
	public static final String X_AUTH_TOKEN = "x-authenticated-user-token";
	public static final String DOT_SEPARATOR = ".";
	public static final String SHA_256_WITH_RSA = "SHA256withRSA";
	public static final String _UNAUTHORIZED = "Unauthorized";
	public static final String SUB = "sub";
	public static final String SSO_URL = "sso.url";
	public static final String SSO_REALM = "sso.realm";
	public static final String ACCESS_TOKEN_PUBLICKEY_BASEPATH = "accesstoken.publickey.basepath";
	public static final String USER_TABLE = "user";
	public static final String USER_ROOT_ORG_ID = "rootorgid";
	public static final String DB_TABLE_NAME = "table";
	public static final String API_VERSION_1 = "1.0";
	public static final String API_FILE_UPLOAD = "api.file.upload";
	public static final String API_FILE_DOWNLOAD = "api.file.download";
	public static final String API_USER_BULK_UPDATE = "api.user.bulk.update";
	public static final String API_USER_BULK_UPDATE_STATUS = "api.user.bulk.update.status";
	public static final String UPDATE_FAILED = "UPDATE FAILED";
	public static final String DESIGNATION = "designation";
	public static final String GROUP = "group";
	public static final String SERVICE = "service";
	public static final String CADRE = "cadre";
	public static final String PAY_TYPE = "payType";
	public static final String INDUSTRY = "industry";
	public static final String LOCATION = "location";
	public static final String ERROR_DETAILS = "Error Details";
	public static final String DATE_OF_JOINING = "dojOfService";
	public static final String STATUS_BULK_UPLOAD = "Status";
	public static final String WITHDRAW = "WITHDRAW";
	public static final String SENDER = "sender";
	public static final String TRANSFER_SUBJECT_LINE = "Transfer Notification: #username";
	public static final String LAST_UPDATED_ON = "lastUpdatedOn";
	public static final String DOMICILE_MEDIUM = "domicileMedium";

	public static final String EMPLOYEE_CODE = "employeeCode";
	public static final String GENDER = "gender";
	public static final String DOB = "dob";
	public static final String CATEGORY = "category";
	public static final String EXTERNAL_SYSTEM_ID = "externalSystemId";
	public static final String PIN_CODE = "pinCode";
	public static final String EXTERNAL_SYSTEM = "externalSystem";
	public static final String IS_ORG_TRANSFER_REQUEST = "isOrgTransferRequest";
	public static final String PROFILE_STATUS = "profileStatus";
	public static final String NOT_VERIFIED = "NOT-VERIFIED";
	public static final String VERIFIED = "VERIFIED";
	public static final String CURRENT_STATUS_KEY = "currentStatus";
	public static final String PROFILE_STATUS_UPDATED_ON = "profileStatusUpdatedOn";
	public static final String PROFILE_STATUS_UPDATED_MSG_VIEWED = "isProfileUpdatedMsgViewed";
	public static final String REDIS_COMMON_KEY = "workflow_";
	public static final String PROFILE_GROUP_STATUS = "profileGroupStatus";
	public static final String PROFILE_DESIGNATION_STATUS = "profileDesignationStatus";
	public static final String PINCODE = "pincode";
	public static final String FIRST_NAME_CAMEL_CASE = "firstName";
	public static final String MOBILE = "mobile";
	public static final String FIELDS_CONSTANT = "fields";
	public static final String ROOT_ORG_NAME = "rootOrgName";
	public static final String MOBILE_NUMBER = "mobileNumber";
	public static final String PROFILE_DETAILS_PERSONAL_DETAILS_MOBILE = "profileDetails.personalDetails.mobile";
	public static final String PROFILE_DETAILS_PERSONAL_DETAILS_FIRST_NAME = "profileDetails.personalDetails.firstname";
	public static final String PROFILE_DETAILS_PERSONAL_DETAILS_PRIMARY_EMAIL = "profileDetails.personalDetails.primaryEmail";

}