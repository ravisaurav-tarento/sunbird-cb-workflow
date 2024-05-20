package org.sunbird.workflow.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.sunbird.workflow.config.Configuration;
import org.sunbird.workflow.config.Constants;
import org.sunbird.workflow.exception.ApplicationException;
import org.sunbird.workflow.models.SBApiResponse;
import org.sunbird.workflow.models.WfRequest;
import org.sunbird.workflow.postgres.entity.WfStatusEntity;
import org.sunbird.workflow.postgres.repo.WfStatusRepo;
import org.sunbird.workflow.service.impl.RequestServiceImpl;
import org.sunbird.workflow.utils.CassandraOperation;
import org.sunbird.workflow.utils.ValidationUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class UserBulkUploadService {
    private final Logger logger = LoggerFactory.getLogger(UserBulkUploadService.class);
    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    Configuration configuration;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    private RequestServiceImpl requestServiceImpl;

//    @Autowired
//    UserUtilityService userUtilityService;

    @Autowired
    private WfStatusRepo wfStatusRepo;

    @Autowired
    UserProfileWfService userProfileWfService;

    @Autowired
    private ObjectMapper mapper;



    @Autowired
    StorageService storageService;

    public void initiateUserBulkUploadProcess(String inputData) {
        logger.info("UserBulkUploadService:: initiateUserBulkUploadProcess: Started");
        long duration = 0;
        long startTime = System.currentTimeMillis();
        try {
            HashMap<String, String> inputDataMap = objectMapper.readValue(inputData, new TypeReference<HashMap<String, String>>() {});
            if (null != inputData) {
                this.updateUserBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID),
                        inputDataMap.get(Constants.IDENTIFIER), Constants.STATUS_IN_PROGRESS_UPPERCASE,
                        0,
                        0,
                        0);
                storageService.downloadFile(inputDataMap.get(Constants.FILE_NAME));
                this.processBulkUpload(inputDataMap);
            } else {
                logger.error("Error in the Kafka Message Received");
            }
        } catch (Exception e) {
            String errMsg = String.format("Error in the scheduler to upload bulk users %s", e.getMessage());
            logger.error(errMsg, e);
        }
        duration = System.currentTimeMillis() - startTime;
        logger.info("UserBulkUploadService:: initiateUserBulkUploadProcess: Completed. Time taken: {} milli-seconds", duration );
    }

    public void updateUserBulkUploadStatus(String rootOrgId, String identifier, String status, int totalRecordsCount,
                                           int successfulRecordsCount, int failedRecordsCount) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
            compositeKeys.put(Constants.IDENTIFIER, identifier);
            Map<String, Object> fieldsToBeUpdated = new HashMap<>();
            if (!status.isEmpty()) {
                fieldsToBeUpdated.put(Constants.STATUS, status);
            }
            if (totalRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.TOTAL_RECORDS, totalRecordsCount);
            }
            if (successfulRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.SUCCESSFUL_RECORDS_COUNT, successfulRecordsCount);
            }
            if (failedRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.FAILED_RECORDS_COUNT, failedRecordsCount);
            }
            fieldsToBeUpdated.put(Constants.DATE_UPDATE_ON, new Timestamp(System.currentTimeMillis()));
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_BULK_UPDATE,
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error(String.format("Error in Updating User Bulk Upload Status in Cassandra %s", e.getMessage()), e);
        }
    }

    private void processBulkUpload(HashMap<String, String> inputDataMap) throws IOException {
        File file = null;
        FileInputStream fis = null;
        XSSFWorkbook wb = null;
        int totalRecordsCount = 0;
        int noOfSuccessfulRecords = 0;
        int failedRecordsCount = 0;
        String status = "";
        try {
            file = new File(Constants.LOCAL_BASE_PATH + inputDataMap.get(Constants.FILE_NAME));
            if (file.exists() && file.length() > 0) {
                fis = new FileInputStream(file);
                wb = new XSSFWorkbook(fis);
                XSSFSheet sheet = wb.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                // incrementing the iterator inorder to skip the headers in the first row
                if (rowIterator.hasNext()) {
                    Row firstRow = rowIterator.next();
                    Cell statusCell = firstRow.getCell(14);
                    Cell errorDetails = firstRow.getCell(15);
                    if (statusCell == null) {
                        statusCell = firstRow.createCell(14);
                    }
                    if (errorDetails == null) {
                        errorDetails = firstRow.createCell(15);
                    }
                    statusCell.setCellValue(Constants.STATUS_BULK_UPLOAD);
                    errorDetails.setCellValue(Constants.ERROR_DETAILS);
                }
                int count = 0;
                while (rowIterator.hasNext()) {
                    logger.info("UserBulkUploadService:: Record {}" , count++);
                    long duration = 0;
                    long startTime = System.currentTimeMillis();
                    StringBuffer str = new StringBuffer();
                    List<String> errList = new ArrayList<>();
                    Row nextRow = rowIterator.next();
                    if (nextRow == null) {
                        break;
                    } else{
                        boolean emailCellExists = true;
                        boolean phoneCellExists = true;
                        if(nextRow.getCell(0) == null)
                            emailCellExists = false;
                        if(nextRow.getCell(1) == null)
                            phoneCellExists  = false;
                        if(nextRow.getCell(0) != null && nextRow.getCell(0).getCellType() == CellType.BLANK)
                            emailCellExists = false;
                        if(nextRow.getCell(1) != null && nextRow.getCell(1).getCellType() == CellType.BLANK)
                            phoneCellExists = false;
                        if(!emailCellExists && !phoneCellExists)
                            break;
                    }
                    Map<String, Object> valuesToBeUpdate = new HashMap<>();
                    Map<String, Object> userDetails = null;
                    boolean isEmailOrPhoneNumberValid = false;
                    Cell statusCell = nextRow.getCell(14);
                    if (statusCell == null) {
                        statusCell = nextRow.createCell(14);
                    }
                    Cell errorDetails = nextRow.getCell(15);
                    if (errorDetails == null) {
                        errorDetails = nextRow.createCell(15);
                    }
                    if (nextRow.getCell(0) != null && nextRow.getCell(0).getCellType() != CellType.BLANK) {
                        String email = nextRow.getCell(0).getStringCellValue().trim();
                        if(ValidationUtil.validateEmailPattern(email)){
                            userDetails = new HashMap<>();
                            isEmailOrPhoneNumberValid = this.verifyUserRecordExists(Constants.EMAIL, email, userDetails);
                        } else{
                            errList.add("The email provided in Invalid");
                        }
                    }
                    if(!isEmailOrPhoneNumberValid){
                        String phoneNumber = null;
                        boolean isValidPhoneNumber = false;
                        if (nextRow.getCell(1) != null && nextRow.getCell(1).getCellType() != CellType.BLANK) {
                            if (nextRow.getCell(1).getCellType() == CellType.NUMERIC) {
                                phoneNumber = NumberToTextConverter.toText(nextRow.getCell(1).getNumericCellValue());
                            } else if (nextRow.getCell(1).getCellType() == CellType.STRING) {
                                phoneNumber = nextRow.getCell(1).getStringCellValue().trim();
                            } else {
                                errList.add("Invalid Value of Phone Number. Expecting number/string format");
                            }
                        } else {
                            errList.add("Phone Number is Missing");
                        }
                        if(!StringUtils.isEmpty(phoneNumber)){
                            isValidPhoneNumber = ValidationUtil.validateContactPattern(phoneNumber);
                            if(isValidPhoneNumber){
                                userDetails = new HashMap<>();
                                isEmailOrPhoneNumberValid = this.verifyUserRecordExists(Constants.PHONE, phoneNumber, userDetails);
                            } else{
                                errList.add("The phone number provided is Invalid");
                            }
                        }
                    }
                    if(!isEmailOrPhoneNumberValid){
                        errList.add("User record does not exist with given email and/or phone number");
                    } else{
                        errList.clear();
                        String userRootOrgId = (String) userDetails.get(Constants.ROOT_ORG_ID);
                        String mdoAdminRootOrgId = inputDataMap.get(Constants.ROOT_ORG_ID);
                        if(!mdoAdminRootOrgId.equalsIgnoreCase(userRootOrgId)){
                            errList.add("The User belongs to a different MDO Organisation");
                            this.setErrorDetails(str, errList, statusCell, errorDetails);
                            failedRecordsCount++;
                            totalRecordsCount++;
                            continue;
                        }
                    }
                    if (nextRow.getCell(2) != null && nextRow.getCell(2).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(2).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.FIRSTNAME, nextRow.getCell(2).getStringCellValue().trim());
                            if (!ValidationUtil.validateFullName(nextRow.getCell(2).getStringCellValue().trim())) {
                                errList.add("Invalid Full Name");
                            }
                        } else {
                            errList.add("Invalid value for Full Name type. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(3) != null && nextRow.getCell(3).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(3).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.DESIGNATION, nextRow.getCell(3).getStringCellValue().trim());
                        } else {
                            errList.add("Invalid value for Designation type. Expecting string format");
                        }

                    }
                    if (nextRow.getCell(4) != null && nextRow.getCell(4).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(4).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.GROUP, nextRow.getCell(4).getStringCellValue().trim());
                        } else {
                            errList.add("Invalid value for Group type. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(5) != null && nextRow.getCell(5).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(5).getCellType() == CellType.NUMERIC) {
                            valuesToBeUpdate.put(Constants.EMPLOYEE_CODE, NumberToTextConverter.toText(nextRow.getCell(5).getNumericCellValue()).trim());
                        } else if (nextRow.getCell(5).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.EMPLOYEE_CODE, nextRow.getCell(5).getStringCellValue().trim());
                        } else {
                            errList.add("Invalid value for Employee ID type. Expecting string/number format");
                        }

                    }
                    if (nextRow.getCell(6) != null && nextRow.getCell(6).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(6).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.GENDER, nextRow.getCell(6).getStringCellValue().trim());
                        } else {
                            errList.add("Invalid value for Gender type. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(7) != null && nextRow.getCell(7).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(7).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.DOB, nextRow.getCell(7).getStringCellValue().trim());
                        } else {
                            errList.add("Invalid value for DOB type. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(8) != null && nextRow.getCell(8).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(8).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.DOMICILE_MEDIUM, nextRow.getCell(8).getStringCellValue().trim());
                        } else {
                            errList.add("Invalid value for Mother tongue type. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(9) != null && nextRow.getCell(9).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(9).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.CATEGORY, nextRow.getCell(9).getStringCellValue().trim());
                        } else {
                            errList.add("Invalid value for category type. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(10) != null && nextRow.getCell(10).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(10).getCellType() == CellType.NUMERIC) {
                            valuesToBeUpdate.put(Constants.PINCODE, NumberToTextConverter.toText(nextRow.getCell(10).getNumericCellValue()));
                        } else if (nextRow.getCell(10).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.PINCODE, nextRow.getCell(10).getStringCellValue().trim());
                        } else {
                            errList.add("Invalid value for pincode type. Expecting number/string format");
                        }
                    }
                    if (nextRow.getCell(11) != null && nextRow.getCell(11).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(11).getCellType() == CellType.NUMERIC) {
                            valuesToBeUpdate.put(Constants.EXTERNAL_SYSTEM_ID, NumberToTextConverter.toText(nextRow.getCell(11).getNumericCellValue()).trim());
                            if (!ValidationUtil.validateExternalSystemId((String)valuesToBeUpdate.get(Constants.EXTERNAL_SYSTEM_ID))) {
                                errList.add("Invalid External System ID : External System Id can contain alphanumeric characters and have a max length of 30");
                            }
                        } else if (nextRow.getCell(11).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.EXTERNAL_SYSTEM_ID, nextRow.getCell(11).getStringCellValue().trim());
                            if (!ValidationUtil.validateExternalSystemId((String)valuesToBeUpdate.get(Constants.EXTERNAL_SYSTEM_ID))) {
                                errList.add("Invalid External System ID : External System Id can contain alphanumeric characters and have a max length of 30");
                            }
                        } else {
                            errList.add("Invalid value for External System ID type. Expecting string/number format");
                        }
                    }
                    if (nextRow.getCell(12) != null && !org.apache.commons.lang.StringUtils.isBlank(nextRow.getCell(12).toString())) {
                        if (nextRow.getCell(12).getCellType() == CellType.STRING) {
                            valuesToBeUpdate.put(Constants.EXTERNAL_SYSTEM, nextRow.getCell(12).getStringCellValue().trim());
                            if (!ValidationUtil.validateExternalSystem((String)valuesToBeUpdate.get(Constants.EXTERNAL_SYSTEM))) {
                                errList.add("Invalid External System : External System can contain only alphabets and can have a max length of 255");
                            }
                        } else {
                            errList.add("Invalid value for External System type. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(13) != null && nextRow.getCell(13).getCellType() != CellType.BLANK) {
                        if (nextRow.getCell(13).getCellType() == CellType.STRING) {
                            String tagStr = nextRow.getCell(13).getStringCellValue().trim();
                            List<String> tagList = new ArrayList<String>();
                            if (!org.apache.commons.lang.StringUtils.isEmpty(tagStr)) {
                                String[] tagStrList = tagStr.split(",", -1);
                                for(String tag : tagStrList) {
                                    tagList.add(tag.trim());
                                }
                            }
                            valuesToBeUpdate.put(Constants.TAG, tagList);
                            if (!ValidationUtil.validateTag((List<String>)valuesToBeUpdate.get(Constants.TAG))) {
                                errList.add("Invalid Tag : Tags are comma seperated string values. A Tag can contain only alphabets with spaces. eg: Bihar Circle, Patna Division");
                            }
                        } else {
                            errList.add("Invalid value for Tags type. Expecting string format");
                        }
                    }
                    if(!CollectionUtils.isEmpty(errList)){
                        this.setErrorDetails(str, errList, statusCell, errorDetails);
                        failedRecordsCount++;
                        totalRecordsCount++;
                        continue;
                    }
                    String userId = null;
                    if(!CollectionUtils.isEmpty(userDetails)){
                        userId = (String) userDetails.get(Constants.USER_ID);
                    }
                    List<WfStatusEntity> userPendingRequest = wfStatusRepo.findByUserIdAndCurrentStatus(userId, true);
                    boolean userRecordUpdate = true;
                    if(!CollectionUtils.isEmpty(userPendingRequest)){
                        for(WfStatusEntity wfStatusEntity : userPendingRequest){
                            String updateValuesString = wfStatusEntity.getUpdateFieldValues();
                            List<Map<String, Object>> updatedValues = mapper.readValue(updateValuesString, List.class);
                            Map<String, String> toValue = (Map<String, String>) updatedValues.get(0).get(Constants.TO_VALUE);
                            for(Map.Entry<String, String> entry : toValue.entrySet()){
                                if(valuesToBeUpdate.containsKey(entry.getKey())){
                                    WfRequest wfRequest = this.getWFRequest(wfStatusEntity, null);
                                    wfStatusEntity.setCurrentStatus(Constants.APPROVED);
                                    wfStatusEntity.setInWorkflow(false);
                                    wfStatusRepo.save(wfStatusEntity);
                                    userProfileWfService.updateUserProfile(wfRequest);
                                    WfStatusEntity wfStatusEntityFailed = wfStatusRepo.findByWfId(wfRequest.getWfId());
                                    if(Constants.REJECTED.equalsIgnoreCase(wfStatusEntityFailed.getCurrentStatus())){
                                        userRecordUpdate = false;
                                        this.setErrorDetails(str, Collections.singletonList(Constants.UPDATE_FAILED), statusCell, errorDetails);
                                    } else{
                                        statusCell.setCellValue(Constants.SUCCESS_UPPERCASE);
                                    }
                                    valuesToBeUpdate.remove(entry.getKey());
                                }
                            }
                        }
                    }
                    if(valuesToBeUpdate.isEmpty()){
                        if(userRecordUpdate)
                            noOfSuccessfulRecords++;
                        else
                            failedRecordsCount++;
                        totalRecordsCount++;
                        continue;
                    }

                    Set<String> employmentDetailsKey = new HashSet<>();
                    employmentDetailsKey.add(Constants.EMPLOYEE_CODE);

                    Set<String> professionalDetailsKey = new HashSet<>();
                    professionalDetailsKey.add(Constants.GROUP);
                    professionalDetailsKey.add(Constants.DESIGNATION);

                    Set<String> personalDetailsKey = new HashSet<>();
                    personalDetailsKey.add(Constants.FIRSTNAME);
                    personalDetailsKey.add(Constants.DOB);
                    personalDetailsKey.add(Constants.DOMICILE_MEDIUM);
                    personalDetailsKey.add(Constants.CATEGORY);
                    personalDetailsKey.add(Constants.PINCODE);
                    personalDetailsKey.add(Constants.GENDER);

                    WfRequest wfRequest = this.getWFRequest(valuesToBeUpdate, userId);
                    List<HashMap<String, Object>> updatedValues = new ArrayList<>();
                    for(Map.Entry<String, Object> entry : valuesToBeUpdate.entrySet()){
                        String fieldKey;
                        HashMap<String, Object> updatedValueMap = new HashMap<>();
                        updatedValueMap.put(entry.getKey(), entry.getValue());
                        HashMap<String, Object> updateValues = new HashMap<>();
                        updateValues.put(Constants.FROM_VALUE, new HashMap<>());
                        updateValues.put(Constants.TO_VALUE, updatedValueMap);
                        if (employmentDetailsKey.contains(entry.getKey())) {
                            fieldKey = Constants.EMPLOYMENT_DETAILS;
                        } else if (professionalDetailsKey.contains(entry.getKey())) {
                            fieldKey = Constants.PROFESSIONAL_DETAILS;
                        } else if (personalDetailsKey.contains(entry.getKey())) {
                            fieldKey = Constants.PERSONAL_DETAILS;
                        } else {
                            fieldKey = Constants.ADDITIONAL_PROPERTIES;
                        }
                        updateValues.put(Constants.FIELD_KEY, fieldKey);
                        updatedValues.add(updateValues);
                        if(null != wfRequest){
                            wfRequest.setUpdateFieldValues(updatedValues);
                        }
                        userProfileWfService.updateUserProfileForBulkUpload(wfRequest);
                        WfStatusEntity wfStatusEntityFailed = wfStatusRepo.findByWfId(wfRequest.getWfId());
                        if(null != wfStatusEntityFailed && Constants.REJECTED.equalsIgnoreCase(wfStatusEntityFailed.getCurrentStatus())){
                            userRecordUpdate = false;
                        }
                    }
                    if(userRecordUpdate){
                        noOfSuccessfulRecords++;
                        errorDetails.setCellValue("NA");
                        statusCell.setCellValue(Constants.SUCCESS_UPPERCASE);
                    } else {
                        failedRecordsCount++;
                        this.setErrorDetails(str, Collections.singletonList(Constants.UPDATE_FAILED), statusCell, errorDetails);
                    }
                    totalRecordsCount++;
                    duration = System.currentTimeMillis() - startTime;
                    logger.info("UserBulkUploadService:: Record Completed. Time taken: {} milli-seconds", duration);
                }
                if (totalRecordsCount == 0) {
                    XSSFRow row = sheet.createRow(sheet.getLastRowNum() + 1);
                    Cell statusCell = row.createCell(14);
                    Cell errorDetails = row.createCell(15);
                    statusCell.setCellValue(Constants.FAILED_UPPERCASE);
                    errorDetails.setCellValue(Constants.EMPTY_FILE_FAILED);
                }
                status = uploadTheUpdatedFile(file, wb);
                if (!(Constants.SUCCESSFUL.equalsIgnoreCase(status) && failedRecordsCount == 0
                        && totalRecordsCount == noOfSuccessfulRecords && totalRecordsCount >= 1)) {
                    status = Constants.FAILED_UPPERCASE;
                }
            } else {
                logger.info("Error in Process Bulk Upload : The File is not downloaded/present");
                status = Constants.FAILED_UPPERCASE;
            }
            this.updateUserBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID), inputDataMap.get(Constants.IDENTIFIER),
                    status, totalRecordsCount, noOfSuccessfulRecords, failedRecordsCount);
        } catch (Exception e) {
            logger.error(String.format("Error in Process Bulk Upload %s", e.getMessage()), e);
            this.updateUserBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID), inputDataMap.get(Constants.IDENTIFIER),
                    Constants.FAILED_UPPERCASE, 0, 0, 0);
        } finally {
            if (wb != null)
                wb.close();
            if (fis != null)
                fis.close();
            if (file != null)
                file.delete();
        }
    }

    private void setErrorDetails(StringBuffer str, List<String> errList, Cell statusCell, Cell errorDetails) {
        str.append("Failed to update user record. Error Cause by - ").append(errList);
        statusCell.setCellValue(Constants.FAILED_UPPERCASE);
        errorDetails.setCellValue(str.toString());
    }

    private String uploadTheUpdatedFile(File file, XSSFWorkbook wb)
            throws IOException {
        FileOutputStream fileOut = new FileOutputStream(file);
        wb.write(fileOut);
        fileOut.close();
        SBApiResponse uploadResponse = storageService.uploadFile(file, configuration.getUserBulkUpdateFolderName(), configuration.getWorkflowCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            String errMsg = String.format("Failed to upload file. Error: %s", uploadResponse.getResult().get(Constants.ERROR_MESSAGE));
            logger.info(errMsg);
            return "FAILED";
        }
        return "SUCCESSFUL";
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
                        userRecordDetails.put(Constants.ROOT_ORG_ID, content.get(Constants.ROOT_ORG_ID));
                    }
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("Exception while fetching user details : ",e);
            throw new ApplicationException("Hub Service ERROR: ", e);
        }
        return false;
    }

    private WfRequest getWFRequest(Object object, String userId) throws IOException {
        WfRequest wfRequest = new WfRequest();
        wfRequest.setAction(Constants.APPROVE_STATE);
        String wfId = UUID.randomUUID().toString();
        if(object instanceof HashMap){
            Map<String, String> userDetails = (Map<String, String>) object;
            wfRequest.setApplicationId(userId);
            wfRequest.setUserId(userDetails.get(Constants.USER_ID));
            wfRequest.setAction(Constants.APPROVE_STATE);
            wfRequest.setRootOrgId("");
            wfRequest.setDeptName("");
            wfRequest.setState(Constants.SEND_FOR_APPROVAL);
            wfRequest.setServiceName(Constants.PROFILE_SERVICE_NAME);
            wfRequest.setActorUserId("");
            wfRequest.setComment("Bulk Update by MDO Admin");
            wfRequest.setWfId(wfId);
            return wfRequest;
        }
        if(object instanceof WfStatusEntity){
            WfStatusEntity wfStatusEntity = (WfStatusEntity) object;
            wfRequest.setWfId(wfStatusEntity.getWfId());
            wfRequest.setComment(wfStatusEntity.getComment());
            wfRequest.setDeptName(wfStatusEntity.getDeptName());
            wfRequest.setRootOrgId(wfStatusEntity.getRootOrg());
            wfRequest.setState(wfStatusEntity.getCurrentStatus());
            wfRequest.setActorUserId(wfStatusEntity.getActorUUID());
            wfRequest.setServiceName(wfStatusEntity.getServiceName());
            wfRequest.setApplicationId(wfStatusEntity.getApplicationId());
            wfRequest.setUpdateFieldValues(mapper.readValue(wfStatusEntity.getUpdateFieldValues(), List.class));
            return wfRequest;
        }
        return null;
    }

}