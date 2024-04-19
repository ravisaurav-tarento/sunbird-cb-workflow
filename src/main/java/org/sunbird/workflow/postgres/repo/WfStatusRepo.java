package org.sunbird.workflow.postgres.repo;


import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.sunbird.workflow.postgres.entity.WfStatusEntity;

import java.util.List;

@Repository
@Transactional
public interface WfStatusRepo extends JpaRepository<WfStatusEntity, String> {

    WfStatusEntity findByRootOrgAndOrgAndApplicationIdAndWfId(String rootOrg, String org, String applicationId, String wfId);

    Page<WfStatusEntity> findByRootOrgAndOrgAndServiceNameAndCurrentStatus(String rootOrg, String org, String servicename , String status, Pageable pageable);

    WfStatusEntity findByApplicationIdAndWfId(String applicationId, String wfId);

    WfStatusEntity findByRootOrgAndWfId(String rootOrg, String wfId);

    @Query(value = "select application_id from wingspan.wf_status where root_org= ?1 and service_name = ?2 and current_status = ?3 group by application_id", countQuery = "select count(application_id) from wingspan.wf_status where root_org= ?1 and service_name = ?2 and current_status = ?3 group by application_id", nativeQuery = true)
    List<String> getListOfDistinctApplication(String rootOrg, String serviceName, String currentStatus, Pageable pageable);
    
    @Query(value = "select application_id from wingspan.wf_status where root_org= ?1 and service_name = ?2 and current_status = ?3 and dept_name = ?4 group by application_id", countQuery = "select count(application_id) from wingspan.wf_status where root_org= ?1 and service_name = ?2 and current_status = ?3 group by application_id", nativeQuery = true)
    List<String> getListOfDistinctApplicationUsingDept(String rootOrg, String serviceName, String currentStatus, String deptName, Pageable pageable);
    
    List<WfStatusEntity> findByServiceNameAndCurrentStatusAndApplicationIdIn(String serviceName, String currentStatus, List<String> applicationId);
    
    List<WfStatusEntity> findByServiceNameAndCurrentStatusAndDeptNameAndApplicationIdIn(String serviceName, String currentStatus, String deptName, List<String> applicationId);

    List<WfStatusEntity> findByRootOrgAndOrgAndServiceNameAndCurrentStatusAndUserId(String rootOrg, String org, String servicename , String status, String userId);
    
    @Query(value = "select update_field_values, wf_id from wingspan.wf_status where root_org= ?1 and org = ?2 and service_name = ?3 and current_status = ?4 and userid = ?5", nativeQuery = true)
    List<Object[]> findWfFieldsForUser(String servicename , String status, String userId);

    @Query(value = "select * from wingspan.wf_status where service_name= ?1 and current_status = ?2 and dept_Name = ?3", nativeQuery = true)
    List<WfStatusEntity> findByServiceNameAndCurrentStatusAndDeptName(String serviceName, String currentStatus, String deptName);

    @Query(value = "select * from wingspan.wf_status where service_name IN ?1 and userid = ?2 and application_id IN ?3", nativeQuery = true)
    List<WfStatusEntity> findByServiceNameAndUserIdAndApplicationIdIn(List<String> servicesName, String userId, List<String> applicationIds);

    @Query(value = "select * from wingspan.wf_status where service_name IN ?1 and current_status = ?2 and application_id IN ?3", nativeQuery = true)
    List<WfStatusEntity> findByServiceNameAndCurrentStatusAndApplicationId(List<String> serviceNames, String currentStatus, List<String> applicationIds);

    @Query(value = "select * from wingspan.wf_status where service_name IN ?1 and current_status = ?2 and dept_name = ?4 and application_id IN ?3", nativeQuery = true)
    List<WfStatusEntity> findByServiceNameAndCurrentStatusAndDeptNameAndApplicationId(List<String> serviceNames, String currentStatus, List<String> applicationIds,String deptName);

    WfStatusEntity findByWfId(String wfId);


    List<WfStatusEntity> findByServiceNameAndApplicationId(String serviceName, String applicationId);

    List<WfStatusEntity>  findByApplicationId(String applicationId);

    /**
     * Query to fetch the data from the wf_status based on the service name, userId and the applicationId/BatchId
     *
     * @param serviceName   -Blended Program.
     * @param userId        - user id of the learner to be enrolled in the Blended program.
     * @param applicationId - batch id of the blended program to be enrolled.
     * @return - Return the data if the user is already enrolled or not using the wf_status table.
     */
    @Query(value = "select * from wingspan.wf_status where service_name= ?1 and userid = ?2 and application_id = ?3", nativeQuery = true)
    List<WfStatusEntity> findByServiceNameAndUserIdAndApplicationId(String serviceName, String userId, String applicationId);

    @Query(value = "select * from wingspan.wf_status where application_id = ?1 and  userid= ?2 and current_status=?3", nativeQuery = true)
    List<WfStatusEntity> findByApplicationIdAndUserIdAndCurrentStatus(String applicationId, String userId ,String currentStatus);

    @Query(value = "SELECT current_status AS currentStatus, COUNT(*) AS statusCount FROM wingspan.wf_status WHERE application_id IN ?1 GROUP BY current_status" , nativeQuery = true )
    List<Object[]> findStatusCountByApplicationId(List<String> applicationIds);


    @Query(value = "SELECT * FROM wingspan.wf_status WHERE " +
            "current_status IN (?1) AND " +
            "application_id IN (?2) AND " +
            "dept_name = (?3)", nativeQuery = true)
    List<WfStatusEntity> findByStatusAndDeptAndAppIds(List<String> currentStatus, List<String> applicationIds, List<String> deptName);

    @Query(value = "SELECT * FROM wingspan.wf_status WHERE current_status IN (?1) AND application_id IN (?2) ", nativeQuery = true)
    List<WfStatusEntity> findByStatusAndAppIds(List<String> currentStatus, List<String> applicationIds);

    @Query(value = "select distinct application_id from wingspan.wf_status where service_name = ?1 and current_status = ?2 and dept_name = ?3", nativeQuery = true)
    Page<String> getListOfDistinctApplicationUsingDept(String serviceName, String currentStatus, String deptName, Pageable pageable);

    @Modifying
    @Query(value = "update wingspan.wf_status set dept_name= ?4 where application_id= ?1 and service_name=?2 and current_status= ?3", nativeQuery = true)
    Integer updatePendingRequestsToNewMDO(String userId, String serviceName, String currentStatus, String newDeptName);

    @Query(value = "select * from wingspan.wf_status  where application_id= ?1 and service_name=?2 and current_status= ?3", nativeQuery = true)
    List<WfStatusEntity> getPendingRequests(String userId, String serviceName, String currentStatus);

    @Query(value = "select * from wingspan.wf_status  where userid = ?1 and in_workflow=?2", nativeQuery = true)
    List<WfStatusEntity> findByUserIdAndCurrentStatus(String userId, boolean currentStatus);
}

