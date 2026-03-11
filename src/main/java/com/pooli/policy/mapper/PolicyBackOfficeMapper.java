package com.pooli.policy.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.pooli.policy.domain.dto.response.ActivePolicyResDto;
import com.pooli.policy.domain.dto.response.PolicyActivationSnapshotResDto;

@Mapper
public interface PolicyBackOfficeMapper {
	
	/**
 * Retrieve the list of policies activated in the back office.
 *
 * @return a List of ActivePolicyResDto representing policies activated in the back office; an empty list if none are found
 */
    List<ActivePolicyResDto> selectActivePolicies();
    
	/**
 * Retrieve the activated policy for the given policyId from the back office.
 *
 * @param policyId the identifier of the policy to retrieve
 * @return the activated policy matching the given policyId, or {@code null} if none is found
 */
    ActivePolicyResDto selectActivePolicy(@Param("policyId") Integer policyId);

    /**
 * Retrieve a snapshot of policy activation state used for global bootstrap and reconciliation.
 *
 * @return a list of PolicyActivationSnapshotResDto objects representing activation snapshots; empty list if no snapshots are available
 */
    List<PolicyActivationSnapshotResDto> selectPolicyActivationSnapshot();
	    
}
