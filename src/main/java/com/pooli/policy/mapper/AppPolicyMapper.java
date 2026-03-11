package com.pooli.policy.mapper;

import com.pooli.policy.domain.dto.request.AppPolicySearchCondReqDto;
import com.pooli.policy.domain.dto.response.AppPolicyResDto;
import com.pooli.policy.domain.entity.AppPolicy;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Optional;

@Mapper
public interface AppPolicyMapper {
    /*
    =================== SELECT ====================
     */
    // lineId, appId로 삭제되지 않은 레코드 조회(조회 결과: entity)
    Optional<AppPolicy> findEntityExistByLineIdAndAppId(@Param("lineId") Long lineId, @Param("appId") Integer appId);
    // lineId, appId로 삭제되지 않은 레코드 조회(조회 결과: dto)
    Optional<AppPolicyResDto> findDtoExistByLineIdAndAppId(@Param("lineId") Long lineId, @Param("appId") Integer appId);
    // pk 로 Dto 조회
    Optional<AppPolicyResDto> findDtoExistById(Long appPolicyId);
    // pk 로 Entity 조회
    Optional<AppPolicy> findEntityExistById(Long appPolicyId);
    /**
 * Retrieve app policies that match the dynamic search criteria.
 *
 * @param request search filters and pagination settings used to select app policies
 * @return a list of AppPolicyResDto objects matching the provided search conditions
 */
    List<AppPolicyResDto> findApplicationsWithPolicy(AppPolicySearchCondReqDto request);
    /**
 * Count application policies that match the provided search conditions.
 *
 * @param request search criteria used to filter application policies
 * @return total number of application policies matching the given conditions
 */
    Long countApplicationsWithPolicy(AppPolicySearchCondReqDto request);
    /**
 * Retrieve all non-deleted AppPolicy entities for a given line identifier, used for traffic hydration snapshots.
 *
 * @param lineId the line identifier to filter policies by
 * @return a list of AppPolicy entities with the given lineId that are not marked deleted; an empty list if none exist
 */
    List<AppPolicy> findAllEntityByLineId(@Param("lineId") Long lineId);


    /*
    =================== UPDATE ====================
     */
    // pk로 삭제되지 않은 레코드의 is_active 값 update
    int updateIsActive(@Param("appPolicyId") Long appPolicyId, @Param("isActive") Boolean isActive);

    // pk로 삭제되지 않은 레코드의 is_whitelist 값 update
    int updateIsWhitelist(@Param("appPolicyId") Long appPolicyId, @Param("isWhitelist") Boolean isWhitelist);
    // pk로 삭제되지 않은 레코드의 data_limit 값 update
    int updateDataLimit(@Param("appPolicyId") Long appPolicyId, @Param("value") Long value);
    // pk로 삭제되지 않은 레코드의 speed_limit 값 update
    int updateSpeedLimit(@Param("appPolicyId") Long appPolicyId, @Param("value") Integer value);

    /*
    =================== DELETE ====================
     */
    // pk로 레코드 삭제처리
    int setDeleted(Long appPolicyId);

    /*
    =================== INSERT ====================
     */
    // 새 레코드 생성
    int insertAppPolicy(AppPolicy appPolicy);
}
