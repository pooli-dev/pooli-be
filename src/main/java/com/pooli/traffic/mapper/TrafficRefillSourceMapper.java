package com.pooli.traffic.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * DB 원천 잔량 리필 차감을 위한 MyBatis Mapper입니다.
 * 개인풀/공유풀 row lock 조회와 조건부 차감 연산을 제공합니다.
 */
@Mapper
public interface TrafficRefillSourceMapper {

    /**
     * 개인풀 원천 잔량을 조회합니다.
     */
    Long selectIndividualRemaining(@Param("lineId") Long lineId);

    /**
     * 개인 회선의 요금제 QoS 속도 제한 값을 조회합니다.
     */
    Long selectIndividualQosSpeedLimit(@Param("lineId") Long lineId);

    /**
     * 개인풀 원천 잔량을 row lock과 함께 조회합니다.
     */
    Long selectIndividualRemainingForUpdate(@Param("lineId") Long lineId);

    /**
     * 개인풀 원천 잔량을 조건부 차감합니다.
     * remaining_data >= deductAmount 조건을 만족할 때만 1건 갱신됩니다.
     */
    int deductIndividualRemaining(
            @Param("lineId") Long lineId,
            @Param("deductAmount") Long deductAmount
    );

    /**
     * 개인풀 원천 잔량을 반납합니다.
     */
    int restoreIndividualRemaining(
            @Param("lineId") Long lineId,
            @Param("restoreAmount") Long restoreAmount
    );

    /**
     * 공유풀 원천 잔량을 조회합니다.
     */
    Long selectSharedRemaining(@Param("familyId") Long familyId);

    /**
     * 공유풀 원천 잔량을 row lock과 함께 조회합니다.
     */
    Long selectSharedRemainingForUpdate(@Param("familyId") Long familyId);

    /**
     * 공유풀 원천 잔량을 조건부 차감합니다.
     * pool_remaining_data >= deductAmount 조건을 만족할 때만 1건 갱신됩니다.
     */
    int deductSharedRemaining(
            @Param("familyId") Long familyId,
            @Param("deductAmount") Long deductAmount
    );

    /**
     * 공유풀 원천 잔량을 반납합니다.
     */
    int restoreSharedRemaining(
            @Param("familyId") Long familyId,
            @Param("restoreAmount") Long restoreAmount
    );
}
