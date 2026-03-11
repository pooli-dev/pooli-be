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
 * Retrieve the remaining balance of the individual (personal) pool.
 *
 * @param lineId the identifier of the line (personal account) to query
 * @return the remaining amount in the personal pool, or `null` if no record exists
 */
    Long selectIndividualRemaining(@Param("lineId") Long lineId);

    /**
 * Retrieve the individual's pool remaining balance while acquiring a row-level lock.
 *
 * @param lineId the identifier of the subscriber line whose personal pool is queried
 * @return the remaining amount for the individual's pool, or `null` if no record exists
 */
    Long selectIndividualRemainingForUpdate(@Param("lineId") Long lineId);

    /**
     * Conditionally deducts from an individual (personal) pool's remaining balance for the given line.
     *
     * The deduction is applied only if the current remaining amount is greater than or equal to the specified deductAmount; at most one row is updated.
     *
     * @param lineId      the identifier of the line whose individual pool will be checked and potentially deducted
     * @param deductAmount the amount to subtract from the remaining balance
     * @return             the number of rows updated: `1` if the deduction was applied, `0` otherwise
     */
    int deductIndividualRemaining(
            @Param("lineId") Long lineId,
            @Param("deductAmount") Long deductAmount
    );

    /**
 * Retrieve the remaining balance of the shared pool for the specified family.
 *
 * @param familyId the identifier of the family whose shared pool remaining is queried
 * @return the remaining amount in the shared pool for the given family, or `null` if no record exists
 */
    Long selectSharedRemaining(@Param("familyId") Long familyId);

    /**
 * Retrieve the shared pool remaining amount while acquiring a row-level lock.
 *
 * @param familyId the identifier of the family (shared pool) to query
 * @return the remaining amount for the shared pool, or {@code null} if no record exists
 */
    Long selectSharedRemainingForUpdate(@Param("familyId") Long familyId);

    /**
     * Conditionally deducts an amount from the shared pool remaining balance for a family.
     *
     * Deduction occurs only if the shared pool's remaining amount is greater than or equal to
     * {@code deductAmount}; at most one row will be updated.
     *
     * @param familyId     the identifier of the family whose shared pool will be deducted
     * @param deductAmount the amount to deduct from the shared pool remaining balance
     * @return             the number of rows updated: `1` if the deduction was applied, `0` otherwise
     */
    int deductSharedRemaining(
            @Param("familyId") Long familyId,
            @Param("deductAmount") Long deductAmount
    );
}
