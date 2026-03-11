package com.pooli.traffic.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.pooli.traffic.domain.entity.TrafficDeductDone;

/**
 * TRAFFIC_DEDUCT_DONE 영속화 전용 MyBatis Mapper입니다.
 * traceId 중복을 허용하지 않는 insert/조회 연산을 제공합니다.
 */
@Mapper
public interface TrafficDeductDoneMapper {

    /**
 * Insert a TrafficDeductDone record while ignoring conflicts on duplicate traceId.
 *
 * @param done the TrafficDeductDone entity to insert (bound to SQL parameter "done")
 * @return the number of rows inserted: 1 if the record was inserted, 0 if the insert was ignored due to a duplicate traceId
 */
int insertIgnore(@Param("done") TrafficDeductDone done);

    /**
 * Checks whether a record with the given traceId exists.
 *
 * @param traceId the trace identifier to look up
 * @return `true` if a matching record exists, `false` otherwise
 */
boolean existsByTraceId(@Param("traceId") String traceId);
}
