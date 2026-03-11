package com.pooli.policy.service;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

import com.pooli.notification.domain.enums.AlarmCode;
import com.pooli.notification.domain.enums.AlarmType;
import com.pooli.notification.service.AlarmHistoryService;
import com.pooli.traffic.service.TrafficPolicyWriteThroughService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.pooli.auth.service.AuthUserDetails;
import com.pooli.common.dto.PagingResDto;
import com.pooli.common.exception.ApplicationException;
import com.pooli.common.exception.CommonErrorCode;
import com.pooli.family.mapper.FamilyMapper;
import com.pooli.notification.domain.enums.AlarmCode;
import com.pooli.notification.domain.enums.AlarmType;
import com.pooli.notification.service.AlarmHistoryService;
import com.pooli.permission.mapper.FamilyLineMapper;
import com.pooli.policy.domain.dto.request.*;
import com.pooli.policy.domain.entity.AppPolicy;
import com.pooli.policy.domain.entity.LineLimit;
import com.pooli.policy.domain.dto.request.AppDataLimitUpdateReqDto;
import com.pooli.policy.domain.dto.request.AppPolicyActiveToggleReqDto;
import com.pooli.policy.domain.dto.request.AppPolicySearchCondReqDto;
import com.pooli.policy.domain.dto.request.AppSpeedLimitUpdateReqDto;
import com.pooli.policy.domain.dto.request.ImmediateBlockReqDto;
import com.pooli.policy.domain.dto.request.LimitPolicyUpdateReqDto;
import com.pooli.policy.domain.dto.request.RepeatBlockDayReqDto;
import com.pooli.policy.domain.dto.request.RepeatBlockPolicyReqDto;
import com.pooli.policy.domain.dto.response.ActivePolicyResDto;
import com.pooli.policy.domain.dto.response.AppPolicyResDto;
import com.pooli.policy.domain.dto.response.AppliedPolicyResDto;
import com.pooli.policy.domain.dto.response.BlockStatusResDto;
import com.pooli.policy.domain.dto.response.ImmediateBlockResDto;
import com.pooli.policy.domain.dto.response.LimitPolicyResDto;
import com.pooli.policy.domain.dto.response.RepeatBlockDayResDto;
import com.pooli.policy.domain.dto.response.RepeatBlockPolicyResDto;
import com.pooli.policy.domain.entity.AppPolicy;
import com.pooli.policy.domain.entity.LineLimit;
import com.pooli.policy.domain.enums.DayOfWeek;
import com.pooli.policy.domain.enums.PolicyScope;
import com.pooli.policy.domain.enums.SortType;
import com.pooli.policy.exception.PolicyErrorCode;
import com.pooli.policy.mapper.AppPolicyMapper;
import com.pooli.policy.mapper.LineLimitMapper;
import com.pooli.policy.mapper.ImmediateBlockMapper;
import com.pooli.policy.mapper.LineLimitMapper;
import com.pooli.policy.mapper.PolicyBackOfficeMapper;
import com.pooli.policy.mapper.RepeatBlockDayMapper;
import com.pooli.policy.mapper.RepeatBlockMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserPolicyServiceImpl implements UserPolicyService {

    private final AlarmHistoryService alarmHistoryService;

    private final FamilyLineMapper familyLineMapper;
    private final LineLimitMapper lineLimitMapper;
    private final AppPolicyMapper appPolicyMapper;
    private final FamilyMapper familyMapper;

    private final PolicyBackOfficeMapper policyBackOfficeMapper;
    private final RepeatBlockMapper repeatBlockMapper;
    private final RepeatBlockDayMapper repeatBlockDayMapper;
    private final ImmediateBlockMapper immediateBlockMapper;
    private final ObjectProvider<TrafficPolicyWriteThroughService> trafficPolicyWriteThroughServiceProvider;

    /**
     * Fetches the list of currently active policies.
     *
     * @return a list of ActivePolicyResDto representing active policies; an empty list if no active policies exist
     */
    @Override
    public List<ActivePolicyResDto> getActivePolicies() {

        return policyBackOfficeMapper.selectActivePolicies();
    }

    @Override
    public List<RepeatBlockPolicyResDto> getRepeatBlockPolicies(Long lineId, AuthUserDetails auth) {
        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);

        return repeatBlockMapper.selectRepeatBlocksByLineId(lineId);
    }

    /**
     * Create a new repeat-block policy for a line and return the created policy.
     *
     * <p>If request.lineId is null, the caller's lineId from {@code auth} is used. The method
     * validates family-group ownership, rejects duplicate repeat-block periods for the same line,
     * persists the repeat block and its days, synchronizes the updated repeat-block list to the
     * write-through store, and emits a creation alarm.</p>
     *
     * @param request DTO containing repeat-block fields (lineId optional) and an optional list of day/time rules
     * @param auth    caller identity used to derive default lineId and to authorize the operation
     * @return the created RepeatBlockPolicyResDto containing the new repeatBlockId, lineId, active flag, and day entries
     * @throws ApplicationException if a duplicate repeat-block period exists for the line (PolicyErrorCode.BLOCK_POLICY_CONFLICT)
     */
    @Override
    @Transactional
    public RepeatBlockPolicyResDto createRepeatBlockPolicy(RepeatBlockPolicyReqDto request, AuthUserDetails auth) {

        Long lineId = request.getLineId() != null ? request.getLineId() : auth.getLineId();
        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);
        request.setLineId(lineId);

        List<RepeatBlockDayReqDto> days = request.getDays();

        // 반복적 차단 요일/시간 중복 체크
        if (days != null && !days.isEmpty()) {
            for (RepeatBlockDayReqDto day : days) {
                boolean exists = repeatBlockMapper.isDuplicatedRepeatBlocks(
                        lineId,
                        day.getDayOfWeek(),
                        day.getStartAt(),
                        day.getEndAt());
                if (exists) {
                    throw new ApplicationException(PolicyErrorCode.BLOCK_POLICY_CONFLICT);
                }
            }

        }

        // 새로운 차단이면 DB 삽입
        repeatBlockMapper.insertRepeatBlock(request);

        if (days != null && !days.isEmpty()) {
            repeatBlockDayMapper.insertRepeatBlockDays(request.getRepeatBlockId(), days);
        }

        // 반복 차단 정책은 line 단위 hash 전체 스냅샷으로 관리하므로
        // 변경 직후 현재 활성 목록을 재조회해 Redis를 한 번에 동기화한다.
        List<RepeatBlockPolicyResDto> latestRepeatBlocks = repeatBlockMapper.selectRepeatBlocksByLineId(lineId);
        applyWriteThrough(
                "repeat_block_create lineId=" + lineId,
                writeThroughService -> writeThroughService.syncRepeatBlock(lineId, latestRepeatBlocks)
        );

        alarmHistoryService.createAlarm(lineId, AlarmCode.POLICY_LIMIT, AlarmType.CREATE_REPEAT_BLOCK);

        // DTO 반환
        List<RepeatBlockDayResDto> dayResList = days != null
                ? days.stream()
                        .map(d -> RepeatBlockDayResDto.builder()
                                .dayOfWeek(d.getDayOfWeek())
                                .startAt(d.getStartAt())
                                .endAt(d.getEndAt())
                                .build())
                        .toList()
                : List.of();

        return RepeatBlockPolicyResDto.builder()
                .repeatBlockId(request.getRepeatBlockId())
                .lineId(lineId)
                .isActive(request.getIsActive())
                .days(dayResList)
                .build();
    }

    @Override
    @Transactional
    public RepeatBlockPolicyResDto updateRepeatBlockPolicy(Long repeatBlockId, RepeatBlockPolicyReqDto request,
            AuthUserDetails auth) {
        RepeatBlockPolicyResDto exist = repeatBlockMapper.selectRepeatBlockById(repeatBlockId);
        if (exist == null) {
            throw new ApplicationException(PolicyErrorCode.REPEAT_BLOCK_NOT_FOUND);
        }
        checkIsSameFamilyGroup(exist.getLineId(), auth.getLineId(), auth);
        request.setLineId(exist.getLineId());
        request.setRepeatBlockId(repeatBlockId);

        // 반복 차단 요일 및 시간대 정보를 삭제한 후 새로 삽입하기
        deleteRepeatBlockPolicy(repeatBlockId, auth);
        RepeatBlockPolicyResDto updated = createRepeatBlockPolicy(request, auth);
        alarmHistoryService.createAlarm(exist.getLineId(), AlarmCode.POLICY_CHANGE, AlarmType.UPDATE_REPEAT_BLOCK);
        return updated;
    }

    /**
     * Deletes the repeat block policy identified by {@code repeatBlockId} and updates external stores and alarms accordingly.
     *
     * @param repeatBlockId the identifier of the repeat block policy to delete
     * @param auth the authenticated user performing the deletion
     * @throws ApplicationException if the repeat block is not found (PolicyErrorCode.REPEAT_BLOCK_NOT_FOUND) or the user is not authorized for the line
     * @return a {@code RepeatBlockPolicyResDto} representing the deleted block with {@code isActive} set to {@code false}
     */
    @Override
    public RepeatBlockPolicyResDto deleteRepeatBlockPolicy(Long repeatBlockId, AuthUserDetails auth) {

        // 반복적 차단 정보 없음
        RepeatBlockPolicyResDto exist = repeatBlockMapper.selectRepeatBlockById(repeatBlockId);

        if (exist == null) {
            throw new ApplicationException(PolicyErrorCode.REPEAT_BLOCK_NOT_FOUND);
        }
        checkIsSameFamilyGroup(exist.getLineId(), auth.getLineId(), auth);

        // soft delete
        repeatBlockMapper.deleteRepeatBlock(repeatBlockId);
        repeatBlockDayMapper.deleteRepeatDayBlock(repeatBlockId);

        // 삭제 후 활성 반복 차단 목록을 다시 읽어 Redis hash를 갱신한다.
        List<RepeatBlockPolicyResDto> latestRepeatBlocks = repeatBlockMapper.selectRepeatBlocksByLineId(exist.getLineId());
        applyWriteThrough(
                "repeat_block_delete lineId=" + exist.getLineId(),
                writeThroughService -> writeThroughService.syncRepeatBlock(exist.getLineId(), latestRepeatBlocks)
        );

        alarmHistoryService.createAlarm(exist.getLineId(), AlarmCode.POLICY_LIMIT, AlarmType.DELETE_REPEAT_BLOCK);

        return RepeatBlockPolicyResDto.builder()
                .repeatBlockId(repeatBlockId)
                .lineId(auth.getLineId())
                .isActive(false)
                .build();
    }

    @Override
    public BlockStatusResDto getBlockStatus(Long lineId, AuthUserDetails auth) {

        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);

        LocalDateTime now = LocalDateTime.now();
        LocalTime nowTime = now.toLocalTime();

        DayOfWeek todayDow = DayOfWeek.values()[now.getDayOfWeek().getValue() % 7];

        DayOfWeek yesterdayDow = DayOfWeek.values()[(now.getDayOfWeek().getValue() + 6) % 7];

        LocalDateTime latestEnd = null;

        // 즉시 차단 야부 조회
        ImmediateBlockResDto immBlock = immediateBlockMapper.selectImmediateBlockPolicy(lineId);

        if (immBlock != null &&
                immBlock.getBlockEndAt() != null &&
                immBlock.getBlockEndAt().isAfter(now)) {

            latestEnd = immBlock.getBlockEndAt();
        }

        // 반복 차단 여부 조회
        List<RepeatBlockPolicyResDto> repeatBlocks = repeatBlockMapper.selectRepeatBlocksByLineId(lineId);

        for (RepeatBlockPolicyResDto block : repeatBlocks) {

            if (!Boolean.TRUE.equals(block.getIsActive()) || block.getDays() == null) {
                continue;
            }

            for (RepeatBlockDayResDto day : block.getDays()) {

                LocalTime start = day.getStartAt();
                LocalTime end = day.getEndAt();

                boolean overnight = start.isAfter(end);

                boolean inRange = false;
                LocalDateTime repeatEnd = null;

                if (!overnight) {
                    // 같은날 block (ex: 11:00 ~ 12:00)

                    if (day.getDayOfWeek() == todayDow &&
                            !nowTime.isBefore(start) &&
                            !nowTime.isAfter(end)) {

                        inRange = true;
                        repeatEnd = now.toLocalDate().atTime(end);
                    }

                } else {
                    // 자정 넘어가는 block (ex: 20:00 ~ 02:00)

                    if (day.getDayOfWeek() == todayDow &&
                            !nowTime.isBefore(start)) {

                        inRange = true;
                        repeatEnd = now.toLocalDate().plusDays(1).atTime(end);
                    }

                    if (day.getDayOfWeek() == yesterdayDow &&
                            !nowTime.isAfter(end)) {

                        inRange = true;
                        repeatEnd = now.toLocalDate().atTime(end);
                    }
                }

                if (inRange) {

                    if (latestEnd == null || repeatEnd.isAfter(latestEnd)) {
                        latestEnd = repeatEnd;
                    }
                }
            }
        }

        if (latestEnd != null) {
            return BlockStatusResDto.builder()
                    .isBlocked(true)
                    .blockEndsAt(latestEnd)
                    .build();
        }

        return BlockStatusResDto.builder()
                .isBlocked(false)
                .blockEndsAt(null)
                .build();
    }

    @Override
    public ImmediateBlockResDto getImmediateBlockPolicy(Long lineId, AuthUserDetails auth) {
        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);

        ImmediateBlockResDto immBlock = immediateBlockMapper.selectImmediateBlockPolicy(lineId);

        // 즉시 차단 정보가 없을 때
        if (immBlock == null) {
            return ImmediateBlockResDto.builder()
                    .lineId(lineId)
                    .blockEndAt(null)
                    .build();
        }

        return ImmediateBlockResDto.builder()
                .lineId(lineId)
                .blockEndAt(immBlock.getBlockEndAt())
                .build();

    }

    /**
     * Update the immediate block end time for the specified line, enforcing family-group ownership.
     *
     * @param lineId the identifier of the line whose immediate block is being updated
     * @param request contains the new block end timestamp to apply (request.getBlockEndAt())
     * @param auth    caller's authentication details used to verify access within the same family group
     * @return an ImmediateBlockResDto containing the lineId and the updated blockEndAt
     */
    @Override
    public ImmediateBlockResDto updateImmediateBlockPolicy(Long lineId, ImmediateBlockReqDto request,
            AuthUserDetails auth) {
        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);

        immediateBlockMapper.updateImmediateBlockPolicy(lineId, request);

        // 즉시 차단 종료 시각 변경은 단일 키 갱신으로 즉시 반영한다.
        applyWriteThrough(
                "immediate_block_update lineId=" + lineId,
                writeThroughService -> writeThroughService.syncImmediateBlockEnd(lineId, request.getBlockEndAt())
        );

        alarmHistoryService.createAlarm(lineId, AlarmCode.POLICY_LIMIT, AlarmType.UPDATE_IMMEDIATE_BLOCK);

        return ImmediateBlockResDto.builder()
                .lineId(lineId)
                .blockEndAt(request.getBlockEndAt())
                .build();

    }

    /**
     * Retrieves the limit policy for the given line and returns its current limit values.
     *
     * Validates that the requester belongs to the same family group as the target line. If a LineLimit record exists, the returned DTO contains the persisted limit fields and active flags; if no record exists, the DTO contains only maximum shared and daily data values. A stored daily limit value of -1 is normalized to 100.
     *
     * @param lineId the identifier of the line whose limit policy is requested
     * @param auth   the requesting user's details used to validate family-group membership
     * @return       a LimitPolicyResDto containing line limit fields when present; otherwise contains only maxSharedData and maxDailyData
     */
    @Override
    @Transactional(readOnly = true)
    public LimitPolicyResDto getLimitPolicy(Long lineId, AuthUserDetails auth) {
        // 1. 조회 대상 lineId가 속한 가족그룹과, 요청을 보낸 사용자의 가족 그룹이 일치하는지 검증
        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);

        // 2. 두 종류의 제한 정보 테이블에서 정보 조회
        Optional<LineLimit> lineLimit = lineLimitMapper.getExistLineLimitByLineId(lineId);
        Long maxSharedData = familyMapper.selectPoolBaseDataByLineId(lineId);
        Long maxDailyData = lineLimitMapper.selectPlanDataLimitByLineId(lineId);

        if (maxDailyData != null && maxDailyData == -1L) {
        	maxDailyData = 100L;
        }

        if (lineLimit.isPresent()) {
            return LimitPolicyResDto.builder()
                    .lineLimitId(lineLimit.get().getLimitId())
                    .dailyDataLimit(lineLimit.get().getDailyDataLimit())
                    .isDailyDataLimitActive(lineLimit.get().getIsDailyLimitActive())
                    .sharedDataLimit(lineLimit.get().getSharedDataLimit())
                    .isSharedDataLimitActive(lineLimit.get().getIsSharedLimitActive())
                    .maxSharedData(maxSharedData)
                    .maxDailyData(maxDailyData)
                    .build();
        } else {
            return LimitPolicyResDto.builder()
                    .maxSharedData(maxSharedData)
                    .maxDailyData(maxDailyData)
                    .build();
        }
    }

    /**
     * Toggle the active state of the daily data limit for the specified line.
     *
     * If a LineLimit exists, flips its daily-limit active flag, emits the corresponding alarm,
     * synchronizes the updated limits to the write-through store, and returns the updated policy.
     * If no LineLimit exists, creates a new default LineLimit and returns it.
     *
     * @param lineId the identifier of the line whose daily limit should be toggled
     * @param auth   the authenticated user performing the operation (used for authorization)
     * @return a LimitPolicyResDto reflecting the line's daily and shared limits and their active states after the operation
     */
    @Override
    @Transactional
    public LimitPolicyResDto toggleDailyTotalLimitPolicy(Long lineId, AuthUserDetails auth) {
        // 1. 조회 대상 lineId가 속한 가족그룹과, 요청을 보낸 사용자의 가족 그룹이 일치하는지 검증
        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);

        // 2. 제한 정책 존재 여부 조회
        Optional<LineLimit> lineLimit = lineLimitMapper.getExistLineLimitByLineId(lineId);
        if (lineLimit.isPresent()) {
            // 삭제 상태가 아닌 lineLimit 레코드가 존재하는 경우
            boolean newDailyLimitActive = !lineLimit.get().getIsDailyLimitActive();
            int def = lineLimitMapper.updateIsDailyLimitActiveById(lineLimit.get().getLimitId(), newDailyLimitActive);
            if (def != 1) {
                throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
            }
            if (newDailyLimitActive) {
                alarmHistoryService.createAlarm(lineId, AlarmCode.POLICY_LIMIT, AlarmType.POLICY_CREATE_DAYDATA_LIMIT);
            } else {
                alarmHistoryService.createAlarm(lineId, AlarmCode.POLICY_LIMIT, AlarmType.POLICY_DELETE_DAYDATA_LIMIT);
            }

            // line_limit 변경은 daily/shared를 항상 함께 동기화해 키 불일치를 방지한다.
            applyWriteThrough(
                    "line_limit_toggle_daily lineId=" + lineId,
                    writeThroughService -> writeThroughService.syncLineLimit(
                            lineId,
                            lineLimit.get().getDailyDataLimit(),
                            newDailyLimitActive,
                            lineLimit.get().getSharedDataLimit(),
                            lineLimit.get().getIsSharedLimitActive()
                    )
            );

            return LimitPolicyResDto.builder()
                    .lineLimitId(lineLimit.get().getLimitId())
                    .dailyDataLimit(lineLimit.get().getDailyDataLimit())
                    .isDailyDataLimitActive(newDailyLimitActive)
                    .sharedDataLimit(lineLimit.get().getSharedDataLimit())
                    .isSharedDataLimitActive(lineLimit.get().getIsSharedLimitActive())
                    .build();
        } else {
            // lineLimit 레코드가 없거나, 삭제 상태인 경우 새 레코드 insert
            return insertNewLineLimit(lineId);
        }
    }

    /**
     * Update the daily data limit value for an existing line limit policy.
     *
     * @param request contains the target limitPolicyId and the new daily limit value
     * @param auth    caller identity used to verify family-group ownership
     * @return a LimitPolicyResDto reflecting the updated dailyDataLimit, the active flags, and the shared limit values
     * @throws ApplicationException with PolicyErrorCode.LIMIT_POLICY_NOT_FOUND if the specified limitPolicyId does not exist
     * @throws ApplicationException with PolicyErrorCode.LINE_OWNERSHIP_FORBIDDEN if the caller is not authorized for the target line
     * @throws ApplicationException with CommonErrorCode.DATABASE_ERROR if the update failed to persist
     */
    @Override
    @Transactional
    public LimitPolicyResDto updateDailyTotalLimitPolicyValue(LimitPolicyUpdateReqDto request, AuthUserDetails auth) {
        // 1. 대상 dailyLimit 레코드 조회
        Optional<LineLimit> lineLimit = lineLimitMapper.getExistLineLimitById(request.getLimitPolicyId());
        if (lineLimit.isEmpty()) {
            throw new ApplicationException(PolicyErrorCode.LIMIT_POLICY_NOT_FOUND);
        }

        // 2. 대상 lineId가 동일 가족에 속했는지 검증
        checkIsSameFamilyGroup(lineLimit.get().getLineId(), auth.getLineId(), auth);

        // 3. update 진행
        int def = lineLimitMapper.updateDailyDataLimit(request);
        if (def != 1) {
            throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
        }

        alarmHistoryService.createAlarm(lineLimit.get().getLineId(), AlarmCode.POLICY_CHANGE,
                AlarmType.POLICY_UPDATE_DAYDATA_LIMIT);

        applyWriteThrough(
                "line_limit_update_daily_value lineId=" + lineLimit.get().getLineId(),
                writeThroughService -> writeThroughService.syncLineLimit(
                        lineLimit.get().getLineId(),
                        request.getPolicyValue(),
                        lineLimit.get().getIsDailyLimitActive(),
                        lineLimit.get().getSharedDataLimit(),
                        lineLimit.get().getIsSharedLimitActive()
                )
        );

        return LimitPolicyResDto.builder()
                .lineLimitId(lineLimit.get().getLimitId())
                .dailyDataLimit(request.getPolicyValue())
                .isDailyDataLimitActive(lineLimit.get().getIsDailyLimitActive())
                .sharedDataLimit(lineLimit.get().getSharedDataLimit())
                .isSharedDataLimitActive(lineLimit.get().getIsSharedLimitActive())
                .build();
    }

    /**
     * Toggles the shared-pool (shared data) limit active state for the specified line.
     *
     * If a LineLimit exists for the line, flips its shared-limit active flag, emits an alarm
     * indicating creation or deletion of the shared-data limit, synchronizes the updated
     * limit to the write-through service, and returns the updated policy DTO. If no LineLimit
     * exists or it is deleted, a new LineLimit is created and returned.
     *
     * @param lineId the identifier of the line whose shared-pool limit should be toggled
     * @param auth   the authenticated user performing the operation (used for ownership checks)
     * @return       a LimitPolicyResDto containing the line's daily/shared limits and their active flags
     * @throws ApplicationException if a database update fails
     */
    @Override
    @Transactional
    public LimitPolicyResDto toggleSharedPoolLimitPolicy(Long lineId, AuthUserDetails auth) {
        // 1. 조회 대상 lineId가 속한 가족그룹과, 요청을 보낸 사용자의 가족 그룹이 일치하는지 검증
        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);

        // 2. 제한 정책 존재 여부 조회
        Optional<LineLimit> lineLimit = lineLimitMapper.getExistLineLimitByLineId(lineId);
        if (lineLimit.isPresent()) {
            // 삭제 상태가 아닌 lineLimit 레코드가 존재하는 경우
            boolean newSharedLimitActive = !lineLimit.get().getIsSharedLimitActive();
            int def = lineLimitMapper.updateIsSharedLimitActiveById(lineLimit.get().getLimitId(), newSharedLimitActive);
            if (def != 1) {
                throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
            }
            if (newSharedLimitActive) {
                alarmHistoryService.createAlarm(lineLimit.get().getLineId(), AlarmCode.POLICY_LIMIT,
                        AlarmType.POLICY_CREATE_SHAREDATA_LIMIT);
            } else {
                alarmHistoryService.createAlarm(lineLimit.get().getLineId(), AlarmCode.POLICY_LIMIT,
                        AlarmType.POLICY_DELETE_SHAREDATA_LIMIT);
            }


            applyWriteThrough(
                    "line_limit_toggle_shared lineId=" + lineLimit.get().getLineId(),
                    writeThroughService -> writeThroughService.syncLineLimit(
                            lineLimit.get().getLineId(),
                            lineLimit.get().getDailyDataLimit(),
                            lineLimit.get().getIsDailyLimitActive(),
                            lineLimit.get().getSharedDataLimit(),
                            newSharedLimitActive
                    )
            );
            return LimitPolicyResDto.builder()
                    .lineLimitId(lineLimit.get().getLimitId())
                    .dailyDataLimit(lineLimit.get().getDailyDataLimit())
                    .isDailyDataLimitActive(lineLimit.get().getIsDailyLimitActive())
                    .sharedDataLimit(lineLimit.get().getSharedDataLimit())
                    .isSharedDataLimitActive(newSharedLimitActive)
                    .build();
        } else {
            // lineLimit 레코드가 없거나, 삭제 상태인 경우 새 레코드 insert
            return insertNewLineLimit(lineId);
        }
    }

    /**
     * Create a new LineLimit with default values, emit creation alarms, synchronize it via write-through, and return a DTO representing the created limit.
     *
     * @param lineId the identifier of the line for which the limit is created
     * @return a LimitPolicyResDto representing the newly created line limit and its flags
     * @throws ApplicationException if persisting the new LineLimit to the database fails
     */
    private LimitPolicyResDto insertNewLineLimit(Long lineId) {
        LineLimit newLineLimit = LineLimit.builder()
                .lineId(lineId)
                .dailyDataLimit(-1L)
                .isDailyLimitActive(true)
                .sharedDataLimit(-1L)
                .isSharedLimitActive(true)
                .build();
        int def = lineLimitMapper.createLineLimit(newLineLimit);
        if (def != 1) {
            throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
        }
        alarmHistoryService.createAlarm(lineId, AlarmCode.POLICY_LIMIT, AlarmType.POLICY_CREATE_DAYDATA_LIMIT);
        alarmHistoryService.createAlarm(lineId, AlarmCode.POLICY_LIMIT, AlarmType.POLICY_CREATE_SHAREDATA_LIMIT);

        applyWriteThrough(
                "line_limit_insert lineId=" + lineId,
                writeThroughService -> writeThroughService.syncLineLimit(
                        lineId,
                        newLineLimit.getDailyDataLimit(),
                        newLineLimit.getIsDailyLimitActive(),
                        newLineLimit.getSharedDataLimit(),
                        newLineLimit.getIsSharedLimitActive()
                )
        );

        return LimitPolicyResDto.builder()
                .lineLimitId(newLineLimit.getLimitId())
                .dailyDataLimit(newLineLimit.getDailyDataLimit())
                .isDailyDataLimitActive(newLineLimit.getIsDailyLimitActive())
                .sharedDataLimit(newLineLimit.getSharedDataLimit())
                .isSharedDataLimitActive(newLineLimit.getIsSharedLimitActive())
                .build();
    }

    /**
     * Update the shared pool data limit for an existing line limit policy.
     *
     * @param request contains the target limit policy ID and the new shared data limit value
     * @param auth    the authenticated user performing the update (used for ownership check)
     * @return a LimitPolicyResDto reflecting the updated sharedDataLimit and the current daily limit values and active flags
     * @throws ApplicationException if the limit policy ID does not exist (LIMIT_POLICY_NOT_FOUND), if the caller is not authorized for the target line (LINE_OWNERSHIP_FORBIDDEN), or if the database update fails (DATABASE_ERROR)
     */
    @Override
    @Transactional
    public LimitPolicyResDto updateSharedPoolLimitPolicyValue(LimitPolicyUpdateReqDto request, AuthUserDetails auth) {
        // 1. 대상 lineLimit 레코드 조회
        Optional<LineLimit> lineLimit = lineLimitMapper.getExistLineLimitById(request.getLimitPolicyId());
        if (lineLimit.isEmpty()) {
            throw new ApplicationException(PolicyErrorCode.LIMIT_POLICY_NOT_FOUND);
        }

        // 2. 대상 lineId가 동일 가족에 속했는지 검증
        checkIsSameFamilyGroup(lineLimit.get().getLineId(), auth.getLineId(), auth);

        // 3. update 진행
        int def = lineLimitMapper.updateSharedDataLimit(request);
        if (def != 1) {
            throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
        }

        alarmHistoryService.createAlarm(lineLimit.get().getLineId(), AlarmCode.POLICY_CHANGE, AlarmType.POLICY_UPDATE_SHAREDATA_LIMIT);

        applyWriteThrough(
                "line_limit_update_shared_value lineId=" + lineLimit.get().getLineId(),
                writeThroughService -> writeThroughService.syncLineLimit(
                        lineLimit.get().getLineId(),
                        lineLimit.get().getDailyDataLimit(),
                        lineLimit.get().getIsDailyLimitActive(),
                        request.getPolicyValue(),
                        lineLimit.get().getIsSharedLimitActive()
                )
        );

        return LimitPolicyResDto.builder()
                .lineLimitId(lineLimit.get().getLimitId())
                .dailyDataLimit(lineLimit.get().getDailyDataLimit())
                .isDailyDataLimitActive(lineLimit.get().getIsDailyLimitActive())
                .sharedDataLimit(request.getPolicyValue())
                .isSharedDataLimitActive(lineLimit.get().getIsSharedLimitActive())
                .build();
    }

    @Override
    @Transactional(readOnly = true)
    public PagingResDto<AppPolicyResDto> getAppPolicies(AppPolicySearchCondReqDto request, AuthUserDetails auth) {
        if (request.getPageNumber() == null || request.getPageNumber() < 0) {
            throw new ApplicationException(CommonErrorCode.INVALID_PAGE_NUMBER);
        }
        if (request.getPageSize() == null || request.getPageSize() <= 0 || request.getPageSize() > 100) {
            throw new ApplicationException(CommonErrorCode.INVALID_PAGE_SIZE);
        }

        PolicyScope policyScope = request.getPolicyScope() != null
                ? request.getPolicyScope()
                : PolicyScope.ALL;
        SortType sortType = request.getSortType() != null
                ? request.getSortType()
                : SortType.ACTIVE;
        String keyword = request.getKeyword() != null && request.getKeyword().isBlank()
                ? null
                : request.getKeyword();

        AppPolicySearchCondReqDto query = AppPolicySearchCondReqDto.builder()
                .lineId(request.getLineId())
                .keyword(keyword)
                .policyScope(policyScope)
                .dataLimit(request.isDataLimit())
                .speedLimit(request.isSpeedLimit())
                .sortType(sortType)
                .pageNumber(request.getPageNumber())
                .pageSize(request.getPageSize())
                .offset(request.getPageNumber() * request.getPageSize())
                .build();
        checkIsSameFamilyGroup(query.getLineId(), auth.getLineId(), auth);

        List<AppPolicyResDto> content = appPolicyMapper.findApplicationsWithPolicy(query);
        Long totalElements = appPolicyMapper.countApplicationsWithPolicy(query);

        int totalPages = (query.getPageSize() == 0) ? 0 : (int) Math.ceil((double) totalElements / query.getPageSize());

        return PagingResDto.<AppPolicyResDto>builder()
                .content(content)
                .page(query.getPageNumber())
                .size(query.getPageSize())
                .totalElements(totalElements)
                .totalPages(totalPages)
                .build();
    }

    @Override
    @Deprecated
    public AppPolicyResDto createAppPolicy(AppPolicyActiveToggleReqDto request, AuthUserDetails auth) {
        // 이 API는 앱 별 정책 활성화/비활성화 toggle API로 통합되었습니다.
        // toggle API 요청 시 해당 앱에 대한 정책이 기존에 존재하지 않았다면, 신규 생성 동작을 수행합니다.
        return null;
    }

    /**
     * Update the daily data limit for an application policy and return the updated policy DTO.
     *
     * @param request contains the target appPolicyId and the new daily data limit value
     * @param auth    caller authentication used to validate family-group ownership
     * @return the updated AppPolicyResDto reflecting the new daily data limit
     * @throws ApplicationException if the app policy is not found (APP_POLICY_NOT_FOUND) or the database update fails (DATABASE_ERROR)
     */
    @Override
    @Transactional
    public AppPolicyResDto updateAppDataLimit(AppDataLimitUpdateReqDto request, AuthUserDetails auth) {
        // 1. 대상 appPolicy DTO 조회
        Optional<AppPolicyResDto> appPolicy = appPolicyMapper.findDtoExistById(request.getAppPolicyId());
        if (appPolicy.isEmpty()) {
            throw new ApplicationException(PolicyErrorCode.APP_POLICY_NOT_FOUND);
        }

        // 2. 대상 lineId가 동일 가족에 속했는지 검증
        checkIsSameFamilyGroup(appPolicy.get().getLineId(), auth.getLineId(), auth);

        // 3. update 진행
        int ret = appPolicyMapper.updateDataLimit(request.getAppPolicyId(), request.getValue());
        if (ret != 1) {
            throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
        }

        alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_CHANGE,
                AlarmType.POLICY_UPDATE_APP_USAGE_LIMIT);
        // 4. toBuilder()를 활용해 변경 사항이 반영된 응답 DTO 반환
        AppPolicyResDto updatedResponse = appPolicy.get().toBuilder()
                .dailyLimitData(request.getValue())
                .build();

        applyWriteThrough(
                "app_policy_update_data_limit lineId=" + updatedResponse.getLineId() + " appId=" + updatedResponse.getAppId(),
                writeThroughService -> writeThroughService.syncAppPolicy(
                        updatedResponse.getLineId(),
                        updatedResponse.getAppId(),
                        Boolean.TRUE.equals(updatedResponse.getIsActive()),
                        updatedResponse.getDailyLimitData(),
                        updatedResponse.getDailyLimitSpeed(),
                        Boolean.TRUE.equals(updatedResponse.getIsWhiteList())
                )
        );

        return updatedResponse;
    }

    /**
     * Update the app's data speed limit for an existing app policy.
     *
     * @param request the request containing the target `appPolicyId` and the new speed limit value
     * @param auth    the caller's authenticated user details used to validate family ownership
     * @return        the updated AppPolicyResDto reflecting the new daily speed limit
     * @throws ApplicationException if the app policy does not exist (PolicyErrorCode.APP_POLICY_NOT_FOUND),
     *                              if the caller is not permitted to modify the policy's line (PolicyErrorCode.LINE_OWNERSHIP_FORBIDDEN),
     *                              or if the database update fails (CommonErrorCode.DATABASE_ERROR)
     */
    @Override
    @Transactional
    public AppPolicyResDto updateAppSpeedLimit(AppSpeedLimitUpdateReqDto request, AuthUserDetails auth) {
        // 1. 대상 appPolicy DTO 조회
        Optional<AppPolicyResDto> appPolicy = appPolicyMapper.findDtoExistById(request.getAppPolicyId());
        if (appPolicy.isEmpty()) {
            throw new ApplicationException(PolicyErrorCode.APP_POLICY_NOT_FOUND);
        }

        // 2. 대상 lineId가 동일 가족에 속했는지 검증
        checkIsSameFamilyGroup(appPolicy.get().getLineId(), auth.getLineId(), auth);

        // 3. update 진행
        int ret = appPolicyMapper.updateSpeedLimit(request.getAppPolicyId(), request.getValue());
        if (ret != 1) {
            throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
        }
        alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_CHANGE,
                AlarmType.POLICY_UPDATE_DATA_SPEED_LIMIT);

        // 4. toBuilder()를 활용해 변경 사항이 반영된 응답 DTO 반환
        AppPolicyResDto updatedResponse = appPolicy.get().toBuilder()
                .dailyLimitSpeed(request.getValue())
                .build();

        applyWriteThrough(
                "app_policy_update_speed_limit lineId=" + updatedResponse.getLineId() + " appId=" + updatedResponse.getAppId(),
                writeThroughService -> writeThroughService.syncAppPolicy(
                        updatedResponse.getLineId(),
                        updatedResponse.getAppId(),
                        Boolean.TRUE.equals(updatedResponse.getIsActive()),
                        updatedResponse.getDailyLimitData(),
                        updatedResponse.getDailyLimitSpeed(),
                        Boolean.TRUE.equals(updatedResponse.getIsWhiteList())
                )
        );

        return updatedResponse;
    }

    /**
     * Toggle the active state of an application policy for a line, creating a default active policy if none exists.
     *
     * Performs ownership validation, persists the change (or inserts a new policy), emits related alarms, and synchronizes the resulting policy state to the external write-through store.
     *
     * @param request contains the target lineId and applicationId used to identify the app policy
     * @param auth caller credentials used to validate family-group ownership and permissions
     * @return the resulting AppPolicyResDto reflecting the updated or newly created policy state
     * @throws ApplicationException with PolicyErrorCode.APP_NOT_FOUND if the application does not exist for the line
     * @throws ApplicationException with CommonErrorCode.DATABASE_ERROR if a database update/insert fails
     */
    @Override
    @Transactional
    public AppPolicyResDto toggleAppPolicyActive(AppPolicyActiveToggleReqDto request, AuthUserDetails auth) {
        // 1. 대상 lineId와 같은 가족인지 검증
        checkIsSameFamilyGroup(request.getLineId(), auth.getLineId(), auth);

        // 2. 대상 앱의 정보 및 정책 정보를 함께 조회(정책이 없어도 조회결과 존재)
        Optional<AppPolicyResDto> appPolicy = appPolicyMapper.findDtoExistByLineIdAndAppId(request.getLineId(),
                request.getApplicationId());
        if (appPolicy.isPresent()) {
            if (appPolicy.get().getAppPolicyId() != null) {
                // 3-1. 기존 정책이 존재한다면 is_active를 반대값으로 설정(toggle)
                boolean newIsActive = !appPolicy.get().getIsActive();
                int ret = appPolicyMapper.updateIsActive(appPolicy.get().getAppPolicyId(), newIsActive);
                if (ret != 1) {
                    throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
                }
                if (newIsActive) {
                    alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT,
                            AlarmType.POLICY_CREATE_APP_USAGE_LIMIT);
                    alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT,
                            AlarmType.POLICY_CREATE_DATA_SPEED_LIMIT);
                } else {
                    alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT,
                            AlarmType.POLICY_DELETE_DATA_SPEED_LIMIT);
                    alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT,
                            AlarmType.POLICY_DELETE_APP_USAGE_LIMIT);
                }
                AppPolicyResDto updatedResponse = appPolicy.get().toBuilder()
                        .isActive(!appPolicy.get().getIsActive())
                        .build();

                applyWriteThrough(
                        "app_policy_toggle_active lineId=" + updatedResponse.getLineId() + " appId=" + updatedResponse.getAppId(),
                        writeThroughService -> writeThroughService.syncAppPolicy(
                                updatedResponse.getLineId(),
                                updatedResponse.getAppId(),
                                Boolean.TRUE.equals(updatedResponse.getIsActive()),
                                updatedResponse.getDailyLimitData(),
                                updatedResponse.getDailyLimitSpeed(),
                                Boolean.TRUE.equals(updatedResponse.getIsWhiteList())
                        )
                );

                return updatedResponse;
            } else {
                // 3-2. 기존 정책이 존재하지 않는다면 기본값 세팅하여 새 레코드 insert
                AppPolicy newAppPolicy = AppPolicy.builder()
                        .lineId(request.getLineId())
                        .applicationId(request.getApplicationId())
                        .dataLimit(-1L)
                        .speedLimit(-1)
                        .isActive(true)
                        .build();
                int ret = appPolicyMapper.insertAppPolicy(newAppPolicy);
                if (ret != 1) {
                    throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
                }

                // 알람 전송
                alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT,
                        AlarmType.POLICY_CREATE_APP_USAGE_LIMIT);
                alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT,
                        AlarmType.POLICY_CREATE_DATA_SPEED_LIMIT);

                AppPolicyResDto createdResponse = AppPolicyResDto.builder()
                        .appPolicyId(newAppPolicy.getAppPolicyId())
                        .lineId(request.getLineId())
                        .appId(request.getApplicationId())
                        .appName(appPolicy.get().getAppName())
                        .isActive(newAppPolicy.getIsActive())
                        .isWhiteList(false)
                        .dailyLimitData(newAppPolicy.getDataLimit())
                        .dailyLimitSpeed(newAppPolicy.getSpeedLimit())
                        .build();

                applyWriteThrough(
                        "app_policy_create lineId=" + createdResponse.getLineId() + " appId=" + createdResponse.getAppId(),
                        writeThroughService -> writeThroughService.syncAppPolicy(
                                createdResponse.getLineId(),
                                createdResponse.getAppId(),
                                Boolean.TRUE.equals(createdResponse.getIsActive()),
                                createdResponse.getDailyLimitData(),
                                createdResponse.getDailyLimitSpeed(),
                                Boolean.TRUE.equals(createdResponse.getIsWhiteList())
                        )
                );

                return createdResponse;
            }
        } else {
            // 3-3. 조회 쿼리의 결과가 아예 존재하지 않는다면 앱이 존재하지 않음을 의미
            throw new ApplicationException(PolicyErrorCode.APP_NOT_FOUND);
        }
    }

    /**
     * Toggle the whitelist setting for an application policy, emit a corresponding alarm, and synchronize the updated policy to the write-through store.
     *
     * This returns a DTO that reflects the policy after the whitelist flag has been toggled.
     *
     * @param appPolicyId the identifier of the application policy to toggle
     * @param auth the authenticated user performing the operation
     * @return an AppPolicyResDto representing the application policy with the updated whitelist state
     * @throws ApplicationException if the app policy does not exist (APP_POLICY_NOT_FOUND), if the caller is not authorized for the target line (LINE_OWNERSHIP_FORBIDDEN), or if the database update fails (DATABASE_ERROR)
     */
    @Override
    @Transactional
    public AppPolicyResDto toggleAppPolicyWhitelist(Long appPolicyId, AuthUserDetails auth) {
        // 1. 대상 appPolicy DTO 조회
        Optional<AppPolicyResDto> appPolicy = appPolicyMapper.findDtoExistById(appPolicyId);
        if (appPolicy.isEmpty()) {
            throw new ApplicationException(PolicyErrorCode.APP_POLICY_NOT_FOUND);
        }

        // 2. 대상 lineId가 동일 가족에 속했는지 검증
        checkIsSameFamilyGroup(appPolicy.get().getLineId(), auth.getLineId(), auth);

        // 3. isWhiteList 토글 update 진행
        boolean newIsWhiteList = !Boolean.TRUE.equals(appPolicy.get().getIsWhiteList());
        int ret = appPolicyMapper.updateIsWhitelist(appPolicyId, newIsWhiteList);
        if (ret != 1) {
            throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
        }

        // 알람 전송
        if (newIsWhiteList) {
            alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT,
                    AlarmType.POLICY_ADD_WHITELIST);
        } else {
            alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT,
                    AlarmType.POLICY_DELETE_WHITELIST);
        }

        // 4. toBuilder()를 활용해 변경 사항이 반영된 응답 DTO 반환
        AppPolicyResDto updatedResponse = appPolicy.get().toBuilder()
                .isWhiteList(newIsWhiteList)
                .build();

        applyWriteThrough(
                "app_policy_toggle_whitelist lineId=" + updatedResponse.getLineId() + " appId=" + updatedResponse.getAppId(),
                writeThroughService -> writeThroughService.syncAppPolicy(
                        updatedResponse.getLineId(),
                        updatedResponse.getAppId(),
                        Boolean.TRUE.equals(updatedResponse.getIsActive()),
                        updatedResponse.getDailyLimitData(),
                        updatedResponse.getDailyLimitSpeed(),
                        Boolean.TRUE.equals(updatedResponse.getIsWhiteList())
                )
        );

        return updatedResponse;
    }

    /**
     * Soft-deletes an application policy, emits related alarms, and evicts the policy from the write-through store.
     *
     * @param appPolicyId the identifier of the application policy to delete
     * @param auth the authenticated user performing the operation (used to validate family-group ownership)
     * @throws ApplicationException if the app policy does not exist, the delete update fails, or the requester is not permitted to operate on the target line
     */
    @Override
    @Transactional
    public void deleteAppPolicy(Long appPolicyId, AuthUserDetails auth) {
        // 삭제 대상 app policy 레코드 조회
        Optional<AppPolicy> appPolicy = appPolicyMapper.findEntityExistById(appPolicyId);
        if (appPolicy.isEmpty()) {
            // 없다면 예외 발생
            throw new ApplicationException(PolicyErrorCode.APP_POLICY_NOT_FOUND);
        }
        // 대상 레코드의 회선과 동일 가족에 속했는지 검증
        checkIsSameFamilyGroup(appPolicy.get().getLineId(), auth.getLineId(), auth);

        // soft delete
        int ret = appPolicyMapper.setDeleted(appPolicyId);
        if (ret != 1) {
            throw new ApplicationException(CommonErrorCode.DATABASE_ERROR);
        }

        // 알람 전송
        alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT, AlarmType.POLICY_DELETE_APP_USAGE_LIMIT);
        alarmHistoryService.createAlarm(appPolicy.get().getLineId(), AlarmCode.POLICY_LIMIT, AlarmType.POLICY_DELETE_DATA_SPEED_LIMIT);

        applyWriteThrough(
                "app_policy_delete lineId=" + appPolicy.get().getLineId() + " appId=" + appPolicy.get().getApplicationId(),
                writeThroughService -> writeThroughService.evictAppPolicy(
                        appPolicy.get().getLineId(),
                        appPolicy.get().getApplicationId()
                )
        );
    }

    /**
	 * Collects the policies currently applied to a given line.
	 *
	 * Retrieves repeat block policies and the immediate block (if present) for the specified line after verifying ownership.
	 *
	 * @param lineId the identifier of the line whose applied policies are requested
	 * @param auth requester authentication details used to verify family-group ownership
	 * @return an AppliedPolicyResDto containing the list of repeat block policies and the immediate block (or `null` if none)
	 */
	@Override
    public AppliedPolicyResDto getAppliedPolicies(Long lineId, AuthUserDetails auth) {
        checkIsSameFamilyGroup(lineId, auth.getLineId(), auth);

        // 제한 정책, 앱 정보 받아와서 추가하기(return에도)
        List<RepeatBlockPolicyResDto> repeatBlockPolicyList = repeatBlockMapper.selectRepeatBlocksByLineId(lineId);

        ImmediateBlockResDto immediateBlock = immediateBlockMapper.selectImmediateBlockPolicy(lineId);

        // null 안전하게 처리
        ImmediateBlockResDto immBlock = (immediateBlock != null)
                ? ImmediateBlockResDto.builder()
                        .lineId(lineId)
                        .blockEndAt(immediateBlock.getBlockEndAt())
                        .build()
                : null;

        return AppliedPolicyResDto.builder()
                .repeatBlockPolicyList(repeatBlockPolicyList)
                .immediateBlock(immBlock)
                .build();

	}

    /**
     * Executes a write-through operation with the available TrafficPolicyWriteThroughService or no-ops if none is available.
     *
     * If a write-through service can be obtained from the configured provider, the provided callback is invoked with that service;
     * otherwise the method returns without performing any action.
     *
     * @param operationName a descriptive name for the write-through operation (used for logging/debug context)
     * @param callback a consumer that performs the write-through action using the provided TrafficPolicyWriteThroughService
     */
    private void applyWriteThrough(
            String operationName,
            java.util.function.Consumer<TrafficPolicyWriteThroughService> callback
    ) {
        // 단위 테스트(@InjectMocks) 환경에서는 ObjectProvider 주입이 생략될 수 있다.
        if (trafficPolicyWriteThroughServiceProvider == null) {
            return;
        }

        TrafficPolicyWriteThroughService writeThroughService = trafficPolicyWriteThroughServiceProvider.getIfAvailable();
        if (writeThroughService == null) {
            // api profile처럼 cache Redis가 비활성인 환경에서는 write-through를 건너뛴다.
            log.debug("traffic_policy_write_through_skipped operation={} reason=no_cache_redis_profile", operationName);
            return;
        }

        callback.accept(writeThroughService);
    }

    /**
     * 대상 lineId가 API 요청자와 동일한 가족 그룹에 속해있는지 검증
     *
     * @param targetLineId 대상 lineId
     * @param myLineId     API 요청자 lineId
     * @param auth         세션 정보를 담은 인증 객체(AuthUserDetails)
     * @throws ApplicationException '접근 권한 없음' 예외 throws
     */
    private void checkIsSameFamilyGroup(Long targetLineId, Long myLineId, AuthUserDetails auth)
            throws ApplicationException {
        if (auth.getRoleNames().contains("ROLE_ADMIN")) {
            return;
        }
        List<Long> myFamilyLineIdList = familyLineMapper.findAllFamilyIdByLineId(myLineId);

        if (!myFamilyLineIdList.contains(targetLineId)) {
            // 동일한 가족 그룹에 속해있지 않다면 권한 없음을 의미
            throw new ApplicationException(CommonErrorCode.LINE_OWNERSHIP_FORBIDDEN);
        }
    }

}
