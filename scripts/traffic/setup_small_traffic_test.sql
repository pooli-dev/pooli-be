-- Small traffic test setup for line_id 1~16 / family_id 1~4
-- Timezone: Asia/Seoul (UTC+09:00)
--
-- Group mapping
--   G1 (no restriction):                line 1~4
--   G2 (daily limit 20MB):             line 5~8
--   G3 (app#2 daily limit 5MB):        line 9~12
--   G4 (shared-pool-only, same family): line 13~14
--   G5 (app#2 speed limit 1Mbps):      line 15~16
--
-- Unit
--   1MB = 1,048,576 bytes
--
-- NOTE:
--   MongoDB done log cleanup must be executed separately.
--   Example (mongosh):
--   use <your_mongo_db>
--   db.traffic_deduct_done_log.deleteMany({ line_id: { $gte: 1, $lte: 16 } })

SET SQL_SAFE_UPDATES = 0;
SET time_zone = '+09:00';

SET @ONE_MB_BYTES = 1048576;
SET @LINE_FULL_BYTES = 100 * @ONE_MB_BYTES;            -- 100MB
SET @FAMILY_SHARED_BYTES = 50 * @ONE_MB_BYTES;         -- 50MB
SET @DAILY_LIMIT_20MB = 20 * @ONE_MB_BYTES;            -- 20MB
SET @APP2_DAILY_LIMIT_5MB = 5 * @ONE_MB_BYTES;         -- 5MB
SET @APP2_SPEED_LIMIT_KBPS = 1000;                     -- 1Mbps

START TRANSACTION;

-- ---------------------------------------------------------------------------
-- 0) Optional pre-check (existence)
-- ---------------------------------------------------------------------------
SELECT 'family_count_1_4' AS check_name, COUNT(*) AS cnt
FROM FAMILY
WHERE family_id BETWEEN 1 AND 4
  AND deleted_at IS NULL;

SELECT 'line_count_1_16' AS check_name, COUNT(*) AS cnt
FROM LINE
WHERE line_id BETWEEN 1 AND 16
  AND deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- 1) Cleanup range for this test
-- ---------------------------------------------------------------------------
-- Reset immediate block for all target lines first.
UPDATE LINE
SET block_end_at = NULL,
    updated_at = NOW(6)
WHERE line_id BETWEEN 1 AND 16;

-- Delete all policy/block records tied to target lines.
DELETE rbd
FROM REPEAT_BLOCK_DAY rbd
JOIN REPEAT_BLOCK rb ON rb.repeat_block_id = rbd.repeat_block_id
WHERE rb.line_id BETWEEN 1 AND 16;

DELETE FROM REPEAT_BLOCK
WHERE line_id BETWEEN 1 AND 16;

DELETE FROM APP_POLICY
WHERE line_id BETWEEN 1 AND 16;

DELETE FROM LINE_LIMIT
WHERE line_id BETWEEN 1 AND 16;

-- Clear usage aggregates for deterministic policy checks.
DELETE FROM DAILY_APP_TOTAL_DATA
WHERE line_id BETWEEN 1 AND 16;

DELETE FROM DAILY_TOTAL_DATA
WHERE line_id BETWEEN 1 AND 16;

DELETE FROM FAMILY_SHARED_USAGE_DAILY
WHERE family_id BETWEEN 1 AND 4
   OR line_id BETWEEN 1 AND 16;

-- Include outbox/fallback tables in setup scope.
DELETE FROM TRAFFIC_REDIS_OUTBOX;

DELETE FROM TRAFFIC_REDIS_USAGE_DELTA
WHERE line_id BETWEEN 1 AND 16;

DELETE FROM TRAFFIC_DB_SPEED_BUCKET
WHERE (pool_type = 'INDIVIDUAL' AND owner_id BETWEEN 1 AND 16)
   OR (pool_type = 'SHARED' AND owner_id BETWEEN 1 AND 4);

-- Optional: legacy SQL done table cleanup (Mongo done-log is separate).
DELETE FROM TRAFFIC_DEDUCT_DONE
WHERE line_id BETWEEN 1 AND 16;

-- ---------------------------------------------------------------------------
-- 2) Base quota reset
-- ---------------------------------------------------------------------------
UPDATE LINE
SET remaining_data = @LINE_FULL_BYTES,
    deleted_at = NULL,
    updated_at = NOW(6)
WHERE line_id BETWEEN 1 AND 16;

UPDATE FAMILY
SET pool_base_data = @FAMILY_SHARED_BYTES,
    pool_total_data = @FAMILY_SHARED_BYTES,
    pool_remaining_data = @FAMILY_SHARED_BYTES,
    deleted_at = NULL,
    updated_at = NOW(6)
WHERE family_id BETWEEN 1 AND 4;

-- ---------------------------------------------------------------------------
-- 3) Enforce family-line mapping (1~16)
-- ---------------------------------------------------------------------------
DELETE FROM FAMILY_LINE
WHERE line_id BETWEEN 1 AND 16;

INSERT INTO FAMILY_LINE (family_id, line_id, role, is_public, created_at, updated_at)
VALUES
    (1,  1, 'OWNER',  1, NOW(6), NOW(6)),
    (1,  2, 'MEMBER', 1, NOW(6), NOW(6)),
    (1,  3, 'MEMBER', 1, NOW(6), NOW(6)),
    (1,  4, 'MEMBER', 1, NOW(6), NOW(6)),
    (2,  5, 'OWNER',  1, NOW(6), NOW(6)),
    (2,  6, 'MEMBER', 1, NOW(6), NOW(6)),
    (2,  7, 'MEMBER', 1, NOW(6), NOW(6)),
    (2,  8, 'MEMBER', 1, NOW(6), NOW(6)),
    (3,  9, 'OWNER',  1, NOW(6), NOW(6)),
    (3, 10, 'MEMBER', 1, NOW(6), NOW(6)),
    (3, 11, 'MEMBER', 1, NOW(6), NOW(6)),
    (3, 12, 'MEMBER', 1, NOW(6), NOW(6)),
    (4, 13, 'OWNER',  1, NOW(6), NOW(6)),
    (4, 14, 'MEMBER', 1, NOW(6), NOW(6)),
    (4, 15, 'MEMBER', 1, NOW(6), NOW(6)),
    (4, 16, 'MEMBER', 1, NOW(6), NOW(6));

-- ---------------------------------------------------------------------------
-- 4) Group-specific setup
-- ---------------------------------------------------------------------------
-- G2: daily total limit 20MB (line 5~8)
INSERT INTO LINE_LIMIT (
    line_id,
    daily_data_limit,
    is_daily_limit_active,
    shared_data_limit,
    is_shared_limit_active,
    created_at,
    updated_at
)
VALUES
    (5, @DAILY_LIMIT_20MB, 1, -1, 0, NOW(6), NOW(6)),
    (6, @DAILY_LIMIT_20MB, 1, -1, 0, NOW(6), NOW(6)),
    (7, @DAILY_LIMIT_20MB, 1, -1, 0, NOW(6), NOW(6)),
    (8, @DAILY_LIMIT_20MB, 1, -1, 0, NOW(6), NOW(6));

-- G3: app_id=2 daily limit 5MB (line 9~12)
INSERT INTO APP_POLICY (
    line_id,
    application_id,
    data_limit,
    speed_limit,
    is_active,
    is_whitelist,
    created_at,
    updated_at
)
VALUES
    (9,  2, @APP2_DAILY_LIMIT_5MB, -1, 1, 0, NOW(6), NOW(6)),
    (10, 2, @APP2_DAILY_LIMIT_5MB, -1, 1, 0, NOW(6), NOW(6)),
    (11, 2, @APP2_DAILY_LIMIT_5MB, -1, 1, 0, NOW(6), NOW(6)),
    (12, 2, @APP2_DAILY_LIMIT_5MB, -1, 1, 0, NOW(6), NOW(6));

-- G5: app_id=2 speed limit 1Mbps(=1000Kbps) (line 15~16)
INSERT INTO APP_POLICY (
    line_id,
    application_id,
    data_limit,
    speed_limit,
    is_active,
    is_whitelist,
    created_at,
    updated_at
)
VALUES
    (15, 2, -1, @APP2_SPEED_LIMIT_KBPS, 1, 0, NOW(6), NOW(6)),
    (16, 2, -1, @APP2_SPEED_LIMIT_KBPS, 1, 0, NOW(6), NOW(6));

-- G4: shared-pool-only users (same family #4, line 13~14)
UPDATE LINE
SET remaining_data = 0,
    updated_at = NOW(6)
WHERE line_id IN (13, 14);

-- Keep global policy 1~7 active for the traffic scenario.
UPDATE POLICY
SET is_active = 1,
    deleted_at = NULL,
    updated_at = NOW(6)
WHERE policy_id BETWEEN 1 AND 7;

COMMIT;

-- ---------------------------------------------------------------------------
-- 5) Post-check queries
-- ---------------------------------------------------------------------------
SELECT line_id, remaining_data, block_end_at
FROM LINE
WHERE line_id BETWEEN 1 AND 16
ORDER BY line_id;

SELECT family_id, pool_base_data, pool_total_data, pool_remaining_data
FROM FAMILY
WHERE family_id BETWEEN 1 AND 4
ORDER BY family_id;

SELECT line_id, daily_data_limit, is_daily_limit_active, shared_data_limit, is_shared_limit_active
FROM LINE_LIMIT
WHERE line_id BETWEEN 1 AND 16
ORDER BY line_id;

SELECT line_id, application_id, data_limit, speed_limit, is_active, is_whitelist
FROM APP_POLICY
WHERE line_id BETWEEN 1 AND 16
ORDER BY line_id, application_id;

SELECT family_id, line_id, role, is_public
FROM FAMILY_LINE
WHERE line_id BETWEEN 1 AND 16
ORDER BY family_id, line_id;

SELECT policy_id, is_active
FROM POLICY
WHERE policy_id BETWEEN 1 AND 7
ORDER BY policy_id;

SET SQL_SAFE_UPDATES = 1;
