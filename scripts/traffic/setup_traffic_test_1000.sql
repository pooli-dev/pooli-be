-- Scaled traffic test setup (1000 lines)
-- Timezone: Asia/Seoul (UTC+09:00)
--
-- Scope:
--   line_id   : 1 ~ 1000
--   family_id : 1 ~ 250 (4 lines per family)
--
-- Group boundaries are calculated with the same rule as k6_scaled_traffic_lib.js:
--   g1 = floor(N*25%), g2 = floor(N*25%), g3 = floor(N*25%), g4 = floor(N*12.5%), g5 = remainder
--
-- Unit: 1MB = 1,048,576 bytes
--
-- NOTE:
--   MongoDB done-log cleanup must be executed separately.
--   Example (mongosh):
--   use <your_mongo_db>
--   db.traffic_deduct_done_log.deleteMany({ line_id: { $gte: 1, $lte: 1000 } })

SET SQL_SAFE_UPDATES = 0;
SET time_zone = '+09:00';

SET @LINE_START = 1;
SET @LINE_END = 1000;
SET @LINE_COUNT = @LINE_END - @LINE_START + 1;

SET @FAMILY_START = 1;
SET @FAMILY_COUNT = FLOOR(@LINE_COUNT / 4);
SET @FAMILY_END = @FAMILY_START + @FAMILY_COUNT - 1;

SET @ONE_MB_BYTES = 1048576;
SET @LINE_FULL_BYTES = 100 * @ONE_MB_BYTES;          -- 100MB
SET @FAMILY_SHARED_BYTES = 50 * @ONE_MB_BYTES;       -- 50MB
SET @DAILY_LIMIT_20MB = 20 * @ONE_MB_BYTES;          -- 20MB
SET @APP2_DAILY_LIMIT_5MB = 5 * @ONE_MB_BYTES;       -- 5MB
SET @APP2_SPEED_LIMIT_KBPS = 1000;                   -- 1Mbps

SET @G1_COUNT = FLOOR(@LINE_COUNT * 0.25);
SET @G2_COUNT = FLOOR(@LINE_COUNT * 0.25);
SET @G3_COUNT = FLOOR(@LINE_COUNT * 0.25);
SET @G4_COUNT = FLOOR(@LINE_COUNT * 0.125);
SET @G5_COUNT = @LINE_COUNT - @G1_COUNT - @G2_COUNT - @G3_COUNT - @G4_COUNT;

SET @G1_START = @LINE_START;
SET @G1_END = @G1_START + @G1_COUNT - 1;
SET @G2_START = @G1_END + 1;
SET @G2_END = @G2_START + @G2_COUNT - 1;
SET @G3_START = @G2_END + 1;
SET @G3_END = @G3_START + @G3_COUNT - 1;
SET @G4_START = @G3_END + 1;
SET @G4_END = @G4_START + @G4_COUNT - 1;
SET @G5_START = @G4_END + 1;
SET @G5_END = @LINE_END;

START TRANSACTION;

-- ---------------------------------------------------------------------------
-- 0) Pre-check
-- ---------------------------------------------------------------------------
SELECT 'line_count_mod_4' AS check_name, MOD(@LINE_COUNT, 4) AS value;

SELECT 'family_count_in_scope' AS check_name, COUNT(*) AS cnt
FROM FAMILY
WHERE family_id BETWEEN @FAMILY_START AND @FAMILY_END
  AND deleted_at IS NULL;

SELECT 'line_count_in_scope' AS check_name, COUNT(*) AS cnt
FROM LINE
WHERE line_id BETWEEN @LINE_START AND @LINE_END
  AND deleted_at IS NULL;

SELECT 'group_boundaries' AS check_name,
       @G1_START AS g1_start, @G1_END AS g1_end,
       @G2_START AS g2_start, @G2_END AS g2_end,
       @G3_START AS g3_start, @G3_END AS g3_end,
       @G4_START AS g4_start, @G4_END AS g4_end,
       @G5_START AS g5_start, @G5_END AS g5_end;

-- ---------------------------------------------------------------------------
-- 1) Cleanup range for this test
-- ---------------------------------------------------------------------------
UPDATE LINE
SET block_end_at = NULL,
    updated_at = NOW(6)
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

DELETE rbd
FROM REPEAT_BLOCK_DAY rbd
JOIN REPEAT_BLOCK rb ON rb.repeat_block_id = rbd.repeat_block_id
WHERE rb.line_id BETWEEN @LINE_START AND @LINE_END;

DELETE FROM REPEAT_BLOCK
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

DELETE FROM APP_POLICY
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

DELETE FROM LINE_LIMIT
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

DELETE FROM DAILY_APP_TOTAL_DATA
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

DELETE FROM DAILY_TOTAL_DATA
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

DELETE FROM FAMILY_SHARED_USAGE_DAILY
WHERE family_id BETWEEN @FAMILY_START AND @FAMILY_END
   OR line_id BETWEEN @LINE_START AND @LINE_END;

DELETE FROM TRAFFIC_REDIS_OUTBOX;

DELETE FROM TRAFFIC_REDIS_USAGE_DELTA
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

DELETE FROM TRAFFIC_DB_SPEED_BUCKET
WHERE (pool_type = 'INDIVIDUAL' AND owner_id BETWEEN @LINE_START AND @LINE_END)
   OR (pool_type = 'SHARED' AND owner_id BETWEEN @FAMILY_START AND @FAMILY_END);

DELETE FROM TRAFFIC_DEDUCT_DONE
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

-- ---------------------------------------------------------------------------
-- 2) Base quota reset
-- ---------------------------------------------------------------------------
UPDATE LINE
SET remaining_data = @LINE_FULL_BYTES,
    deleted_at = NULL,
    updated_at = NOW(6)
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

UPDATE FAMILY
SET pool_base_data = @FAMILY_SHARED_BYTES,
    pool_total_data = @FAMILY_SHARED_BYTES,
    pool_remaining_data = @FAMILY_SHARED_BYTES,
    deleted_at = NULL,
    updated_at = NOW(6)
WHERE family_id BETWEEN @FAMILY_START AND @FAMILY_END;

-- ---------------------------------------------------------------------------
-- 3) Enforce family-line mapping (4 lines per family)
-- ---------------------------------------------------------------------------
DELETE FROM FAMILY_LINE
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

INSERT INTO FAMILY_LINE (family_id, line_id, role, is_public, created_at, updated_at)
SELECT
    FLOOR((l.line_id - @LINE_START) / 4) + @FAMILY_START AS family_id,
    l.line_id,
    CASE WHEN MOD(l.line_id - @LINE_START, 4) = 0 THEN 'OWNER' ELSE 'MEMBER' END AS role,
    1,
    NOW(6),
    NOW(6)
FROM LINE l
WHERE l.line_id BETWEEN @LINE_START AND @LINE_END
  AND l.deleted_at IS NULL
ORDER BY l.line_id;

-- ---------------------------------------------------------------------------
-- 4) Group-specific setup
-- ---------------------------------------------------------------------------
-- G2: daily total limit 20MB
INSERT INTO LINE_LIMIT (
    line_id,
    daily_data_limit,
    is_daily_limit_active,
    shared_data_limit,
    is_shared_limit_active,
    created_at,
    updated_at
)
SELECT
    l.line_id,
    @DAILY_LIMIT_20MB,
    1,
    -1,
    0,
    NOW(6),
    NOW(6)
FROM LINE l
WHERE l.line_id BETWEEN @G2_START AND @G2_END
  AND l.deleted_at IS NULL
ORDER BY l.line_id;

-- G3: app_id=2 daily limit 5MB
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
SELECT
    l.line_id,
    2,
    @APP2_DAILY_LIMIT_5MB,
    -1,
    1,
    0,
    NOW(6),
    NOW(6)
FROM LINE l
WHERE l.line_id BETWEEN @G3_START AND @G3_END
  AND l.deleted_at IS NULL
ORDER BY l.line_id;

-- G5: app_id=2 speed limit 1Mbps (=1000Kbps)
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
SELECT
    l.line_id,
    2,
    -1,
    @APP2_SPEED_LIMIT_KBPS,
    1,
    0,
    NOW(6),
    NOW(6)
FROM LINE l
WHERE l.line_id BETWEEN @G5_START AND @G5_END
  AND l.deleted_at IS NULL
ORDER BY l.line_id;

-- G4: shared-pool-only users
UPDATE LINE
SET remaining_data = 0,
    updated_at = NOW(6)
WHERE line_id BETWEEN @G4_START AND @G4_END;

UPDATE POLICY
SET is_active = 1,
    deleted_at = NULL,
    updated_at = NOW(6)
WHERE policy_id BETWEEN 1 AND 7;

COMMIT;

-- ---------------------------------------------------------------------------
-- 5) Post-check summary
-- ---------------------------------------------------------------------------
SELECT 'line_scope' AS check_name, COUNT(*) AS cnt
FROM LINE
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

SELECT 'family_scope' AS check_name, COUNT(*) AS cnt
FROM FAMILY
WHERE family_id BETWEEN @FAMILY_START AND @FAMILY_END;

SELECT 'family_line_scope' AS check_name, COUNT(*) AS cnt
FROM FAMILY_LINE
WHERE line_id BETWEEN @LINE_START AND @LINE_END;

SELECT 'family_owner_count' AS check_name, COUNT(*) AS cnt
FROM FAMILY_LINE
WHERE line_id BETWEEN @LINE_START AND @LINE_END
  AND role = 'OWNER';

SELECT 'line_limit_g2_count' AS check_name, COUNT(*) AS cnt, @G2_COUNT AS expected
FROM LINE_LIMIT
WHERE line_id BETWEEN @G2_START AND @G2_END;

SELECT 'app_policy_g3_count' AS check_name, COUNT(*) AS cnt, @G3_COUNT AS expected
FROM APP_POLICY
WHERE line_id BETWEEN @G3_START AND @G3_END
  AND application_id = 2
  AND data_limit = @APP2_DAILY_LIMIT_5MB
  AND speed_limit = -1;

SELECT 'app_policy_g5_count' AS check_name, COUNT(*) AS cnt, @G5_COUNT AS expected
FROM APP_POLICY
WHERE line_id BETWEEN @G5_START AND @G5_END
  AND application_id = 2
  AND speed_limit = @APP2_SPEED_LIMIT_KBPS;

SELECT 'g4_zero_personal_count' AS check_name, COUNT(*) AS cnt, @G4_COUNT AS expected
FROM LINE
WHERE line_id BETWEEN @G4_START AND @G4_END
  AND remaining_data = 0;

SET SQL_SAFE_UPDATES = 1;
