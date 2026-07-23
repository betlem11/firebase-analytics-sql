CREATE TEMPORARY TABLE tmp
                AS (
            WITH
            permissions AS (
        SELECT userId, permissionId, CAST(startedAt AS TIMESTAMP) AS startedAt, CAST(endedAt AS TIMESTAMP) AS endedAt
        FROM EXTERNAL_QUERY("xxx",
            '''
            SELECT DISTINCT
                   userId,
                   permissionId,
                   FIRST_VALUE(startedAt) OVER w AS startedAt,
                   FIRST_VALUE(endedAt) OVER w   AS endedAt
            FROM Permissions
            WHERE userId IS NOT NULL
            WINDOW w AS (PARTITION BY permissionId, userId ORDER BY id DESC);
            '''
            )
            ),
            user_events AS (
        SELECT
          e.userId,
          name,
          FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S%Ez', TIMESTAMP(recordedAt), '+0000') AS recordedAtLocal,
          recordedAt,
          LAG(recordedAt) OVER (PARTITION BY e.userId ORDER BY recordedAt) AS previous_recordedAt,
          -- Extra columns to decorate event
          (SELECT value FROM UNNEST(props) WHERE type = 'type_1') AS type_1,
          (SELECT value FROM UNNEST(props) WHERE type in ('type_2_a', 'type_2_b', 'type_3_c')) AS type_2,
          (SELECT value FROM UNNEST(props) WHERE type = 'type_3') AS type_3,
          domain,
        FROM `firebase-analytics.events` e
        WHERE recordedAt BETWEEN 'date_start_timestamp' AND 'date_end_timestamp'
        ),
            t0 AS (
        SELECT DISTINCT
          ue.userId,
          name,
          type_2,
          recordedAtLocal,
          recordedAt,
          TIMESTAMP_DIFF(recordedAt, previous_recordedAt, SECOND) AS secs_from_prev_event,
          -- Fill in type_1 for events that don't have it
          FIRST_VALUE(type_1 IGNORE NULLS) 
            OVER (PARTITION BY ue.userId 
                  ORDER BY recordedAt ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS type_1,
          -- Update type_3_a and type_3_b events type_3 to the next event type_3
          FIRST_VALUE(IF(type_3 IN ('type_3_a', 'type_3_b'), NULL, type_3) IGNORE NULLS) 
            OVER (PARTITION BY ue.userId 
                  ORDER BY recordedAt ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS type_3,
          domain,
          IF(name='app_in_background', FALSE, TRUE) AS inFocus,
          IF(p.permissionId IS NULL, FALSE, TRUE) AS hasPermission
        FROM user_events ue
        LEFT JOIN permissions p ON ue.userId = p.userId 
                                        AND recordedAt BETWEEN p.startedAt AND IFNULL(p.endedAt, CURRENT_TIMESTAMP())
        ),
            t1 AS (
        SELECT
          *,
          -- Cumsum that increases by 1 when condition for new session is met. All events in a session are given the 
          -- same number.
          SUM(CASE
              WHEN secs_from_prev_event > 30*60 THEN 1
              ELSE 0
              END) OVER (PARTITION BY DATE(recordedAtLocal), userId, type_3 ORDER BY recordedAt) AS session_number
        FROM t0
        ),
            t2 AS (
        SELECT
          *,
          CONCAT(userId, '-', session_number, '-', DATE(recordedAtLocal), '-', type_3) AS sessionId
        FROM t1
        ),
            t3 AS (
        SELECT
          *,
          LEAD(recordedAt) OVER (PARTITION BY sessionId ORDER BY recordedAt) AS next_recordedAt_in_session
        FROM t2
        ),
            t4 AS (
        SELECT
          *,
          TIMESTAMP_DIFF(next_recordedAt_in_session, recordedAt, SECOND) AS eventDurationSeconds
        FROM t3
        )
            
            SELECT
              DATE('date') AS date,
              recordedAt,
              TIMESTAMP(recordedAtLocal) AS recordedAtLocal,
              name,
              type_2,
              sessionId,
              userId,
              hasPermission,
              type_1,
              domain,
              type_3,
              eventDurationSeconds,
              inFocus,
              CURRENT_TIMESTAMP() AS createdAt
            FROM t4
            );

                -- First, delete all rows from table with same date
                DELETE FROM `production-data.events`  WHERE date = 'date';
                -- Then insert all rows from tmp table into table
                INSERT INTO `production-data.events`
                SELECT * FROM tmp;

