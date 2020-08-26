-- clean names, fix up timestamp
CREATE OR REPLACE STREAM "INCOMING_STREAM" (
    "uniqueId" INTEGER,
    "speed" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" TIMESTAMP);

CREATE OR REPLACE PUMP "INCOMING_STREAM_PUMP" AS
    INSERT INTO "INCOMING_STREAM"
        SELECT STREAM
            "unieke_id",
            "voertuigsnelheid_rekenkundig_klasse2",
            "rekendata_bezettingsgraad",
            TO_TIMESTAMP(CAST("tijd_waarneming" AS BIGINT) * 1000) AS "recordTimestamp"
        FROM "SOURCE_SQL_STREAM_001";

-- tryout timestamps
-- CREATE OR REPLACE STREAM "TS_STREAM" (
--     "EVENT_TIME" TIMESTAMP,
--     "ROW_TIME" TIMESTAMP,
--     "APPROXIMATE_ARRIVAL_TIME" TIMESTAMP);

-- CREATE OR REPLACE PUMP "INCOMING_STREAM_PUMP" AS
--     INSERT INTO "TS_STREAM"
--         SELECT STREAM
--             "EVENT_TIME",
--             "SOURCE_SQL_STREAM_001".ROWTIME,
--             "SOURCE_SQL_STREAM_001".APPROXIMATE_ARRIVAL_TIME
--         FROM "SOURCE_SQL_STREAM_001";


-- Aggregate the speed over a 3/10 minute window for each unique Id (location)
CREATE OR REPLACE STREAM "SPEED_SQL_STREAM" (
    "uniqueId" INTEGER,
    "speed" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT,
    "avgSpeed3Minutes" INTEGER,
    "avgSpeed10Minutes" INTEGER,
    "originalRecordTimestamp" TIMESTAMP);

CREATE OR REPLACE PUMP "STREAM_PUMP_SPEED" AS
    INSERT INTO "SPEED_SQL_STREAM"
        SELECT STREAM
            "uniqueId",
            AVG("speed") over W0,
            AVG("bezettingsgraad") over W0,
            MAX(UNIX_TIMESTAMP("recordTimestamp")) over W0,
            AVG("speed") over W2,
            AVG("speed") over W10,
            MAX("recordTimestamp") over W0 AS "originalRecordTimestamp"
        FROM "INCOMING_STREAM"
            WINDOW
                W0 AS ( PARTITION BY "uniqueId"
                    RANGE INTERVAL '0' MINUTE PRECEDING),
                W2 AS ( PARTITION BY "uniqueId"
                    RANGE INTERVAL '3' MINUTE PRECEDING),
                W10 AS ( PARTITION BY "uniqueId"
                    RANGE INTERVAL '10' MINUTE PRECEDING);

            --   WINDOW W1 AS (
            --   PARTITION BY ticker_symbol
            --   RANGE INTERVAL '10' SECOND PRECEDING);

            --     WINDOWED BY STAGGER (
            -- PARTITION BY FLOOR(EVENT_TIME TO MINUTE), TICKER_SYMBOL RANGE INTERVAL '1' MINUTE);
        -- WINDOW
        --     W0 AS (PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE), "uniqueId" ROWS 0 PRECEDING),
        --     W2 AS (PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to HOUR), "uniqueId" ROWS 2 PRECEDING),
        --     W10 AS (PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to HOUR), "uniqueId" ROWS 10 PRECEDING);

        -- below is working
        -- WINDOWED BY STAGGER (
            -- PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE), "uniqueId" RANGE INTERVAL '2' MINUTE);
        -- WINDOW W1 AS (
        --     PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE), "uniqueId"
        --     RANGE INTERVAL '2' MINUTE PRECEDING);

        -- not working yet
        -- GROUP BY "uniqueId", FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE),
        --      STEP("INCOMING_STREAM"."recordTimestamp" BY INTERVAL '2' MINUTE);


-- find out if traffic is standing still
CREATE OR REPLACE STREAM "TRAFFIC_JAM_SQL_STREAM" (
    "uniqueId" INTEGER,
    "speed" INTEGER,
    "trafficJamIndicator" INTEGER,
    "recordTimestamp" BIGINT,
    "originalRecordTimestamp" TIMESTAMP);

CREATE OR REPLACE PUMP "TRAFFIC_JAM_SQL_PUMP" AS
    INSERT INTO "TRAFFIC_JAM_SQL_STREAM"
        SELECT STREAM
            "s"."uniqueId" AS "uniqueId",
            "s"."speed" as "speed",
            CASE
                WHEN ("s"."avgSpeed3Minutes" BETWEEN 0 AND 40) AND ("s"."bezettingsgraad" <> 0) THEN 1
                WHEN ("s"."avgSpeed3Minutes" BETWEEN 41 AND 250) OR ("s"."bezettingsgraad" = 0) THEN 0
                ELSE -1
            END AS "trafficJamIndicator",
            "s"."recordTimestamp" AS "recordTimestamp",
            "s"."originalRecordTimestamp" as "originalRecordTimestamp"
        FROM "SPEED_SQL_STREAM" as "s";


-- Stream all the latest speed averages
CREATE OR REPLACE STREAM "SPEED_AGG_AVG_STREAM" (
    "uniqueId" INTEGER,
    "currentSpeed" INTEGER,
    "avgSpeed3Minutes" INTEGER,
    "avgSpeed10Minutes" INTEGER,
    "recordTimestamp" BIGINT,
    "originalRecordTimestamp" TIMESTAMP);

CREATE OR REPLACE PUMP "SPEED_AGG_AVG_PUMP" AS
    INSERT INTO "SPEED_AGG_AVG_STREAM"
        SELECT STREAM "s"."uniqueId",
            "s"."speed" AS "currentSpeed",
            "s"."avgSpeed3Minutes" AS "avgSpeed3Minutes",
            "s"."avgSpeed10Minutes" AS "avgSpeed10Minutes",
            "s"."recordTimestamp",
            "s"."originalRecordTimestamp"
        FROM "SPEED_SQL_STREAM" AS "s";


-- Calculate the difference in speed between the current window and the previous one
-- Get previous speed
CREATE OR REPLACE STREAM "SPEED_CHANGE_SQL_STREAM" (
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT,
    "originalRecordTimestamp" TIMESTAMP);

CREATE OR REPLACE PUMP "SPEED_CHANGE_PUMP" AS
    INSERT INTO "SPEED_CHANGE_SQL_STREAM"
        SELECT STREAM "s"."uniqueId",
            LAG("s"."speed", 1, "s"."speed") OVER CURRENT_WINDOW AS "previousSpeed",
            "s"."speed" AS "currentSpeed",
            "s"."bezettingsgraad",
            "s"."recordTimestamp",
            "s"."originalRecordTimestamp"
        FROM "SPEED_SQL_STREAM" AS "s"
        WINDOW CURRENT_WINDOW AS (PARTITION BY "s"."uniqueId" ROWS 3 PRECEDING);


-- Calculate the difference in speed between the current window and the previous one
-- Calculate difference
CREATE OR REPLACE STREAM "SPEED_DIFF_SQL_STREAM" (
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "speedDiff" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT,
    "originalRecordTimestamp" TIMESTAMP);

CREATE OR REPLACE PUMP "SPEED_DIFF_PUMP" AS
    INSERT INTO "SPEED_DIFF_SQL_STREAM"
        SELECT STREAM "uniqueId",
        "previousSpeed",
        "currentSpeed",
        ("currentSpeed" - "previousSpeed") AS "speedDiff",
        "bezettingsgraad",
        "recordTimestamp",
        "originalRecordTimestamp"
        FROM "SPEED_CHANGE_SQL_STREAM";


-- Indicate speed difference with -1 0 or 1
CREATE OR REPLACE STREAM "SPEED_DIFF_INDICATOR_SQL_STREAM" (
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "speedDiffIndicator" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT,
    "originalRecordTimestamp" TIMESTAMP);


CREATE OR REPLACE PUMP "SPEED_DIFF_INDICATOR_PUMP" AS
    INSERT INTO "SPEED_DIFF_INDICATOR_SQL_STREAM"
        SELECT STREAM "uniqueId",
        "previousSpeed",
        "currentSpeed",
        CASE
            WHEN "speedDiff" >= 20 THEN 1
            WHEN "speedDiff" <= -20 THEN -1
            ELSE 0
        END AS "speedDiffIndicator",
        "bezettingsgraad",
        "recordTimestamp",
        "originalRecordTimestamp"
        FROM "SPEED_DIFF_SQL_STREAM";


--Create output stream 1
CREATE OR REPLACE STREAM "OUTPUT_STREAM" (
    "outputType" VARCHAR(20),
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "avgSpeed3Minutes" INTEGER,
    "avgSpeed10Minutes" INTEGER,
    "speedDiffIndicator" INTEGER,
    "trafficJamIndicator" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT,
    "location" VARCHAR(128),
    "originalRecordTimestamp" TIMESTAMP);


-- Publish speed data to output stream
CREATE OR REPLACE PUMP "SPEED_DIFF_TO_OUTPUT_PUMP" AS
    INSERT INTO "OUTPUT_STREAM" ("outputType", "uniqueId", "previousSpeed", "currentSpeed", "speedDiffIndicator", "bezettingsgraad", "recordTimestamp", "location", "originalRecordTimestamp")
        SELECT STREAM
        'SPEED_DIFFERENTIAL',
        "sdi"."uniqueId",
        "sdi"."previousSpeed",
        "sdi"."currentSpeed",
        "sdi"."speedDiffIndicator",
        "sdi"."bezettingsgraad",
        "sdi"."recordTimestamp",
        "ml"."locatie",
        "sdi"."originalRecordTimestamp"
        FROM "SPEED_DIFF_INDICATOR_SQL_STREAM" AS "sdi" LEFT JOIN "measurementLocations" as "ml"
        ON "sdi"."uniqueId" = "ml"."id";


-- Publish traffic jam data to output stream
CREATE OR REPLACE PUMP "TRAFFIC_JAM_TO_OUTPUT_PUMP" AS
    INSERT INTO "OUTPUT_STREAM" ("outputType", "uniqueId", "currentSpeed", "trafficJamIndicator", "recordTimestamp", "location", "originalRecordTimestamp")
        SELECT STREAM
        'TRAFFIC_JAM',
        "tjs"."uniqueId",
        "tjs"."speed",
        "tjs"."trafficJamIndicator",
        "tjs"."recordTimestamp",
        "ml"."locatie",
        "tjs"."originalRecordTimestamp"
        FROM "TRAFFIC_JAM_SQL_STREAM" AS "tjs" LEFT JOIN "measurementLocations" as "ml"
        ON "tjs"."uniqueId" = "ml"."id";


-- Publish avg speeds data to output stream
CREATE OR REPLACE PUMP "AVG_SPEED_TO_OUTPUT_PUMP" AS
    INSERT INTO "OUTPUT_STREAM" ("outputType", "uniqueId", "currentSpeed", "avgSpeed3Minutes", "avgSpeed10Minutes", "recordTimestamp", "location", "originalRecordTimestamp")
        SELECT STREAM
        'SPEED_AVG',
        "saa"."uniqueId",
        "saa"."currentSpeed",
        "saa"."avgSpeed3Minutes",
        "saa"."avgSpeed10Minutes",
        "saa"."recordTimestamp",
        "ml"."locatie",
        "saa"."originalRecordTimestamp"
        FROM "SPEED_AGG_AVG_STREAM" AS "saa" LEFT JOIN "measurementLocations" as "ml"
        ON "saa"."uniqueId" = "ml"."id";




--Create output stream 2
CREATE OR REPLACE STREAM "OUTPUT_STREAM_2" (
    "outputType" VARCHAR(20),
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "avgSpeed3Minutes" INTEGER,
    "avgSpeed10Minutes" INTEGER,
    "speedDiffIndicator" INTEGER,
    "trafficJamIndicator" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT,
    "location" VARCHAR(128),
    "originalRecordTimestamp" TIMESTAMP);


-- Publish speed data to output stream
CREATE OR REPLACE PUMP "OUTPUT_STREAM_2_SPEED_DIFF_PUMP" AS
    INSERT INTO "OUTPUT_STREAM_2" ("outputType", "uniqueId", "previousSpeed", "currentSpeed", "speedDiffIndicator", "bezettingsgraad", "recordTimestamp", "location", "originalRecordTimestamp")
        SELECT STREAM
        'SPEED_DIFFERENTIAL',
        "sdi"."uniqueId",
        "sdi"."previousSpeed",
        "sdi"."currentSpeed",
        "sdi"."speedDiffIndicator",
        "sdi"."bezettingsgraad",
        "sdi"."recordTimestamp",
        "ml"."locatie",
        "sdi"."originalRecordTimestamp"
        FROM "SPEED_DIFF_INDICATOR_SQL_STREAM" AS "sdi" LEFT JOIN "measurementLocations" as "ml"
        ON "sdi"."uniqueId" = "ml"."id";


-- Publish traffic jam data to output stream
CREATE OR REPLACE PUMP "OUTPUT_STREAM_2_TRAFFIC_JAM_PUMP" AS
    INSERT INTO "OUTPUT_STREAM_2" ("outputType", "uniqueId", "currentSpeed", "trafficJamIndicator", "recordTimestamp", "location", "originalRecordTimestamp")
        SELECT STREAM
        'TRAFFIC_JAM',
        "tjs"."uniqueId",
        "tjs"."speed",
        "tjs"."trafficJamIndicator",
        "tjs"."recordTimestamp",
        "ml"."locatie",
        "tjs"."originalRecordTimestamp"
        FROM "TRAFFIC_JAM_SQL_STREAM" AS "tjs" LEFT JOIN "measurementLocations" as "ml"
        ON "tjs"."uniqueId" = "ml"."id";


-- Publish avg speeds data to output stream
CREATE OR REPLACE PUMP "OUTPUT_STREAM_2_SPEED_AVG_PUMP" AS
    INSERT INTO "OUTPUT_STREAM_2" ("outputType", "uniqueId", "currentSpeed", "avgSpeed3Minutes", "avgSpeed10Minutes", "recordTimestamp", "location", "originalRecordTimestamp")
        SELECT STREAM
        'SPEED_AVG',
        "saa"."uniqueId",
        "saa"."currentSpeed",
        "saa"."avgSpeed3Minutes",
        "saa"."avgSpeed10Minutes",
        "saa"."recordTimestamp",
        "ml"."locatie",
        "saa"."originalRecordTimestamp"
        FROM "SPEED_AGG_AVG_STREAM" AS "saa" LEFT JOIN "measurementLocations" as "ml"
        ON "saa"."uniqueId" = "ml"."id";
