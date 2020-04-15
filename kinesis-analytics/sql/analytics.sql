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

-- Aggregate the speed over a 3 minute window for each unique Id (location)
CREATE OR REPLACE STREAM "SPEED_SQL_STREAM" (
    "uniqueId" INTEGER,
    "speed" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT);

CREATE OR REPLACE PUMP "STREAM_PUMP_SPEED" AS
    INSERT INTO "SPEED_SQL_STREAM"
        SELECT STREAM
            "uniqueId",
            AVG("speed") over last3rows,
            AVG("bezettingsgraad") over last3rows,
            -- MAX("recordTimestamp")
            MAX(UNIX_TIMESTAMP("recordTimestamp")) over last3rows
        FROM "INCOMING_STREAM"
        WINDOW
            last3rows AS (PARTITION BY "uniqueId" ROWS 3 PRECEDING);
        -- GROUP BY "uniqueId", STEP("INCOMING_STREAM"."recordTimestamp" BY  INTERVAL '3' SECOND);
        -- I cannot get this thumbling window working with my own timestamp field

        -- STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND);


-- Calculate the difference in speed between the current window and the previous one
-- Get previous speed
CREATE OR REPLACE STREAM "SPEED_CHANGE_SQL_STREAM" (
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT);

CREATE OR REPLACE PUMP "SPEED_CHANGE_PUMP" AS
    INSERT INTO "SPEED_CHANGE_SQL_STREAM"
        SELECT STREAM "s"."uniqueId",
            LAG("s"."speed", 1, -1) OVER CURRENT_WINDOW AS "previousSpeed",
            "s"."speed" AS "currentSpeed",
            "s"."bezettingsgraad",
            "s"."recordTimestamp"
        FROM "SPEED_SQL_STREAM" AS "s"
        WINDOW CURRENT_WINDOW AS (PARTITION BY "s"."uniqueId" ROWS 1 PRECEDING);


-- Calculate the difference in speed between the current window and the previous one
-- Calculate difference
CREATE OR REPLACE STREAM "SPEED_DIFF_SQL_STREAM" (
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "speedDiff" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT);

CREATE OR REPLACE PUMP "SPEED_DIFF_PUMP" AS
    INSERT INTO "SPEED_DIFF_SQL_STREAM"
        SELECT STREAM "uniqueId",
        "previousSpeed",
        "currentSpeed",
        ("currentSpeed" - "previousSpeed") AS "speedDiff",
        "bezettingsgraad",
        "recordTimestamp"
        FROM "SPEED_CHANGE_SQL_STREAM";


-- Indicate speed difference with -1 0 or 1
CREATE OR REPLACE STREAM "SPEED_DIFF_INDICATOR_SQL_STREAM" (
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "speedDiffIndicator" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT);


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
        "recordTimestamp"
        FROM "SPEED_DIFF_SQL_STREAM";


-- Publish data to output stream
CREATE OR REPLACE STREAM "OUTPUT_STREAM" (
    "outputType" VARCHAR(10),
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "speedDiffIndicator" INTEGER,
    "trafficJamIndicator" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT);

CREATE OR REPLACE PUMP "SPEED_DIFF_TO_OUTPUT_PUMP" AS
    INSERT INTO "OUTPUT_STREAM" ("outputType", "uniqueId", "previousSpeed", "currentSpeed", "speedDiffIndicator", "bezettingsgraad", "recordTimestamp")
        SELECT STREAM
        'SPEED_DIFFERENTIAL',
        "sdi"."uniqueId",
        "sdi"."previousSpeed",
        "sdi"."currentSpeed",
        "sdi"."speedDiffIndicator",
        "sdi"."bezettingsgraad",
        "sdi"."recordTimestamp"
        FROM "SPEED_DIFF_INDICATOR_SQL_STREAM" AS "sdi"
