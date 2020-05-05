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


-- find out if traffic is standing still
CREATE OR REPLACE STREAM "TRAFFIC_JAM_SQL_STREAM" (
    "uniqueId" INTEGER,
    "speed" INTEGER,
    "trafficJamIndicator" INTEGER,
    "recordTimestamp" BIGINT);

CREATE OR REPLACE PUMP "TRAFFIC_JAM_SQL_PUMP" AS
    INSERT INTO "TRAFFIC_JAM_SQL_STREAM"
        SELECT STREAM
            "uniqueId",
            "speed",
            CASE
                WHEN "speed" BETWEEN 0 AND 40 THEN 1
                WHEN "speed" BETWEEN 40 AND 200 THEN 0
                ELSE -1
            END AS "trafficJamIndicator",
            UNIX_TIMESTAMP("recordTimestamp") AS "recordTimestamp"
        FROM "INCOMING_STREAM";


-- Aggregate the speed over a 3 minute window for each unique Id (location)
CREATE OR REPLACE STREAM "SPEED_SQL_STREAM" (
    "uniqueId" INTEGER,
    "speed" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT,
    "avgSpeed2Minutes" INTEGER,
    "avgSpeed10Minutes" INTEGER);

CREATE OR REPLACE PUMP "STREAM_PUMP_SPEED" AS
    INSERT INTO "SPEED_SQL_STREAM"
        SELECT STREAM
            "uniqueId",
            AVG("speed") over W0,
            AVG("bezettingsgraad") over W0,
            MAX(UNIX_TIMESTAMP("recordTimestamp")) over W0,
            AVG("speed") over W2,
            AVG("speed") over W10
        FROM "INCOMING_STREAM"
        WINDOW
            W0 AS (PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE), "uniqueId" ROWS 0 PRECEDING),
            W2 AS (PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE), "uniqueId" ROWS 2 PRECEDING),
            W10 AS (PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE), "uniqueId" ROWS 10 PRECEDING);

        -- below is working
        -- WINDOWED BY STAGGER (
            -- PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE), "uniqueId" RANGE INTERVAL '2' MINUTE);
        -- WINDOW W1 AS (
        --     PARTITION BY FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE), "uniqueId"
        --     RANGE INTERVAL '2' MINUTE PRECEDING);

        -- not working yet
        -- GROUP BY "uniqueId", FLOOR("INCOMING_STREAM"."recordTimestamp" to MINUTE),
        --      STEP("INCOMING_STREAM"."recordTimestamp" BY INTERVAL '2' MINUTE);



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
            LAG("s"."speed", 1, "s"."speed") OVER CURRENT_WINDOW AS "previousSpeed",
            "s"."speed" AS "currentSpeed",
            "s"."bezettingsgraad",
            "s"."recordTimestamp"
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


--Create output stream 1
CREATE OR REPLACE STREAM "OUTPUT_STREAM" (
    "outputType" VARCHAR(20),
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "speedDiffIndicator" INTEGER,
    "trafficJamIndicator" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT,
    "location" VARCHAR(128));


-- Publish speed data to output stream
CREATE OR REPLACE PUMP "SPEED_DIFF_TO_OUTPUT_PUMP" AS
    INSERT INTO "OUTPUT_STREAM" ("outputType", "uniqueId", "previousSpeed", "currentSpeed", "speedDiffIndicator", "bezettingsgraad", "recordTimestamp", "location")
        SELECT STREAM
        'SPEED_DIFFERENTIAL',
        "sdi"."uniqueId",
        "sdi"."previousSpeed",
        "sdi"."currentSpeed",
        "sdi"."speedDiffIndicator",
        "sdi"."bezettingsgraad",
        "sdi"."recordTimestamp",
        "ml"."locatie"
        FROM "SPEED_DIFF_INDICATOR_SQL_STREAM" AS "sdi" LEFT JOIN "measurementLocations" as "ml"
        ON "sdi"."uniqueId" = "ml"."id";


-- Publish traffic jam data to output stream
CREATE OR REPLACE PUMP "TRAFFIC_JAM_TO_OUTPUT_PUMP" AS
    INSERT INTO "OUTPUT_STREAM" ("outputType", "uniqueId", "currentSpeed", "trafficJamIndicator", "recordTimestamp", "location")
        SELECT STREAM
        'TRAFFIC_JAM',
        "tjs"."uniqueId",
        "tjs"."speed",
        "tjs"."trafficJamIndicator",
        "tjs"."recordTimestamp",
        "ml"."locatie"
        FROM "TRAFFIC_JAM_SQL_STREAM" AS "tjs" LEFT JOIN "measurementLocations" as "ml"
        ON "tjs"."uniqueId" = "ml"."id";

   


--Create output stream 2
CREATE OR REPLACE STREAM "OUTPUT_STREAM_2" (
    "outputType" VARCHAR(20),
    "uniqueId" INTEGER,
    "previousSpeed" INTEGER,
    "currentSpeed" INTEGER,
    "speedDiffIndicator" INTEGER,
    "trafficJamIndicator" INTEGER,
    "bezettingsgraad" INTEGER,
    "recordTimestamp" BIGINT);


-- Publish speed data to output stream
CREATE OR REPLACE PUMP "OUTPUT_STREAM_2_SPEED_DIFF_PUMP" AS
    INSERT INTO "OUTPUT_STREAM_2" ("outputType", "uniqueId", "previousSpeed", "currentSpeed", "speedDiffIndicator", "bezettingsgraad", "recordTimestamp")
        SELECT STREAM
        'SPEED_DIFFERENTIAL',
        "sdi"."uniqueId",
        "sdi"."previousSpeed",
        "sdi"."currentSpeed",
        "sdi"."speedDiffIndicator",
        "sdi"."bezettingsgraad",
        "sdi"."recordTimestamp"
        FROM "SPEED_DIFF_INDICATOR_SQL_STREAM" AS "sdi";


-- Publish traffic jam data to output stream
CREATE OR REPLACE PUMP "OUTPUT_STREAM_2_TRAFFIC_JAM_PUMP" AS
    INSERT INTO "OUTPUT_STREAM_2" ("outputType", "uniqueId", "currentSpeed", "trafficJamIndicator", "recordTimestamp")
        SELECT STREAM
        'TRAFFIC_JAM',
        "tjs"."uniqueId",
        "tjs"."speed",
        "tjs"."trafficJamIndicator",
        "tjs"."recordTimestamp"
        FROM "TRAFFIC_JAM_SQL_STREAM" AS "tjs";
        
        

