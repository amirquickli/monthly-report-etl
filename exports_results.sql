/*
 EXPORTS RESULTS DATA PIPELINE - Modified for Tableau/Excel Compatibility (MotherDuck)
 ======================================================
 
 This SQL matches the logic from exports_results.js cube definition, adjusted for MotherDuck/DuckDB
 and Tableau/Excel compatibility. Ensures clean exportedLender values and excludes JSON columns.
 Uses placeholders {start_date}, {end_date}, and {lender_name} for dynamic substitution.
 
 Key Parameters:
 - Time range: Provided via {start_date} and {end_date} placeholders
 - Lender: Filtered via {lender_name} placeholder
*/
-- Step 1: Apply filters and extract lender-specific results from exported deals
WITH base AS (
    SELECT
        "time",
        "scenarioId",
        "results",
        COALESCE("exportedLender", '') AS "exportedLender",
        "primaryIncome",
        "rateType",
        "loanPurpose",
        "totalProposedLoanAmount",
        "applicantCount",
        "householdCount",
        "transactionType",
        "dependantsCount",
        "lvr",
        "lvrBucket",
        "applicantsWithHecs",
        "paygIncome",
        "weeklyRentalIncome",
        "selfEmployedIncome",
        CASE
            WHEN "exportedLender" IS NOT NULL THEN (
                array_filter(
                    "results" :: JSON [],
                    x -> json_extract_string(x, 'lenderName') = "exportedLender"
                )
            ) [1]
            ELSE NULL
        END AS exported_lender_result
    FROM
        quickli_labs.main."exports-deals-view"
    WHERE
        isValidExport = true
        AND "time" >= '{start_date}' :: TIMESTAMP WITH TIME ZONE
        AND "time" < '{end_date}' :: TIMESTAMP WITH TIME ZONE
),
-- Step 2: Deduplicate scenarios by taking the latest record per scenarioId
grouped_by_scenarioId AS (
    SELECT
        "scenarioId",
        MAX("time") as "time",
        MAX_BY("results", "time") as "results",
        MAX_BY(COALESCE("exportedLender", ''), "time") as "exportedLender",
        MAX_BY("primaryIncome", "time") as "primaryIncome",
        MAX_BY("rateType", "time") as "rateType",
        MAX_BY("loanPurpose", "time") as "loanPurpose",
        MAX_BY("totalProposedLoanAmount", "time") as "totalProposedLoanAmount",
        MAX_BY("applicantCount", "time") as "applicantCount",
        MAX_BY("householdCount", "time") as "householdCount",
        MAX_BY("transactionType", "time") as "transactionType",
        MAX_BY("dependantsCount", "time") as "dependantsCount",
        MAX_BY("lvr", "time") as "lvr",
        MAX_BY("lvrBucket", "time") as "lvrBucket",
        MAX_BY("applicantsWithHecs", "time") as "applicantsWithHecs",
        MAX_BY("paygIncome", "time") as "paygIncome",
        MAX_BY("weeklyRentalIncome", "time") as "weeklyRentalIncome",
        MAX_BY("selfEmployedIncome", "time") as "selfEmployedIncome",
        MAX_BY("exported_lender_result", "time") as "exported_lender_result",
                -- Create array of non-null exported lender results for secondary export analysis
        list_filter(
            list(exported_lender_result),
            x -> x IS NOT NULL
        ) as exportedLendersResults
    FROM
        base
    GROUP BY
        "scenarioId"
),
-- Step 3: Identify failed exports based on business rules
with_failing_export AS (
    SELECT
        *,
        CASE
            WHEN "exportedLender" = '' THEN true
            WHEN exported_lender_result IS NULL
            OR json_extract_string(exported_lender_result, 'doesService') = 'false'
            OR json_extract_string(exported_lender_result, 'maxBorrowingCapacity') IS NULL
            OR json_extract_string(exported_lender_result, 'maxBorrowingCapacity') = 'null' THEN true
            ELSE false
        END as failingExport
    FROM
        grouped_by_scenarioId
),
-- Step 4: Filter to only successful exports
harsh_filtered AS (
    SELECT
        *
    FROM
        with_failing_export
    WHERE
        failingExport = false
),
-- Step 5: Calculate global totals using window functions
with_global_calculations AS (
    SELECT
        *,
        COUNT(DISTINCT "scenarioId") OVER () as count_all_unique_scenario_id,
        COUNT(DISTINCT "scenarioId") OVER (PARTITION BY "loanPurpose") as count_all_loan_purpose,
        SUM("totalProposedLoanAmount") OVER () as sum_all_total_proposed_loan_amount
    FROM
        harsh_filtered
),
-- Step 6: Match scenarios with specific lender
lender_results AS (
    SELECT
        *,
        unnest(
            COALESCE(
                NULLIF(
                    array_filter(
                        results :: JSON [],
                        lender_record -> json_extract_string(lender_record, 'lenderName') = '{lender_name}'
                    ),
                    []
                ),
                [json_object('lenderName', '{lender_name}')]
            )
        ) as lender_result
    FROM
        with_global_calculations
),
-- Step 7: Extract performance metrics and lender information from JSON
performance_extracted AS (
    SELECT
        *,
        json_extract_string(lender_result, 'lenderName') as associated_lender,
        json_extract(lender_result, 'performance') as performance_json
    FROM
        lender_results
    WHERE
        json_extract_string(lender_result, 'lenderName') IS NOT NULL
),
-- Final output: Calculate performance metrics
performance_result AS (
    SELECT
        *,
        CASE
            WHEN associated_lender != exportedLender
            AND EXISTS (
                SELECT
                    1
                FROM
                    unnest(exportedLendersResults :: JSON []) AS t(exported_result)
                    -- unnest(array_filter(results :: JSON [], x -> json_extract_string(x, 'lenderName') = associated_lender)) AS t(exported_result)
                WHERE
                    json_extract_string(exported_result, 'lenderName') = associated_lender
                    AND json_extract_string(exported_result, 'doesService') = 'true'
                    AND json_extract_string(exported_result, 'maxBorrowingCapacity') IS NOT NULL
                    AND json_extract_string(exported_result, 'maxBorrowingCapacity') != 'null'
            ) THEN 'Secondary Export Deals'
            WHEN performance_json IS NULL THEN 'Not Available Scenarios'
            WHEN json_extract_string(performance_json, 'lenderFailedServicing') = 'true' THEN CASE
                WHEN json_extract_string(performance_json, 'lenderFailedInScope') = 'true' THEN 'Failed In Scope Deals'
                WHEN json_extract_string(performance_json, 'lenderFailedOutOfScope') = 'true' THEN 'Failed Out of Scope Deals'
                ELSE 'Unknown'
            END
            WHEN json_extract_string(performance_json, 'lenderPassedServicing') = 'true' THEN CASE
                WHEN json_extract_string(performance_json, 'lenderExportWinner') = 'true' THEN 'Export Winner Deals'
                ELSE 'Deals Not Exported'
            END
            ELSE 'Unknown'
        END as performance
    FROM
        performance_extracted
)
SELECT
    associated_lender,
    "applicantCount",
    "applicantsWithHecs",
    "dependantsCount",
    COALESCE("exportedLender", '') AS "exportedLender",
    "householdCount",
    "loanPurpose",
    "lvr",
    "lvrBucket",
    "paygIncome",
    "primaryIncome",
    "rateType",
    "scenarioId",
    "selfEmployedIncome",
    "time",
    "totalProposedLoanAmount",
    "transactionType",
    "weeklyRentalIncome",
    count_all_loan_purpose,
    count_all_unique_scenario_id,
    sum_all_total_proposed_loan_amount,
    performance
FROM
    performance_result
ORDER BY
    associated_lender, "scenarioId";

