CREATE
OR REPLACE PROCEDURE sp_niis_table_itemlvl_fi AS

BEGIN

/******************************************************************************

NAME:       sp_niis_table_itemlvl_fi
PURPOSE:    niis table item level data 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        01/13/2026             Francis           1. Create sp_niis_table_itemlvl_fi

NOTES:

******************************************************************************/

-- =============================================================================
-- STEP 1: Initial INSERT - Only essential data from niis table - 13s
-- =============================================================================


INSERT INTO temp_niis_item_data (
            co_cd,line_pref,subline_pref,iss_pref,pol_yy,
            pol_seq_no,ren_seq_no,pol_type,clm_yy,clm_seq_no,
            loss_dt,loss_desc,loss_det,event_no,loss_cat_cd,curr_cd,
            clm_stat_cd,item_no,org_type,itm_loss_reserve,itm_expense_reserve,
            itm_loss_paid,itm_expense_paid,policy_number,claim_number
)
SELECT              
                    c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no,c.loss_dt,c.loss_desc, c.loss_det,
                    c.event_no,c.loss_cat_cd,c.curr_cd, c.clm_stat_cd,
                    a.item_no,c.org_type,

                    SUM(NVL(a.clm_shr_ramt,0)) itm_loss_reserve,
                    SUM(NVL(a.exp_shr_ramt,0)) itm_expense_reserve,
                    SUM(NVL(a.clm_pd_amt,0)) itm_loss_paid,
                    SUM(NVL(a.exp_pd_amt,0)) itm_expense_paid,

                    c.line_pref||'-'||c.subline_pref||'-'||
                    c.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c.pol_yy),3,2)))||'-'||
                    LTRIM(RTRIM(TO_CHAR(c.pol_seq_no,'0999999')))||'-'||
                    LTRIM(RTRIM(TO_CHAR(c.ren_seq_no,'09')))||'-'||c.pol_type as policy_number,
    
                    c.line_pref||'-'||c.subline_pref||'-'||
                        c.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c.clm_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c.clm_seq_no,'09999'))) as claim_number

               FROM niis_clmprlds a, niis_clm_hist b, niis_claims c
               WHERE a.co_cd = b.co_cd
               AND 'FI' in (a.schema_name,b.schema_name,c.schema_name) --schema 
               AND a.line_pref = b.line_pref
               AND a.subline_pref = b.subline_pref
               AND a.iss_pref = b.iss_pref
               AND a.clm_yy = b.clm_yy
               AND a.clm_seq_no = b.clm_seq_no
               AND a.hist_seq_no = b.hist_seq_no
               AND b.co_cd = c.co_cd
               AND b.line_pref = c.line_pref
               AND b.subline_pref = c.subline_pref
               AND b.iss_pref = c.iss_pref
               AND b.clm_yy = c.clm_yy
               AND b.clm_seq_no = c.clm_seq_no
               AND b.fla_stat_cd != 'C'
               AND a.hist_seq_no = (SELECT MAX(d.hist_seq_no)
                                       FROM niis_clm_hist d, niis_clmprlds c
                                       WHERE d.co_cd = c.co_cd
                                       AND 'FI' in (c.schema_name,d.schema_name)
                                       AND d.line_pref = c.line_pref
                                       AND d.subline_pref = c.subline_pref
                                       AND d.iss_pref = c.iss_pref
                                       AND d.clm_yy = c.clm_yy
                                       AND d.clm_seq_no = c.clm_seq_no
                                       AND d.hist_seq_no = c.hist_seq_no
                                       AND d.fla_stat_cd != 'C'
                                       AND c.co_cd = a.co_cd
                                       AND c.line_pref = a.line_pref
                                       AND c.subline_pref = a.subline_pref
                                       AND c.iss_pref = a.iss_pref
                                       AND c.clm_yy = a.clm_yy
                                       AND c.clm_seq_no = a.clm_seq_no
                                       AND c.item_no = a.item_no
                                       AND c.peril_cd = a.peril_cd)
         GROUP BY c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no,c.loss_dt,c.loss_desc,c.loss_det,
                    c.event_no,c.loss_cat_cd,c.curr_cd,c.clm_stat_cd,
                    a.item_no, c.org_type
         ORDER BY   c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no;


commit;

-- =============================================================================
-- STEP 2: UPDATE - UPDATE ITEM title - 14.5s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        co_cd,
        line_pref,
        subline_pref,
        iss_pref,
        pol_yy,
        pol_seq_no,
        ren_seq_no,
        pol_type,
        item_no,
        item_title
    FROM (
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            a.pol_type,
            b.item_no,
            b.item_title,
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd,
                    a.line_pref,
                    a.subline_pref,
                    a.iss_pref,
                    a.pol_yy,
                    a.pol_seq_no,
                    a.ren_seq_no,
                    a.pol_type,
                    b.item_no
                ORDER BY a.eff_dt DESC
            ) rn
        FROM niis_polbasic a
        JOIN niis_item b
          ON a.pol_rec_no = b.pol_rec_no
        WHERE NVL(a.pol_stat,'X') <> '5'
          AND b.item_title IS NOT NULL
          AND a.schema_name = 'FI'
          AND b.schema_name = 'FI'
    )
    WHERE rn = 1
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.item_title = src.item_title
    where main.line_pref = 'FI';


COMMIT;

-- =============================================================================
-- STEP 3: UPDATE - UPDATE ITEM DESCRIPTION
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        co_cd,
        line_pref,
        subline_pref,
        iss_pref,
        pol_yy,
        pol_seq_no,
        ren_seq_no,
        pol_type,
        item_no,
        item_desc
    FROM (
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            a.pol_type,
            b.item_no,
            b.item_desc,
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd,
                    a.line_pref,
                    a.subline_pref,
                    a.iss_pref,
                    a.pol_yy,
                    a.pol_seq_no,
                    a.ren_seq_no,
                    a.pol_type,
                    b.item_no
                ORDER BY a.eff_dt DESC
            ) rn
        FROM niis_polbasic a
        JOIN niis_item b
          ON a.pol_rec_no = b.pol_rec_no
        WHERE NVL(a.pol_stat,'X') <> '5'
          AND b.item_desc IS NOT NULL
          AND a.schema_name = 'FI'
          AND b.schema_name = 'FI'
    )
    WHERE rn = 1
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.item_desc = src.item_desc
    where main.line_pref = 'FI';

COMMIT;


-- =============================================================================
-- STEP 4: UPDATE - UPDATE POLICY GROSS SUM INSURED
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        SUM(NVL(b.shr_tsi, 0)) AS pol_gross_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
        ON b.co_cd = a.co_cd
       AND b.line_pref = a.line_pref
       AND b.dist_no = a.dist_no
    WHERE a.dist_stat = '3'
      AND a.redist_stat IN (1, 3)
      AND a.acct_neg_dt IS NULL
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type
) s
ON (
       t.co_cd = s.co_cd
   AND t.line_pref = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref = s.iss_pref
   AND t.pol_yy = s.pol_yy
   AND t.pol_seq_no = s.pol_seq_no
   AND t.ren_seq_no = s.ren_seq_no
   AND t.pol_type = s.pol_type
)
WHEN MATCHED THEN
UPDATE SET t.pol_gross_sum_insured = s.pol_gross_sum_insured
where t.line_pref = 'FI';

COMMIT;

UPDATE temp_niis_item_data
SET pol_gross_sum_insured = 0
WHERE pol_gross_sum_insured IS NULL
and line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 5: UPDATE - UPDATE POLICY RETAINED SUM INSURED
-- =============================================================================
MERGE INTO temp_niis_item_data t
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        SUM(NVL(b.shr_tsi, 0)) AS pol_retained_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
        ON b.co_cd = a.co_cd
       AND b.line_pref = a.line_pref
       AND b.dist_no = a.dist_no
    WHERE a.dist_stat = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type = 'N'
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type
) s
ON (
       t.co_cd = s.co_cd
   AND t.line_pref = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref = s.iss_pref
   AND t.pol_yy = s.pol_yy
   AND t.pol_seq_no = s.pol_seq_no
   AND t.ren_seq_no = s.ren_seq_no
   AND t.pol_type = s.pol_type
)
WHEN MATCHED THEN
UPDATE SET t.pol_retained_sum_insured = s.pol_retained_sum_insured
where t.line_pref = 'FI';

COMMIT;

UPDATE temp_niis_item_data
SET pol_retained_sum_insured = 0
WHERE pol_retained_sum_insured IS NULL
and line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 6: UPDATE - UPDATE POLICY QS SUM INSURED
-- =============================================================================
MERGE INTO temp_niis_item_data t
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        SUM(NVL(b.shr_tsi, 0)) AS pol_qs_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
        ON b.co_cd = a.co_cd
       AND b.line_pref = a.line_pref
       AND b.dist_no = a.dist_no
    WHERE a.dist_stat = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type = 'T'
      AND EXISTS (
          SELECT 1
          FROM niis_outreaty x
          WHERE x.co_cd = b.co_cd
            AND x.line_pref = b.line_pref
            AND x.ri_cd = b.ri_cd
            AND x.ca_trty_type = 3
      )
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type
) s
ON (
       t.co_cd = s.co_cd
   AND t.line_pref = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref = s.iss_pref
   AND t.pol_yy = s.pol_yy
   AND t.pol_seq_no = s.pol_seq_no
   AND t.ren_seq_no = s.ren_seq_no
   AND t.pol_type = s.pol_type
)
WHEN MATCHED THEN
UPDATE SET t.pol_qs_sum_insured = s.pol_qs_sum_insured
where t.line_pref = 'FI';

COMMIT;

UPDATE temp_niis_item_data
SET pol_qs_sum_insured = 0
WHERE pol_qs_sum_insured IS NULL
and line_pref = 'FI';

COMMIT;


-- =============================================================================
-- STEP 7: UPDATE - UPDATE POLICY SURPLUS SUM INSURED
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        SUM(NVL(b.shr_tsi, 0)) AS pol_surplus_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
        ON b.co_cd = a.co_cd
       AND b.line_pref = a.line_pref
       AND b.dist_no = a.dist_no
    WHERE a.dist_stat = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type = 'T'
      AND EXISTS (
          SELECT 1
          FROM niis_outreaty x
          WHERE x.co_cd = b.co_cd
            AND x.line_pref = b.line_pref
            AND x.ri_cd = b.ri_cd
            AND x.ca_trty_type IN (1,2)
      )
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type
) s
ON (
       t.co_cd = s.co_cd
   AND t.line_pref = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref = s.iss_pref
   AND t.pol_yy = s.pol_yy
   AND t.pol_seq_no = s.pol_seq_no
   AND t.ren_seq_no = s.ren_seq_no
   AND t.pol_type = s.pol_type
)
WHEN MATCHED THEN
UPDATE SET t.pol_surplus_sum_insured = s.pol_surplus_sum_insured
where t.line_pref = 'FI';

COMMIT;

UPDATE temp_niis_item_data
SET pol_surplus_sum_insured = 0
WHERE pol_surplus_sum_insured IS NULL
and line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 8: UPDATE - UPDATE POLICY PMMSC SUM INSURED
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        SUM(NVL(b.shr_tsi,0))  AS pol_pmmsc_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
        ON b.co_cd = a.co_cd
       AND b.line_pref = a.line_pref
       AND b.dist_no = a.dist_no
    WHERE a.dist_stat = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type = 'T'
      AND EXISTS (
          SELECT 1
          FROM niis_outreaty x
          WHERE x.co_cd = b.co_cd
            AND x.line_pref = b.line_pref
            AND x.ri_cd = b.ri_cd
            AND x.ca_trty_type = 10
      )
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type
) s
ON (
       t.co_cd = s.co_cd
   AND t.line_pref = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref = s.iss_pref
   AND t.pol_yy = s.pol_yy
   AND t.pol_seq_no = s.pol_seq_no
   AND t.ren_seq_no = s.ren_seq_no
   AND t.pol_type = s.pol_type
)
WHEN MATCHED THEN
UPDATE SET t.pol_pmmsc_sum_insured  = s.pol_pmmsc_sum_insured
where t.line_pref = 'FI';

COMMIT;

UPDATE temp_niis_item_data
SET pol_pmmsc_sum_insured = 0
WHERE pol_pmmsc_sum_insured IS NULL
and line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 9: UPDATE - UPDATE POLICY OTHER TREATY SUM INSURED
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        SUM(NVL(b.shr_tsi, 0)) AS pol_other_treaty_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
        ON b.co_cd = a.co_cd
       AND b.line_pref = a.line_pref
       AND b.dist_no = a.dist_no
    WHERE a.dist_stat = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type = 'T'
      AND EXISTS (
          SELECT 1
          FROM niis_outreaty x
          WHERE x.co_cd = b.co_cd
            AND x.line_pref = b.line_pref
            AND x.ri_cd = b.ri_cd
            AND x.ca_trty_type NOT IN (1,2,3,10)
      )
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type
) s
ON (
       t.co_cd = s.co_cd
   AND t.line_pref = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref = s.iss_pref
   AND t.pol_yy = s.pol_yy
   AND t.pol_seq_no = s.pol_seq_no
   AND t.ren_seq_no = s.ren_seq_no
   AND t.pol_type = s.pol_type
)
WHEN MATCHED THEN
UPDATE SET t.pol_other_treaty_sum_insured = s.pol_other_treaty_sum_insured
where t.line_pref = 'FI';

COMMIT;

UPDATE temp_niis_item_data
SET pol_other_treaty_sum_insured = 0
WHERE pol_other_treaty_sum_insured IS NULL
and line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 10: UPDATE - UPDATE POLICY FACUL SUM INSURED
-- =============================================================================
MERGE INTO temp_niis_item_data t
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        SUM(NVL(b.shr_tsi, 0)) AS pol_facul_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
        ON b.co_cd = a.co_cd
       AND b.line_pref = a.line_pref
       AND b.dist_no = a.dist_no
    WHERE a.dist_stat = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type = 'F'
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type
) s
ON (
       t.co_cd = s.co_cd
   AND t.line_pref = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref = s.iss_pref
   AND t.pol_yy = s.pol_yy
   AND t.pol_seq_no = s.pol_seq_no
   AND t.ren_seq_no = s.ren_seq_no
   AND t.pol_type = s.pol_type
)
WHEN MATCHED THEN
UPDATE SET t.pol_facul_sum_insured = s.pol_facul_sum_insured
where t.line_pref = 'FI';


COMMIT;

UPDATE temp_niis_item_data
SET pol_facul_sum_insured = 0
WHERE pol_facul_sum_insured IS NULL
and line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 11: UPDATE - UPDATE ITEM GROSS SUM INSURED 44s
-- =============================================================================
MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no,
        SUM(NVL(c.tsi_amt, 0)  * NVL(b.shr_pct, 0) / 100) AS itm_gross_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
      ON b.co_cd     = a.co_cd
     AND b.line_pref = a.line_pref
     AND b.dist_no   = a.dist_no
    JOIN niis_itemsrel c
      ON c.co_cd     = a.co_cd
     AND c.line_pref = a.line_pref
     AND c.dist_no   = a.dist_no
    WHERE a.dist_stat    = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND 'FI' in (a.schema_name,b.schema_name,c.schema_name) --schema
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.itm_gross_sum_insured  = NVL(src.itm_gross_sum_insured, 0)
    where main.line_pref = 'FI';

UPDATE temp_niis_item_data
SET itm_gross_sum_insured = 0
WHERE itm_gross_sum_insured IS NULL
and line_pref = 'FI';      

COMMIT;

-- =============================================================================
-- STEP 12: UPDATE - UPDATE ITEM RETAINED SUM INSURED 34.5s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no,
        SUM(NVL(c.tsi_amt, 0)  * NVL(b.shr_pct, 0) / 100) AS itm_retained_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
      ON b.co_cd     = a.co_cd
     AND b.line_pref = a.line_pref
     AND b.dist_no   = a.dist_no
    JOIN niis_itemsrel c
      ON c.co_cd     = a.co_cd
     AND c.line_pref = a.line_pref
     AND c.dist_no   = a.dist_no
    WHERE a.dist_stat    = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type = 'N'
      AND 'FI' in (a.schema_name,b.schema_name,c.schema_name) --schema
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.itm_retained_sum_insured  = NVL(src.itm_retained_sum_insured, 0)
    where main.line_pref = 'FI';

UPDATE temp_niis_item_data
SET itm_retained_sum_insured = 0
WHERE itm_retained_sum_insured IS NULL
and line_pref = 'FI';    

COMMIT;


-- =============================================================================
-- STEP 13: UPDATE - UPDATE ITEM QS SUM INSURED 14.5s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no,
        SUM(NVL(c.tsi_amt,0) * NVL(b.shr_pct,0) / 100) AS itm_qs_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
      ON b.co_cd     = a.co_cd
     AND b.line_pref = a.line_pref
     AND b.dist_no   = a.dist_no
    JOIN niis_itemsrel c
      ON c.co_cd     = a.co_cd
     AND c.line_pref = a.line_pref
     AND c.dist_no   = a.dist_no
    WHERE a.dist_stat    = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type    = 'T'
      AND 'FI' in (a.schema_name,b.schema_name,c.schema_name) --schema
      AND EXISTS (
          SELECT 1
          FROM niis_outreaty x
          WHERE x.co_cd        = b.co_cd
            AND x.line_pref    = b.line_pref
            AND x.ri_cd        = b.ri_cd
            AND x.ca_trty_type = 3
      )
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.itm_qs_sum_insured = NVL(src.itm_qs_sum_insured, 0)
    where main.line_pref = 'FI';

UPDATE temp_niis_item_data
SET itm_qs_sum_insured = 0
WHERE itm_qs_sum_insured IS NULL
and line_pref = 'FI';    

COMMIT;

-- =============================================================================
-- STEP 14: UPDATE - UPDATE ITEM SURPLUS SUM INSURE 15.8s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no,
        SUM(NVL(c.tsi_amt,0) * NVL(b.shr_pct,0) / 100) AS itm_surplus_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
      ON b.co_cd     = a.co_cd
     AND b.line_pref = a.line_pref
     AND b.dist_no   = a.dist_no
    JOIN niis_itemsrel c
      ON c.co_cd     = a.co_cd
     AND c.line_pref = a.line_pref
     AND c.dist_no   = a.dist_no
    WHERE a.dist_stat    = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type    = 'T'
      AND 'FI' in (a.schema_name,b.schema_name,c.schema_name) --schema
      AND EXISTS (
          SELECT 1
          FROM niis_outreaty x
          WHERE x.co_cd        = b.co_cd
            AND x.line_pref    = b.line_pref
            AND x.ri_cd        = b.ri_cd
            AND x.ca_trty_type IN (1,2)   -- SURPLUS
      )
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.itm_surplus_sum_insured = NVL(src.itm_surplus_sum_insured, 0)
    where main.line_pref = 'FI';

UPDATE temp_niis_item_data
SET itm_surplus_sum_insured = 0
WHERE itm_surplus_sum_insured IS NULL
and line_pref = 'FI';   


COMMIT;

-- =============================================================================
-- STEP 15: UPDATE - UPDATE ITEM PMMSC SUM INSURE 14.3s 
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no,
        SUM(NVL(c.tsi_amt,0) * NVL(b.shr_pct,0) / 100) AS itm_pmmsc_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
      ON b.co_cd     = a.co_cd
     AND b.line_pref = a.line_pref
     AND b.dist_no   = a.dist_no
    JOIN niis_itemsrel c
      ON c.co_cd     = a.co_cd
     AND c.line_pref = a.line_pref
     AND c.dist_no   = a.dist_no
    WHERE a.dist_stat    = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type    = 'T'
      AND 'FI' in (a.schema_name,b.schema_name,c.schema_name) --schema
      AND EXISTS (
          SELECT 1
          FROM niis_outreaty x
          WHERE x.co_cd        = b.co_cd
            AND x.line_pref    = b.line_pref
            AND x.ri_cd        = b.ri_cd
            AND x.ca_trty_type = 10    -- PMMSC
      )
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.itm_pmmsc_sum_insured = NVL(src.itm_pmmsc_sum_insured, 0)
    where main.line_pref = 'FI'
    ;

UPDATE temp_niis_item_data
SET itm_pmmsc_sum_insured = 0
WHERE itm_pmmsc_sum_insured IS NULL
and line_pref = 'FI';   

COMMIT;

-- =============================================================================
-- STEP 16: UPDATE - UPDATE ITEM OTHER TREATY SUM INSURED 12s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no,
        SUM(NVL(c.tsi_amt,0) * NVL(b.shr_pct,0) / 100) AS itm_other_treaty_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
      ON b.co_cd     = a.co_cd
     AND b.line_pref = a.line_pref
     AND b.dist_no   = a.dist_no
    JOIN niis_itemsrel c
      ON c.co_cd     = a.co_cd
     AND c.line_pref = a.line_pref
     AND c.dist_no   = a.dist_no
    WHERE a.dist_stat    = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type    = 'T'
      AND 'FI' in (a.schema_name,b.schema_name,c.schema_name) --schema
      AND EXISTS (
          SELECT 1
          FROM niis_outreaty x
          WHERE x.co_cd     = b.co_cd
            AND x.line_pref = b.line_pref
            AND x.ri_cd     = b.ri_cd
            AND x.ca_trty_type NOT IN (1,2,3,10)
      )
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.itm_other_treaty_sum_insured = NVL(src.itm_other_treaty_sum_insured, 0)
    where main.line_pref = 'FI';


UPDATE temp_niis_item_data
SET itm_other_treaty_sum_insured = 0
WHERE itm_other_treaty_sum_insured IS NULL
and line_pref = 'FI'; 

COMMIT;


-- =============================================================================
-- STEP 17: UPDATE - UPDATE ITEM FACUL SUM INSURED 16.7s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no,
        SUM(NVL(c.tsi_amt,0) * NVL(b.shr_pct,0) / 100) AS itm_facul_sum_insured
    FROM niis_pol_dist a
    JOIN niis_policyds b
      ON b.co_cd     = a.co_cd
     AND b.line_pref = a.line_pref
     AND b.dist_no   = a.dist_no
    JOIN niis_itemsrel c
      ON c.co_cd     = a.co_cd
     AND c.line_pref = a.line_pref
     AND c.dist_no   = a.dist_no
    WHERE a.dist_stat    = '3'
      AND a.redist_stat IN (1,3)
      AND a.acct_neg_dt IS NULL
      AND b.shr_type    = 'F'   -- Facultative
      AND 'FI' in (a.schema_name,b.schema_name,c.schema_name) --schema
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        c.item_no
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND main.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    main.itm_facul_sum_insured = NVL(src.itm_facul_sum_insured, 0)
    where main.line_pref = 'FI';

UPDATE temp_niis_item_data
SET itm_facul_sum_insured = 0
WHERE itm_facul_sum_insured IS NULL
and line_pref = 'FI'; 

COMMIT;    

-- =============================================================================
-- STEP 18: UPDATE - UPDATE item recovery
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.clm_yy,
        a.clm_seq_no,
        a.item_no,
        SUM(a.loss_adv_amt) AS recoveries
    FROM niis_clm_loss a
    JOIN niis_clm_hist b
      ON b.co_cd        = a.co_cd
     AND b.line_pref    = a.line_pref
     AND b.subline_pref = a.subline_pref
     AND b.iss_pref     = a.iss_pref
     AND b.clm_yy       = a.clm_yy
     AND b.clm_seq_no   = a.clm_seq_no
     AND b.hist_seq_no  = a.hist_seq_no

    WHERE b.fla_stat_cd <> 'C'
      AND a.loss_adv_amt < 0
      AND 'FI' in (a.schema_name,b.schema_name) --schema
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.clm_yy,
        a.clm_seq_no,
        a.item_no
) s
ON (
       t.co_cd       = s.co_cd
   AND t.line_pref   = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref   = s.iss_pref
   AND t.clm_yy     = s.clm_yy
   AND t.clm_seq_no = s.clm_seq_no
   AND t.item_no    = s.item_no
)
WHEN MATCHED THEN
UPDATE SET t.itm_recoveries = s.recoveries
where t.line_pref = 'FI';

UPDATE temp_niis_item_data
SET itm_recoveries = 0
WHERE itm_recoveries IS NULL
and line_pref = 'FI';

COMMIT;


-- =============================================================================
-- STEP 19: UPDATE - PROCESS INCEPT DATE & EXPIRY DATE 30.6s
-- =============================================================================

MERGE INTO temp_niis_item_data t --incept
USING (
    SELECT 
        co_cd, line_pref, subline_pref, iss_pref,
        pol_yy, pol_seq_no, ren_seq_no,
        incept_dt
    FROM niis_max_incept_dt
    
    UNION ALL
    
    SELECT 
        pb.co_cd, pb.line_pref, pb.subline_pref, pb.iss_pref,
        pb.pol_yy, pb.pol_seq_no, pb.ren_seq_no,
        pb.incept_dt
    FROM niis_polbasic pb
    WHERE pb.endt_seq_no = 0
    AND pb.schema_name = 'FI' --schema 
    AND NOT EXISTS (
        SELECT 1 FROM niis_max_incept_dt mid
        WHERE mid.co_cd = pb.co_cd
        AND mid.line_pref = pb.line_pref
        AND mid.subline_pref = pb.subline_pref
        AND mid.iss_pref = pb.iss_pref
        AND mid.pol_yy = pb.pol_yy
        AND mid.pol_seq_no = pb.pol_seq_no
        AND mid.ren_seq_no = pb.ren_seq_no
    )
) s
ON (
    t.co_cd = s.co_cd
    AND t.line_pref = s.line_pref
    AND t.subline_pref = s.subline_pref
    AND t.iss_pref = s.iss_pref
    AND t.pol_yy = s.pol_yy
    AND t.pol_seq_no = s.pol_seq_no
    AND t.ren_seq_no = s.ren_seq_no
)
WHEN MATCHED THEN
    UPDATE SET t.incept_date = s.incept_dt
    where t.line_pref = 'FI';


COMMIT;    


MERGE INTO temp_niis_item_data t --expiry
USING (
    SELECT 
        co_cd, line_pref, subline_pref, iss_pref,
        pol_yy, pol_seq_no, ren_seq_no,
        expiry_dt
    FROM niis_max_expiry_dt
    
    UNION ALL
    
    SELECT 
        pb.co_cd, pb.line_pref, pb.subline_pref, pb.iss_pref,
        pb.pol_yy, pb.pol_seq_no, pb.ren_seq_no,
        pb.expiry_dt
    FROM niis_polbasic pb
    WHERE pb.endt_seq_no = 0
    AND pb.schema_name = 'FI' --schema 
    AND NOT EXISTS (
        SELECT 1 FROM niis_max_expiry_dt mid
        WHERE mid.co_cd = pb.co_cd
        AND mid.line_pref = pb.line_pref
        AND mid.subline_pref = pb.subline_pref
        AND mid.iss_pref = pb.iss_pref
        AND mid.pol_yy = pb.pol_yy
        AND mid.pol_seq_no = pb.pol_seq_no
        AND mid.ren_seq_no = pb.ren_seq_no
    )
) s
ON (
    t.co_cd = s.co_cd
    AND t.line_pref = s.line_pref
    AND t.subline_pref = s.subline_pref
    AND t.iss_pref = s.iss_pref
    AND t.pol_yy = s.pol_yy
    AND t.pol_seq_no = s.pol_seq_no
    AND t.ren_seq_no = s.ren_seq_no
)
WHEN MATCHED THEN
    UPDATE SET t.expiry_date = s.expiry_dt
    where t.line_pref = 'FI';


COMMIT;  


-- =============================================================================
-- STEP 20: UPDATE - PROCESS OCCUPANCY - 589s to merge 32s
-- =============================================================================


EXECUTE IMMEDIATE 'TRUNCATE TABLE temp_item_lookup';
EXECUTE IMMEDIATE 'TRUNCATE TABLE temp_occ_lookup';
EXECUTE IMMEDIATE 'TRUNCATE TABLE temp_max_eff_dt';

INSERT INTO temp_item_lookup
SELECT 
    b.co_cd,
    b.line_pref,
    b.subline_pref,
    b.iss_pref,
    b.clm_yy,
    b.clm_seq_no,
    MIN(b.item_no) AS item_no
FROM niis_clm_loss b
WHERE b.hist_seq_no = (
    SELECT MAX(x.hist_seq_no)
    FROM niis_clm_hist x
    WHERE b.co_cd = x.co_cd
    AND b.line_pref = x.line_pref
    AND b.subline_pref = x.subline_pref
    AND b.iss_pref = x.iss_pref
    AND b.clm_yy = x.clm_yy
    AND b.clm_seq_no = x.clm_seq_no
    AND x.fla_stat_cd != 'C'
    AND 'FI' in (x.schema_name)
    
)
AND 'FI' in (b.schema_name)
GROUP BY b.co_cd, b.line_pref, b.subline_pref, b.iss_pref, b.clm_yy, b.clm_seq_no;

COMMIT;

INSERT INTO temp_max_eff_dt
SELECT 
    u.co_cd,
    u.line_pref,
    u.subline_pref,
    u.iss_pref,
    u.pol_yy,
    u.pol_seq_no,
    u.ren_seq_no,
    v.item_no,
    MAX(u.eff_dt) AS max_eff_dt
FROM niis_polbasic u
INNER JOIN niis_fireitem v ON u.pol_rec_no = v.pol_rec_no
WHERE NVL(u.pol_stat,'0') != '5'
AND v.occ_cd IS NOT NULL
AND u.schema_name = 'FI' --schema
AND EXISTS (
    SELECT 1 FROM temp_niis_item_data t
    WHERE t.co_cd = u.co_cd
    AND t.line_pref = u.line_pref
    AND t.subline_pref = u.subline_pref
    AND t.iss_pref = u.iss_pref
    AND t.pol_yy = u.pol_yy
    AND t.pol_seq_no = u.pol_seq_no
    AND t.ren_seq_no = u.ren_seq_no
)
GROUP BY u.co_cd, u.line_pref, u.subline_pref, u.iss_pref, 
         u.pol_yy, u.pol_seq_no, u.ren_seq_no, v.item_no;

COMMIT;

INSERT INTO temp_occ_lookup
SELECT DISTINCT
    a.co_cd,
    a.line_pref,
    a.subline_pref,
    a.iss_pref,
    a.pol_yy,
    a.pol_seq_no,
    a.ren_seq_no,
    til.clm_yy,
    til.clm_seq_no,
    b.occ_cd
FROM temp_niis_item_data main
INNER JOIN temp_item_lookup til
    ON til.co_cd = main.co_cd
    AND til.line_pref = main.line_pref
    AND til.subline_pref = main.subline_pref
    AND til.iss_pref = main.iss_pref
    AND til.clm_yy = main.clm_yy
    AND til.clm_seq_no = main.clm_seq_no
INNER JOIN temp_max_eff_dt med
    ON med.co_cd = main.co_cd
    AND med.line_pref = main.line_pref
    AND med.subline_pref = main.subline_pref
    AND med.iss_pref = main.iss_pref
    AND med.pol_yy = main.pol_yy
    AND med.pol_seq_no = main.pol_seq_no
    AND med.ren_seq_no = main.ren_seq_no
    AND med.item_no = til.item_no
INNER JOIN niis_polbasic a
    ON a.co_cd = main.co_cd
    AND a.line_pref = main.line_pref
    AND a.subline_pref = main.subline_pref
    AND a.iss_pref = main.iss_pref
    AND a.pol_yy = main.pol_yy
    AND a.pol_seq_no = main.pol_seq_no
    AND a.ren_seq_no = main.ren_seq_no
    AND a.eff_dt = med.max_eff_dt
    AND NVL(a.pol_stat,'0') != '5'
    AND a.schema_name = 'FI'
INNER JOIN niis_fireitem b
    ON b.pol_rec_no = a.pol_rec_no
    AND b.item_no = til.item_no;

COMMIT;


-- UPDATE temp_niis_item_data t
-- SET t.occupancy = (
--     SELECT occ.occ_title
--     FROM temp_occ_lookup tol
--     INNER JOIN niis_occupancy occ
--         ON occ.co_cd = tol.co_cd
--         AND occ.occ_cd = tol.occ_cd
--     WHERE tol.co_cd = t.co_cd
--         AND tol.line_pref = t.line_pref
--         AND tol.subline_pref = t.subline_pref
--         AND tol.iss_pref = t.iss_pref
--         AND tol.pol_yy = t.pol_yy
--         AND tol.pol_seq_no = t.pol_seq_no
--         AND tol.ren_seq_no = t.ren_seq_no
--         AND tol.clm_yy = t.clm_yy
--         AND tol.clm_seq_no = t.clm_seq_no
--         AND ROWNUM = 1
-- );

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        tol.co_cd,
        tol.line_pref,
        tol.subline_pref,
        tol.iss_pref,
        tol.pol_yy,
        tol.pol_seq_no,
        tol.ren_seq_no,
        tol.clm_yy,
        tol.clm_seq_no,
        MAX(occ.occ_title) AS occ_title
    FROM temp_occ_lookup tol
    JOIN niis_occupancy occ
      ON occ.co_cd = tol.co_cd
     AND occ.occ_cd = tol.occ_cd
    GROUP BY
        tol.co_cd,
        tol.line_pref,
        tol.subline_pref,
        tol.iss_pref,
        tol.pol_yy,
        tol.pol_seq_no,
        tol.ren_seq_no,
        tol.clm_yy,
        tol.clm_seq_no
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
   AND t.clm_yy       = src.clm_yy
   AND t.clm_seq_no   = src.clm_seq_no
)
WHEN MATCHED THEN
UPDATE SET
    t.occupancy = src.occ_title
    where t.line_pref = 'FI';


COMMIT;

-- =============================================================================
-- STEP 21: UPDATE - UPDATE FRONTING/REFERRED 0.6s
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        co_cd,
        line_pref,
        subline_pref,
        iss_pref,
        pol_yy,
        pol_seq_no,
        ren_seq_no,
        pol_type,
        ri_name AS fronting_referred
    FROM (
        SELECT
            p.co_cd,
            p.line_pref,
            p.subline_pref,
            p.iss_pref,
            p.pol_yy,
            p.pol_seq_no,
            p.ren_seq_no,
            p.pol_type,
            r.ri_name,
            ROW_NUMBER() OVER (
                PARTITION BY
                    p.co_cd,
                    p.line_pref,
                    p.subline_pref,
                    p.iss_pref,
                    p.pol_yy,
                    p.pol_seq_no,
                    p.ren_seq_no,
                    p.pol_type
                ORDER BY p.eff_dt DESC
            ) rn
        FROM niis_polbasic p
        JOIN niis_rinsurer r
          ON r.co_cd = p.co_cd
         AND r.ri_cd = p.referring_inst_cd
        WHERE p.referring_inst_cd IS NOT NULL
          AND NVL(p.pol_stat,'X') <> '5'
          AND p.schema_name = 'FI'
    )
    WHERE rn = 1
) s
ON (
       t.co_cd        = s.co_cd
   AND t.line_pref    = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref     = s.iss_pref
   AND t.pol_yy       = s.pol_yy
   AND t.pol_seq_no   = s.pol_seq_no
   AND t.ren_seq_no   = s.ren_seq_no
   AND t.pol_type     = s.pol_type
)
WHEN MATCHED THEN
UPDATE SET
    t.fronting_referred = s.fronting_referred
    where t.line_pref = 'FI';

COMMIT;


-- =============================================================================
-- STEP 22: UPDATE - UPDATE INTM NUMBER & INTERMEDIARY NAME 20.8s
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT *
    FROM (
        -- 1️⃣ Invoice / commission intermediary (highest priority)
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            b.intm_no,
            c.intm_name,
            1 AS priority,
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd, a.line_pref, a.subline_pref,
                    a.iss_pref, a.pol_yy, a.pol_seq_no, a.ren_seq_no
                ORDER BY 1
            ) rn
        FROM niis_invoice a
        JOIN niis_invcomm b
          ON a.co_cd = b.co_cd
         AND a.line_pref = b.line_pref
         AND a.bill_yy = b.bill_yy
         AND a.bill_seq_no = b.bill_seq_no
        JOIN intrmdry c
          ON b.co_cd = c.co_cd
         AND b.intm_no = c.intm_no
        WHERE a.endt_seq_no = 0
        and 'FI' in (a.schema_name,b.schema_name) -- schema

        UNION ALL

        -- 2️⃣ Fallback for pol_type = 'I'
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            b.ri_cd        AS intm_no,
            c.ri_name      AS intm_name,
            2 AS priority,
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd, a.line_pref, a.subline_pref,
                    a.iss_pref, a.pol_yy, a.pol_seq_no, a.ren_seq_no
                ORDER BY 1
            ) rn
        FROM niis_polbasic a
        JOIN niis_inpolicy b
          ON a.pol_rec_no = b.pol_rec_no
        JOIN niis_rinsurer c
          ON a.co_cd = c.co_cd
         AND b.ri_cd = c.ri_cd
        WHERE a.pol_type = 'I'
          AND a.endt_seq_no = 0
          AND 'FI' in (b.schema_name,a.schema_name) --schema
    )
    WHERE rn = 1
) s
ON (
       t.co_cd       = s.co_cd
   AND t.line_pref   = s.line_pref
   AND t.subline_pref = s.subline_pref
   AND t.iss_pref   = s.iss_pref
   AND t.pol_yy     = s.pol_yy
   AND t.pol_seq_no = s.pol_seq_no
   AND t.ren_seq_no = s.ren_seq_no
)
WHEN MATCHED THEN
UPDATE SET
    t.intm_number   = s.intm_no,
    t.intermediary_name = s.intm_name
    where t.line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 23: UPDATE - UPDATE RISK LOC CODE 56.8s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        til.co_cd,
        til.line_pref,
        til.subline_pref,
        til.iss_pref,
        til.clm_yy,
        til.clm_seq_no,
        tmain.pol_yy,
        tmain.pol_seq_no,
        tmain.ren_seq_no,
        b.risk_loc_cd,
        ROW_NUMBER() OVER (
            PARTITION BY til.co_cd, til.line_pref, til.subline_pref, til.iss_pref,
                         til.clm_yy, til.clm_seq_no, tmain.pol_yy, tmain.pol_seq_no, tmain.ren_seq_no
            ORDER BY a.eff_dt DESC
        ) AS rn
    FROM temp_item_lookup til
    INNER JOIN temp_niis_item_data tmain
        ON tmain.co_cd = til.co_cd
        AND tmain.line_pref = til.line_pref
        AND tmain.subline_pref = til.subline_pref
        AND tmain.iss_pref = til.iss_pref
        AND tmain.clm_yy = til.clm_yy
        AND tmain.clm_seq_no = til.clm_seq_no
    INNER JOIN niis_polbasic a
        ON a.co_cd = tmain.co_cd
        AND a.line_pref = tmain.line_pref
        AND a.subline_pref = tmain.subline_pref
        AND a.iss_pref = tmain.iss_pref
        AND a.pol_yy = tmain.pol_yy
        AND a.pol_seq_no = tmain.pol_seq_no
        AND a.ren_seq_no = tmain.ren_seq_no
        AND NVL(a.pol_stat,'0') <> '5'
        AND a.schema_name = 'FI'
    INNER JOIN niis_fireitem b
        ON b.pol_rec_no = a.pol_rec_no
        AND b.item_no = til.item_no
        AND b.risk_loc_cd IS NOT NULL
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.clm_yy       = src.clm_yy
   AND main.clm_seq_no   = src.clm_seq_no
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
)
WHEN MATCHED THEN
UPDATE SET main.risk_loc_code = src.risk_loc_cd
WHERE src.rn = 1
;


COMMIT;

-- =============================================================================
-- STEP 24: UPDATE - UPDATE RISK LOC DESC 12.2s
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT DISTINCT
        s.co_cd,
        s.risk_loc_cd,
        s.risk_loc_desc
    FROM niis_risklocn s
    WHERE EXISTS (
        SELECT 1
        FROM temp_niis_item_data tm
        WHERE tm.co_cd = s.co_cd
        AND tm.risk_loc_code = s.risk_loc_cd
    )
) src
ON (
    t.co_cd = src.co_cd
    AND t.risk_loc_code = src.risk_loc_cd
)
WHEN MATCHED THEN
UPDATE SET t.risk_loc_desc = src.risk_loc_desc
where t.line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 25: UPDATE - UPDATE WITH EX-GRATIA 17s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT DISTINCT
        x.co_cd,
        x.line_pref,
        x.subline_pref,
        x.iss_pref,
        x.clm_yy,
        x.clm_seq_no,
        'Y' AS with_ex_gratia
    FROM niis_clm_loss x
    JOIN niis_clm_hist y
      ON x.co_cd        = y.co_cd
     AND x.line_pref    = y.line_pref
     AND x.subline_pref = y.subline_pref
     AND x.iss_pref     = y.iss_pref
     AND x.clm_yy       = y.clm_yy
     AND x.clm_seq_no   = y.clm_seq_no
     AND x.hist_seq_no  = y.hist_seq_no
    WHERE y.fla_stat_cd != 'C'
      AND 'FI' IN (x.schema_name, y.schema_name)
      AND (
           INSTR(UPPER(y.remarks), 'GRATIA') <> 0
        OR NVL(x.x_gratia_tag, 'N') = 'Y'
      )
      AND EXISTS (
          SELECT 1
          FROM temp_niis_item_data t
          WHERE t.co_cd = x.co_cd
            AND t.line_pref = x.line_pref
            AND t.subline_pref = x.subline_pref
            AND t.iss_pref = x.iss_pref
            AND t.clm_yy = x.clm_yy
            AND t.clm_seq_no = x.clm_seq_no
      )
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.clm_yy       = src.clm_yy
   AND main.clm_seq_no   = src.clm_seq_no
)
WHEN MATCHED THEN
UPDATE SET main.with_ex_gratia = src.with_ex_gratia
where main.line_pref = 'FI';

COMMIT;

UPDATE temp_niis_item_data
SET with_ex_gratia = 'N'
WHERE with_ex_gratia IS NULL
and line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 26: UPDATE - UPDATE CO INSURANCE, PISC LEAD CO I 50.3s
-- =============================================================================
MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        b.pisc_share,
        b.share_tag,
        b.pol_rec_no,
        ROW_NUMBER() OVER (
            PARTITION BY a.co_cd, a.line_pref, a.subline_pref, a.iss_pref,
                         a.pol_yy, a.pol_seq_no, a.ren_seq_no, a.pol_type
            ORDER BY a.pol_rec_no DESC  -- Or any other column to determine which row to keep
        ) AS rn
    FROM niis_polbasic a
    JOIN niis_fire b
      ON a.pol_rec_no = b.pol_rec_no
    WHERE NVL(a.endt_seq_no, 0) = 0
    AND a.schema_name = 'FI'
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
   AND src.rn = 1  -- Only take the first row
)
WHEN MATCHED THEN
UPDATE SET
    main.pisc_share     = src.pisc_share,
    main.co_insurance   = src.share_tag,
    main.pol_rec_no     = src.pol_rec_no
    where main.line_pref = 'FI'
    ;

COMMIT;


MERGE INTO temp_niis_item_data main
USING (
    SELECT
        m.pol_rec_no,
        m.pisc_share,
        NVL(MAX(c.share_pct), 0) AS max_share_pct
    FROM temp_niis_item_data m
    LEFT JOIN niis_coinsurer c
      ON c.pol_rec_no = m.pol_rec_no
     AND c.insurer_cd <> 99996
    GROUP BY
        m.pol_rec_no,
        m.pisc_share
) src
ON (main.pol_rec_no = src.pol_rec_no)
WHEN MATCHED THEN
UPDATE SET
    main.pisc_lead_co_i =
        CASE
            WHEN NVL(src.pisc_share, 0) = 0 THEN 'N'
            WHEN src.pisc_share > src.max_share_pct THEN 'Y'
            WHEN src.pisc_share = src.max_share_pct
             AND src.pisc_share + src.max_share_pct = 100 THEN 'E'
            ELSE 'N'
        END
where main.line_pref = 'FI'        ;

COMMIT;

UPDATE temp_niis_item_data
SET pisc_lead_co_i = 'N'
WHERE pisc_lead_co_i IS NULL
and line_pref = 'FI';

COMMIT; 

UPDATE temp_niis_item_data
SET co_insurance = 'N'
WHERE NVL(pisc_share,0) = 0
and line_pref = 'FI';


COMMIT;

-- =============================================================================
-- STEP 27: UPDATE - UPDATE ORG TYPE 4.725
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        co_cd,
        line_pref,
        subline_pref,
        iss_pref,
        pol_yy,
        pol_seq_no,
        ren_seq_no,
        org_type
    FROM (
        SELECT
            co_cd,
            line_pref,
            subline_pref,
            iss_pref,
            pol_yy,
            pol_seq_no,
            ren_seq_no,
            org_type,
            ROW_NUMBER() OVER (
                PARTITION BY
                    co_cd,
                    line_pref,
                    subline_pref,
                    iss_pref,
                    pol_yy,
                    pol_seq_no,
                    ren_seq_no
                ORDER BY eff_dt DESC
            ) rn
        FROM niis_polbasic
        WHERE endt_seq_no = 0
          AND schema_name = 'FI'
    )
    WHERE rn = 1
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
)
WHEN MATCHED THEN
UPDATE SET
    main.org_type = src.org_type
WHERE main.org_type IS NULL
and main.line_pref = 'FI';


commit;

-- =============================================================================
-- STEP 28: UPDATE - PROCESS ASSURED NAME 24s
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT DISTINCT
        main.co_cd,
        main.line_pref,
        main.subline_pref,
        main.iss_pref,
        main.pol_yy,
        main.pol_seq_no,
        main.ren_seq_no,
        COALESCE(pb1.name_to_appear, pb2.name_to_appear) AS name_to_appear
    FROM temp_niis_item_data main
    LEFT JOIN (
        SELECT 
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            a.name_to_appear
        FROM niis_polbasic a
        WHERE NVL(a.pol_stat,'0') != '5'
        AND a.schema_name = 'FI' --schema
        AND a.eff_dt = (SELECT MAX(u.eff_dt)
                       FROM niis_polbasic u
                       WHERE u.co_cd = a.co_cd
                       AND u.line_pref = a.line_pref
                       AND u.subline_pref = a.subline_pref
                       AND u.iss_pref = a.iss_pref
                       AND u.pol_yy = a.pol_yy
                       AND u.pol_seq_no = a.pol_seq_no
                       AND u.ren_seq_no = a.ren_seq_no
                       AND NVL(u.pol_stat,'0') != '5'
                       AND u.name_to_appear IS NOT NULL
                       AND u.schema_name = 'FI' --schema
                       )
    ) pb1
        ON pb1.co_cd = main.co_cd
        AND pb1.line_pref = main.line_pref
        AND pb1.subline_pref = main.subline_pref
        AND pb1.iss_pref = main.iss_pref
        AND pb1.pol_yy = main.pol_yy
        AND pb1.pol_seq_no = main.pol_seq_no
        AND pb1.ren_seq_no = main.ren_seq_no
    LEFT JOIN (
        SELECT 
            co_cd,
            line_pref,
            subline_pref,
            iss_pref,
            pol_yy,
            pol_seq_no,
            ren_seq_no,
            name_to_appear
        FROM niis_polbasic
        WHERE endt_seq_no = 0
        AND schema_name = 'FI'
    ) pb2
        ON pb2.co_cd = main.co_cd
        AND pb2.line_pref = main.line_pref
        AND pb2.subline_pref = main.subline_pref
        AND pb2.iss_pref = main.iss_pref
        AND pb2.pol_yy = main.pol_yy
        AND pb2.pol_seq_no = main.pol_seq_no
        AND pb2.ren_seq_no = main.ren_seq_no
        AND pb1.name_to_appear IS NULL
) s
ON (
    t.co_cd = s.co_cd
    AND t.line_pref = s.line_pref
    AND t.subline_pref = s.subline_pref
    AND t.iss_pref = s.iss_pref
    AND t.pol_yy = s.pol_yy
    AND t.pol_seq_no = s.pol_seq_no
    AND t.ren_seq_no = s.ren_seq_no
)
WHEN MATCHED THEN
    UPDATE SET t.assd_name = s.name_to_appear
    where t.line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 29: UPDATE - PROCESS ASSURED NO 16.6s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        co_cd,
        line_pref,
        subline_pref,
        iss_pref,
        pol_yy,
        pol_seq_no,
        ren_seq_no,
        pol_type,
        assd_no
    FROM max_assd_no
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
)
WHEN MATCHED THEN
UPDATE SET
    main.assd_no = src.assd_no
    where main.line_pref = 'FI';

commit;

-- =============================================================================
-- STEP 30: UPDATE - LOCATION,CHANNEL & PRODUCT 47.6s
-- =============================================================================

MERGE INTO temp_niis_item_data main
USING (
    SELECT
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        MAX(d.channel_desc) AS channel,
        MAX(e.product_desc) AS product,
        MAX(b.iss_name) AS location
    FROM ADW_PROD_TGT.NIIS_POLBASIC a
    LEFT JOIN (
        SELECT 
            co_cd,
            iss_pref,
            MAX(iss_name) KEEP (DENSE_RANK LAST ORDER BY updt_dt NULLS FIRST) AS iss_name
        FROM ADW_PROD_TGT.NIIS_ISSOURCE
        WHERE SCHEMA_NAME = 'FI'
        GROUP BY co_cd, iss_pref
    ) b
           ON b.co_cd     = a.co_cd
          AND b.iss_pref  = a.iss_pref
    LEFT JOIN (
        SELECT 
            channel_cd,
            MAX(channel_desc) KEEP (DENSE_RANK LAST ORDER BY updt_dt NULLS FIRST) AS channel_desc
        FROM ADW_PROD_TGT.NIIS_PRODUCT_CHANNEL
        GROUP BY channel_cd
    ) d
           ON d.channel_cd  = a.product_channel_cd
    LEFT JOIN (
        SELECT 
            product_cd,
            MAX(product_desc) KEEP (DENSE_RANK LAST ORDER BY updt_dt NULLS FIRST) AS product_desc
        FROM ADW_PROD_TGT.NIIS_PRODUCT_LINE
        GROUP BY product_cd
    ) e
           ON e.product_cd  = a.product_cd
    WHERE 1=1 
    AND A.SCHEMA_NAME = 'FI'       
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type
) src
ON (
       main.co_cd        = src.co_cd
   AND main.line_pref    = src.line_pref
   AND main.subline_pref = src.subline_pref
   AND main.iss_pref     = src.iss_pref
   AND main.pol_yy       = src.pol_yy
   AND main.pol_seq_no   = src.pol_seq_no
   AND main.ren_seq_no   = src.ren_seq_no
   AND main.pol_type     = src.pol_type
)
WHEN MATCHED THEN
UPDATE SET
    main.channel  = src.channel,
    main.product  = src.product,
    main.location = src.location
    where main.line_pref = 'FI';

COMMIT;

UPDATE temp_niis_item_data
SET product = CASE 
    WHEN co_cd = 1 AND line_pref = 'FI' THEN 'Property'
    WHEN co_cd = 4 AND line_pref = 'FI' THEN 'Special Lines'
    ELSE product
END
WHERE product IS NULL
  AND ((co_cd = 1 AND line_pref = 'FI')
    OR (co_cd = 4 AND line_pref = 'FI'));

COMMIT;


-- =============================================================================
-- STEP 31: UPDATE - PROCESS COMPANY NAME 20s
-- =============================================================================

UPDATE temp_niis_item_data t
SET company = (
    SELECT co_name
    FROM ADW_PROD_TGT.NIIS_COMPANY c
    WHERE c.co_cd = t.co_cd
)
WHERE EXISTS (
    SELECT 1
    FROM ADW_PROD_TGT.NIIS_COMPANY c
    WHERE c.co_cd = t.co_cd
)
and t.line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 32: UPDATE - PROCESS PAYMENT DATE 12.6
-- =============================================================================

MERGE INTO temp_niis_item_data c
USING (
    SELECT
        b.co_cd,
        b.line_pref,
        b.subline_pref,
        b.iss_pref,
        b.clm_yy,
        b.clm_seq_no,
        b.clm_stat_dt
    FROM niis_claims b
) src
ON (
       src.co_cd        = c.co_cd
   AND src.line_pref    = c.line_pref
   AND src.subline_pref = c.subline_pref
   AND src.iss_pref     = c.iss_pref
   AND src.clm_yy       = c.clm_yy
   AND src.clm_seq_no   = c.clm_seq_no
)
WHEN MATCHED THEN
UPDATE SET
    c.payment_dt = src.clm_stat_dt
WHERE
      NVL(c.itm_loss_reserve,0) + NVL(c.itm_expense_reserve,0)
  =   NVL(c.itm_loss_paid,0)   + NVL(c.itm_expense_paid,0)
  AND NVL(c.itm_loss_reserve,0) > 1
  AND c.payment_dt IS NULL
  and c.line_pref = 'FI';

COMMIT;

-- =============================================================================
-- STEP 33: UPDATE - UPDATE STATUS 17.3
-- =============================================================================

UPDATE temp_niis_item_data t
SET t.status = (
    SELECT s.clm_stat_des
    FROM clm_stat s
    WHERE s.co_cd = t.co_cd
      AND s.clm_stat_cd = t.clm_stat_cd
)
WHERE EXISTS (
    SELECT 1
    FROM clm_stat s
    WHERE s.co_cd = t.co_cd
      AND s.clm_stat_cd = t.clm_stat_cd
)
and t.line_pref = 'FI'
;

COMMIT;



END sp_niis_table_itemlvl_fi;