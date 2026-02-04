CREATE
OR REPLACE PROCEDURE sp_niis_table_itemlvl_mc AS

v_batch_id temp_niis_item_data.batch_id%TYPE;

BEGIN

    

/******************************************************************************

NAME:       sp_niis_table_itemlvl_mc
PURPOSE:    niis table item level data 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        01/22/2026             Francis           1. Create sp_niis_table_itemlvl_mc
2.0        02/02/2026             Francis           1. added batch_id,creation_date,last_update_date        

NOTES:

******************************************************************************/

-- =============================================================================
-- STEP 1: Initial INSERT - Only essential data from niis table 3.7s
-- =============================================================================

-- adw_prod_tgt.sp_adw_table_logs('gen_expiry','sp_niis_table_itemlvl',SYSDATE,'','INSERT');

-- Get ONE batch id
    SELECT seq_niis_item_batch_id.NEXTVAL
    INTO   v_batch_id
    FROM   dual;

INSERT INTO temp_niis_item_data (
            co_cd,line_pref,subline_pref,iss_pref,clm_yy,
            clm_seq_no,pol_yy,pol_seq_no,ren_seq_no,pol_type,
            loss_dt,loss_desc,loss_det,event_no,loss_cat_cd,
            curr_cd,clm_stat_cd,org_type,itm_loss_reserve,itm_loss_paid,
            itm_expense_reserve,itm_expense_paid,policy_number,claim_number,
            batch_id,creation_date,last_update_date

)
SELECT      a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.clm_yy,
            a.clm_seq_no,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            a.pol_type,
            a.loss_dt,
            a.loss_desc,
            a.loss_det,
            a.event_no,
            a.loss_cat_cd,
            a.curr_cd,
            a.clm_stat_cd,
            a.org_type,
            a.loss_res_amt as loss_reserve,
            a.loss_pd_amt as loss_paid,
            a.exp_res_amt as expense_reserve,
            a.exp_pd_amt as expense_paid,

            a.line_pref||'-'||a.subline_pref||'-'||
                    a.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(a.pol_yy),3,2)))||'-'||
                    LTRIM(RTRIM(TO_CHAR(a.pol_seq_no,'0999999')))||'-'||
                    LTRIM(RTRIM(TO_CHAR(a.ren_seq_no,'09')))||'-'||a.pol_type as policy_number,

            a.line_pref||'-'||a.subline_pref||'-'||
                        a.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(a.clm_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(a.clm_seq_no,'09999'))) as claim_number,

            v_batch_id,sysdate,sysdate                
                        
        --    a.clm_file_dt as claim_file_dt,
        --    a.loss_location          
            FROM niis_claims a
            WHERE 1=1 
            and a.schema_name = 'MC'
            ;

-- =============================================================================
-- STEP 2: UPDATE - PROCESS ITEM NO 43s 
-- =============================================================================       

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        b.co_cd,
        b.line_pref,
        b.subline_pref,
        b.iss_pref,
        b.clm_yy,
        b.clm_seq_no,
        MAX(b.item_no) AS item_no
    FROM niis_clm_loss b
    JOIN niis_clm_hist x
      ON x.co_cd        = b.co_cd
     AND x.line_pref    = b.line_pref
     AND x.subline_pref = b.subline_pref
     AND x.iss_pref     = b.iss_pref
     AND x.clm_yy       = b.clm_yy
     AND x.clm_seq_no   = b.clm_seq_no
     AND x.hist_seq_no  = b.hist_seq_no
    WHERE x.fla_stat_cd <> 'C'
      AND 'MC' in (b.schema_name,x.schema_name)  
      AND b.hist_seq_no = (
            SELECT MAX(h.hist_seq_no)
            FROM niis_clm_hist h
            WHERE h.co_cd        = b.co_cd
              AND h.line_pref    = b.line_pref
              AND h.subline_pref = b.subline_pref
              AND h.iss_pref     = b.iss_pref
              AND h.clm_yy       = b.clm_yy
              AND h.clm_seq_no   = b.clm_seq_no
              AND h.fla_stat_cd <> 'C'
              AND h.schema_name = 'MC'
      )
    GROUP BY
        b.co_cd,
        b.line_pref,
        b.subline_pref,
        b.iss_pref,
        b.clm_yy,
        b.clm_seq_no
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.clm_yy       = src.clm_yy
   AND t.clm_seq_no   = src.clm_seq_no
)
WHEN MATCHED THEN
UPDATE SET
    t.item_no =  TO_CHAR(src.item_no,'09999'),
    t.last_update_date = sysdate
    where t.line_pref = 'MC';

-- =============================================================================
-- STEP 3: UPDATE - UPDATE ITEM title - 55.6S
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
          AND a.schema_name = 'MC'
          AND b.schema_name = 'MC'
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
    main.item_title = src.item_title,
    main.last_update_date = sysdate
    where main.line_pref = 'MC';


COMMIT;

-- =============================================================================
-- STEP 4: UPDATE - UPDATE ITEM DESCRIPTION 56s
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
          AND a.schema_name = 'MC'
          AND b.schema_name = 'MC'
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
    main.item_desc = src.item_desc,
    main.last_update_date = sysdate
    WHERE main.line_pref = 'MC';

COMMIT;

-- =============================================================================
-- STEP 5: UPDATE - PROCESS ASSURED NAME 65.6s
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
        AND a.schema_name = 'MC' --schema
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
                       AND u.schema_name = 'MC' --schema
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
        AND schema_name = 'MC'
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
    UPDATE SET t.assd_name = s.name_to_appear,
    t.last_update_date = sysdate
    WHERE t.line_pref = 'MC';

COMMIT;

-- =============================================================================
-- STEP 6: UPDATE - PROCESS ASSURED NO 44.6 
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
    main.assd_no = src.assd_no,
    main.last_update_date = sysdate
    WHERE main.line_pref = 'MC';

commit;

-- =============================================================================
-- STEP 7: UPDATE - PROCESS pol address 1,2,3 - 73.7S
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
        pol_addr1,
        pol_addr2,
        pol_addr3
    FROM (
        SELECT
            a.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    co_cd, line_pref, subline_pref, iss_pref,
                    pol_yy, pol_seq_no, ren_seq_no
                ORDER BY
                    CASE
                        WHEN pol_addr1 IS NOT NULL THEN 1
                        ELSE 2
                    END,
                    eff_dt DESC
            ) rn
        FROM niis_polbasic a
        WHERE NVL(pol_stat, '0') <> '5'
          AND a.schema_name = 'MC'
          AND (
                pol_addr1 IS NOT NULL
             OR endt_seq_no = 0
          )
    )
    WHERE rn = 1
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
)
WHEN MATCHED THEN
UPDATE SET
    t.pol_addr1 = src.pol_addr1,
    t.pol_addr2 = src.pol_addr2,
    t.pol_addr3 = src.pol_addr3,
    t.last_update_date = sysdate
    WHERE t.line_pref = 'MC';

COMMIT;



-- =============================================================================
-- STEP 8: UPDATE - PROCESS INCEPT DATE & EXPIRY DATE 136.8s
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
    AND pb.schema_name = 'MC' --schema 
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
    WHERE t.line_pref = 'MC';


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
    AND pb.schema_name = 'MC' --schema 
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
    UPDATE SET t.expiry_date = s.expiry_dt,
    t.last_update_date = sysdate
    WHERE t.line_pref = 'MC';


COMMIT;  



-- =============================================================================
-- STEP 9: UPDATE - UPDATE INTM NUMBER & INTERMEDIARY NAME  79.4s
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
        and 'MC' in (a.schema_name,b.schema_name) -- schema

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
          AND 'MC' in (b.schema_name,a.schema_name) --schema
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
    t.intermediary_name = s.intm_name,
    t.last_update_date = sysdate
    WHERE t.line_pref = 'MC';

COMMIT;

-- =============================================================================
-- STEP 10: UPDATE - PROCESS plate number 92.3s
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        x.co_cd,
        x.line_pref,
        x.subline_pref,
        x.iss_pref,
        x.pol_yy,
        x.pol_seq_no,
        x.ren_seq_no,
        x.item_no,
        x.plate_no
    FROM (
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            b.item_no,
            b.plate_no,
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd,
                    a.line_pref,
                    a.subline_pref,
                    a.iss_pref,
                    a.pol_yy,
                    a.pol_seq_no,
                    a.ren_seq_no,
                    b.item_no
                ORDER BY a.eff_dt DESC
            ) rn
        FROM niis_polbasic a
        JOIN niis_motcar b
          ON a.pol_rec_no = b.pol_rec_no
        WHERE NVL(a.pol_stat, '0') <> '5'
          AND b.plate_no IS NOT NULL
          AND a.schema_name = 'MC'
    ) x
    WHERE x.rn = 1
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
   AND t.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    t.plate_no = src.plate_no,
    t.last_update_date = sysdate
WHERE t.line_pref = 'MC';

COMMIT;

-- =============================================================================
-- STEP 11: UPDATE - PROCESS model year 141.1s
-- =============================================================================


MERGE INTO temp_niis_item_data t
USING (
    SELECT
        x.co_cd,
        x.line_pref,
        x.subline_pref,
        x.iss_pref,
        x.pol_yy,
        x.pol_seq_no,
        x.ren_seq_no,
        x.item_no,
        x.model_year
    FROM (
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            b.item_no,
            b.model_year,
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd,
                    a.line_pref,
                    a.subline_pref,
                    a.iss_pref,
                    a.pol_yy,
                    a.pol_seq_no,
                    a.ren_seq_no,
                    b.item_no
                ORDER BY a.eff_dt DESC
            ) rn
        FROM niis_polbasic a
        JOIN niis_motcar b
          ON a.pol_rec_no = b.pol_rec_no
        WHERE NVL(a.pol_stat, '0') <> '5'
          AND b.model_year IS NOT NULL
          AND a.schema_name = 'MC'
    ) x
    WHERE x.rn = 1
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
   AND t.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    t.model_year = src.model_year,
    t.last_update_date = sysdate
WHERE t.line_pref = 'MC';

COMMIT;

-- =============================================================================
-- STEP 12: UPDATE - PROCESS brand
-- =============================================================================

MERGE INTO temp_niis_item_data t
USING (
    SELECT
        x.co_cd,
        x.line_pref,
        x.subline_pref,
        x.iss_pref,
        x.pol_yy,
        x.pol_seq_no,
        x.ren_seq_no,
        x.item_no,
        x.brand_desc
    FROM (
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            b.item_no,
            c.brand_desc,
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd,
                    a.line_pref,
                    a.subline_pref,
                    a.iss_pref,
                    a.pol_yy,
                    a.pol_seq_no,
                    a.ren_seq_no,
                    b.item_no
                ORDER BY a.eff_dt DESC
            ) rn
        FROM niis_polbasic a
        JOIN niis_motcar b
          ON a.pol_rec_no = b.pol_rec_no
        JOIN mc_brand c
          ON c.co_cd = a.co_cd
         AND c.brand_cd = b.brand_cd
        WHERE NVL(a.pol_stat, '0') <> '5'
          AND b.brand_cd IS NOT NULL
          AND a.schema_name = 'MC'
    ) x
    WHERE x.rn = 1
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
   AND t.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    t.brand_desc = src.brand_desc,
    t.last_update_date = sysdate
WHERE t.line_pref = 'MC';

COMMIT;

-- =============================================================================
-- STEP 13: UPDATE - PROCESS brand type 149s
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
        item_no,
        brand_type_desc
    FROM (
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            b.item_no,
            c.brand_type_desc AS brand_type_desc,  
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd,
                    a.line_pref,
                    a.subline_pref,
                    a.iss_pref,
                    a.pol_yy,
                    a.pol_seq_no,
                    a.ren_seq_no,
                    b.item_no
                ORDER BY a.eff_dt DESC
            ) rn
        FROM niis_polbasic a
        JOIN niis_motcar b
          ON a.pol_rec_no = b.pol_rec_no
        JOIN mc_brand_type c  
          ON c.co_cd      = a.co_cd
         AND c.brand_cd   = b.brand_cd
         AND c.brand_type = b.brand_type
        WHERE NVL(a.pol_stat, '0') <> '5'
          AND b.brand_type IS NOT NULL
          AND a.schema_name = 'MC'
    )
    WHERE rn = 1
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
   AND t.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    t.brand_type_desc = src.brand_type_desc,
    t.last_update_date = sysdate
WHERE t.brand_type_desc IS NULL
  AND t.line_pref = 'MC';


-- =============================================================================
-- STEP 14: UPDATE - PROCESS spec type 94.6s
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
        item_no,
        specific_desc
    FROM (
        SELECT
            a.co_cd,
            a.line_pref,
            a.subline_pref,
            a.iss_pref,
            a.pol_yy,
            a.pol_seq_no,
            a.ren_seq_no,
            b.item_no,
            c.specific_desc,
            ROW_NUMBER() OVER (
                PARTITION BY
                    a.co_cd,
                    a.line_pref,
                    a.subline_pref,
                    a.iss_pref,
                    a.pol_yy,
                    a.pol_seq_no,
                    a.ren_seq_no,
                    b.item_no
                ORDER BY a.eff_dt DESC
            ) rn
        FROM niis_polbasic a
        JOIN niis_motcar b
          ON a.pol_rec_no = b.pol_rec_no
        JOIN mc_spec_type c
          ON c.co_cd          = a.co_cd
         AND c.brand_cd       = b.brand_cd
         AND c.brand_type     = b.brand_type
         AND c.specific_type  = b.specific_type
        WHERE NVL(a.pol_stat, '0') <> '5'
          AND b.specific_type IS NOT NULL
          AND a.schema_name = 'MC'
    )
    WHERE rn = 1
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
   AND t.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    t.specific_desc = src.specific_desc,
    t.last_update_date = sysdate
WHERE t.specific_desc IS NULL
  AND t.line_pref = 'MC';

-- =============================================================================
-- STEP 15: UPDATE - PROCESS sum insured 86s
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
        b.item_no,
        SUM(NVL(b.tsi_amt, 0)) AS mc_sum_insured
    FROM niis_polbasic a
    JOIN niis_item b
      ON a.pol_rec_no = b.pol_rec_no
    WHERE NVL(a.pol_stat, 'X') <> '5'
    AND 'MC' in (a.schema_name,b.schema_name)
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        b.item_no
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
   AND t.pol_type     = src.pol_type
   AND t.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    t.mc_sum_insured = src.mc_sum_insured,
    t.last_update_date = sysdate
    where t.line_pref = 'MC'
    ;


-- =============================================================================
-- STEP 16: UPDATE - PROCESS od tsi
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
        b.item_no,
        SUM(DISTINCT NVL(b.tsi_amt, 0)) AS mc_od_tsi
    FROM niis_polbasic a
    JOIN itmperil b
      ON a.pol_rec_no = b.pol_rec_no
    WHERE NVL(a.pol_stat, 'X') <> '5'
      AND b.peril_cd = 2
      AND 'MC' in (a.schema_name,b.schema_name)
    GROUP BY
        a.co_cd,
        a.line_pref,
        a.subline_pref,
        a.iss_pref,
        a.pol_yy,
        a.pol_seq_no,
        a.ren_seq_no,
        a.pol_type,
        b.item_no
) src
ON (
       t.co_cd        = src.co_cd
   AND t.line_pref    = src.line_pref
   AND t.subline_pref = src.subline_pref
   AND t.iss_pref     = src.iss_pref
   AND t.pol_yy       = src.pol_yy
   AND t.pol_seq_no   = src.pol_seq_no
   AND t.ren_seq_no   = src.ren_seq_no
   AND t.pol_type     = src.pol_type
   AND t.item_no      = src.item_no
)
WHEN MATCHED THEN
UPDATE SET
    t.mc_od_tsi = src.mc_od_tsi,
    t.last_update_date = sysdate
    where t.line_pref = 'MC';


COMMIT;

-- =============================================================================
-- STEP 17: UPDATE - LOCATION,CHANNEL & PRODUCT 
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
        WHERE schema_name IN ('MC') --schema 
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
    AND a.schema_name IN ('MC') --schema 
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
    main.location = src.location,
    main.last_update_date = sysdate
    where main.line_pref IN ('MC') ;

COMMIT;

-- UPDATE temp_niis_item_data
-- SET product = CASE 
--     WHEN co_cd = 1 AND line_pref = 'GA' THEN 'General Accident'
--     WHEN co_cd = 4 AND line_pref = 'GA' THEN 'Special Lines'
--     ELSE product
-- END,
--     last_update_date = sysdate
-- WHERE product IS NULL
--   AND ((co_cd = 1 AND line_pref = 'GA')
--     OR (co_cd = 4 AND line_pref = 'GA'));

-- COMMIT;


-- =============================================================================
-- STEP 18: UPDATE - PROCESS COMPANY NAME 
-- =============================================================================

UPDATE temp_niis_item_data t
SET company = (
    SELECT co_name
    FROM ADW_PROD_TGT.NIIS_COMPANY c
    WHERE c.co_cd = t.co_cd
),
last_update_date = sysdate
WHERE EXISTS (
    SELECT 1
    FROM ADW_PROD_TGT.NIIS_COMPANY c
    WHERE c.co_cd = t.co_cd
)
and t.line_pref IN ('MC');

COMMIT;

-- =============================================================================
-- STEP 19: UPDATE - PROCESS PAYMENT DATE 
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
    c.payment_dt = src.clm_stat_dt,
    c.last_update_date = sysdate
WHERE
      NVL(c.itm_loss_reserve,0) + NVL(c.itm_expense_reserve,0)
  =   NVL(c.itm_loss_paid,0)   + NVL(c.itm_expense_paid,0)
  AND NVL(c.itm_loss_reserve,0) > 1
  AND c.payment_dt IS NULL
  and c.line_pref IN ('MC')
  ;

COMMIT;


-- =============================================================================
-- STEP 20: UPDATE - UPDATE STATUS 
-- =============================================================================

UPDATE temp_niis_item_data t
SET t.status = (
    SELECT s.clm_stat_des
    FROM clm_stat s
    WHERE s.co_cd = t.co_cd
      AND s.clm_stat_cd = t.clm_stat_cd
),
t.last_update_date = sysdate
WHERE EXISTS (
    SELECT 1
    FROM clm_stat s
    WHERE s.co_cd = t.co_cd
      AND s.clm_stat_cd = t.clm_stat_cd
)
and t.line_pref IN ('MC')
;

COMMIT;

-- adw_prod_tgt.sp_adw_table_logs('gen_expiry','sp_insert_expiry_adw',SYSDATE,SYSDATE,'UPDATE');


END sp_niis_table_itemlvl_mc;