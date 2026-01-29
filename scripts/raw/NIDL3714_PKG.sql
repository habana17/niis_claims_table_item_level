create or replace PACKAGE BODY nidl3714_pkg AS
-------------------------------------------------------------------------------------------------- cat_evt_file_mc
   PROCEDURE cat_evt_file_mc IS
      v_policy         VARCHAR2(50);
      v_claim         VARCHAR2(50);
      v_assd_no         max_assured.assd_no%TYPE;
      v_item_no         item.item_no%TYPE;
      v_item_title      item.item_title%TYPE;
      v_item_desc       item.item_desc%TYPE;
      v_name_to_appear  polbasic.name_to_appear%TYPE;
      v_plate_no        dbadm_mc.motcar.plate_no%TYPE;
      v_model_year      dbadm_mc.motcar.model_year%TYPE;
      v_brand_desc      dbadm_mc.mc_brand.brand_desc%TYPE;
      v_brand_type_desc dbadm_mc.mc_brand_type.brand_type_desc%TYPE;
      v_specific_desc   dbadm_mc.mc_spec_type.specific_desc%TYPE;
      v_pol_addr1       polbasic.pol_addr1%TYPE;
      v_pol_addr2       polbasic.pol_addr2%TYPE;
      v_pol_addr3       polbasic.pol_addr3%TYPE;
      v_clm_stat_des    clm_stat.clm_stat_des%TYPE;
      v_intm_no         intrmdry.intm_no%TYPE;
      v_intm_name       VARCHAR2(100);
      v_tsi_amt         polbasic.tsi_amt%TYPE;
      v_rep_evaln_no    dbadm_mc.mc_rep_evaln.rep_evaln_no%TYPE;
      v_od_prem_rate    itmperil.prem_rate%TYPE;
      v_aog_prem_rate   itmperil.prem_rate%TYPE;
      v_od_comm_rate    comperl.comm_rate%TYPE;
      v_aog_comm_rate   comperl.comm_rate%TYPE;
      v_mort_cd         mortgagee.mort_cd%TYPE;
      v_mort_name       mortgagee.mort_name%TYPE;
      v_od_tsi          itmperil.tsi_amt%TYPE;
      v_incept_dt       polbasic.incept_dt%TYPE;
      v_expiry_dt       polbasic.expiry_dt%TYPE;
      v_recovery        clm_loss.loss_adv_amt%TYPE;
      v_items           NUMBER(5);
      v_fleet_tag       VARCHAR2(1);
      v_x_gratia_cnt    NUMBER(5);
      v_x_gratia        VARCHAR2(1);
      v_org_type        VARCHAR2(1);

      out_file         UTL_FILE.file_type;

      CURSOR c1 IS
         SELECT a.*
            FROM claims a
            WHERE a.co_cd = g_co_cd
            AND line_pref = g_line_pref
            AND iss_pref = NVL(g_iss_pref,a.iss_pref)
            AND event_no = NVL(g_event_no,a.event_no);
   BEGIN
      FOR c1_rec IN c1
         LOOP
            IF g_cnt = 0 THEN
               g_cnt          := 1;
               g_file_path    := 'CAT_EVT_FILE_'||g_line_pref||'_'||TO_CHAR(SYSDATE,'MMDDYYYYHH24MISS')||'.csv';
               g_dir_path     := get_directory('DDFDIR');
               out_file       := utl_file.fopen('DDFDIR', g_file_path, 'W');

               UTL_FILE.PUT_LINE(out_file,
                  'ASSURED NAME'  ||','||
                  'POLICY ADDRESS1'||','||
                  'POLICY ADDRESS2'||','||
                  'POLICY ADDRESS3'||','||
                  'POLICY NUMBER'||','||
                  'INCEPT DATE'||','||                --REV-002552
                  'EXPIRY DATE'||','||                --REV-002552
                  'INTM NUMBER'||','||
                  'INTERMEDIARY NAME'||','||
                  'ITEM NUMBER'||','||
                  'ITEM TITLE'||','||
                  'ITEM DESC'||','||
                  'PLATE NO'||','||
                  'MODEL YEAR'||','||
                  'BRAND'||','||
                  'BRAND TYPE'||','||
                  'SPEC TYPE'||','||
                  'CLAIM NUMBER' ||','||
                  'CLAIM FILE DATE'  ||','||
                  'DATE OF LOSS' ||','||
                  'SUM INSURED'||','||
                  'OD TSI'||','||
                  'LOSS RESERVE' ||','||
                  'LOSS PAID'    ||','||
                  'EXPENSE RESERVE'  ||','||
                  'EXPENSE PAID'     ||','||
                  'RECOVERIES'     ||','||
                  'CURRENCY'     ||','||
                  'LOSS DESCRIPTION'    ||','||
                  'LOSS DETAIL'  ||','||
                  'LOSS LOCATION'||','||
                  'STATUS'||','||
                  'REPAIR EVALN NO.'||','||
                  'ITEM OD PREM RATE'||','||
                  'ITEM AOG PREM RATE'||','||
                  'OD COMM RATE'||','||
                  'AOG COMM RATE'||','||
                  'MORTAGEE CODE'||','||
                  'MORTGAGEE NAME'||','||
                  'FLEET TAG' ||','||
                  'ITEM COUNT'||','||
                  'WITH EX-GRATIA'||','||
                  'ORG TYPE');

               UTL_FILE.NEW_LINE(out_file,1);
            END IF;

            v_item_no := NULL;
            v_item_title := NULL;
            v_item_desc := NULL;
            v_name_to_appear := NULL;
            v_plate_no := NULL;
            v_model_year := NULL;
            v_brand_desc := NULL;
            v_brand_type_desc := NULL;
            v_specific_desc := NULL;
            v_pol_addr1 := NULL;
            v_pol_addr2 := NULL;
            v_pol_addr3 := NULL;
            v_clm_stat_des := NULL;
            v_intm_no := NULL;
            v_intm_name := NULL;
            v_tsi_amt := NULL;
            v_rep_evaln_no := NULL;
            v_od_prem_rate := NULL;
            v_aog_prem_rate := NULL;
            v_od_comm_rate := NULL;
            v_aog_comm_rate := NULL;
            v_mort_cd := NULL;
            v_mort_name := NULL;
            v_od_tsi := NULL;
            v_incept_dt := NULL;
            v_expiry_dt := NULL;
            v_recovery := NULL;
            v_items := NULL;
            v_fleet_tag := NULL;
            v_org_type := NULL;

            v_policy := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.pol_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.pol_seq_no,'0999999')))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.ren_seq_no,'09')))||'-'||c1_rec.pol_type;

            v_claim  := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.clm_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.clm_seq_no,'09999')));
            BEGIN
               SELECT assd_no
                  INTO v_assd_no
                  FROM max_assd_no
                  WHERE co_cd      = c1_rec.co_cd
                  AND line_pref    = c1_rec.line_pref
                  AND subline_pref = c1_rec.subline_pref
                  AND iss_pref     = c1_rec.iss_pref
                  AND pol_yy       = c1_rec.pol_yy
                  AND pol_seq_no   = c1_rec.pol_seq_no
                  AND ren_seq_no   = c1_rec.ren_seq_no
                  AND pol_type     = c1_rec.pol_type;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_assd_no := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: assd no '||v_policy);
            END;

            BEGIN
               SELECT a.name_to_appear
                  INTO v_name_to_appear
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u
                                    WHERE u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND u.name_to_appear IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.name_to_appear
                        INTO v_name_to_appear
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_name_to_appear := NULL;
                  END;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: name '||v_policy);
            END;

            BEGIN
               SELECT a.pol_addr1, a.pol_addr2, a.pol_addr3
                  INTO v_pol_addr1, v_pol_addr2, v_pol_addr3
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u
                                    WHERE u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND u.pol_addr1 IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.pol_addr1, a.pol_addr2, a.pol_addr3
                        INTO v_pol_addr1, v_pol_addr2, v_pol_addr3
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_pol_addr1 := NULL;
                        v_pol_addr2 := NULL;
                        v_pol_addr3 := NULL;
                  END;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: addr '||v_policy);
            END;

            BEGIN
               SELECT b.intm_no, intm_name
                  INTO v_intm_no, v_intm_name
                  FROM invoice a,invcomm b, intrmdry c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.bill_yy = b.bill_yy
                  AND a.bill_seq_no = b.bill_seq_no
                  AND b.co_cd = c.co_cd
                  AND b.intm_no = c.intm_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.endt_seq_no = 0;
            EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     IF c1_rec.pol_type = 'I' THEN
                        BEGIN
                           SELECT b.ri_cd, ri_name
                              INTO v_intm_no, v_intm_name
                              FROM polbasic a,inpolicy b, rinsurer c
                              WHERE a.pol_rec_no = b.pol_rec_no
                              AND a.co_cd = c.co_cd
                              AND b.ri_cd = c.ri_cd
                              AND a.co_cd = c1_rec.co_cd
                              AND a.line_pref = c1_rec.line_pref
                              AND a.subline_pref = c1_rec.subline_pref
                              AND a.iss_pref = c1_rec.iss_pref
                              AND a.pol_yy = c1_rec.pol_yy
                              AND a.pol_seq_no = c1_rec.pol_seq_no
                              AND a.ren_seq_no = c1_rec.ren_seq_no
                              AND a.endt_seq_no = 0;
                        EXCEPTION
                           WHEN NO_DATA_FOUND THEN
                              v_intm_no := NULL;
                              v_intm_name := NULL;
                        END;
                  ELSE
                     v_intm_no := NULL;
                     v_intm_name := NULL;
                  END IF;
            WHEN TOO_MANY_ROWS THEN
               SELECT b.intm_no, intm_name
                  INTO v_intm_no, v_intm_name
                  FROM invoice a,invcomm b, intrmdry c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.bill_yy = b.bill_yy
                  AND a.bill_seq_no = b.bill_seq_no
                  AND b.co_cd = c.co_cd
                  AND b.intm_no = c.intm_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.endt_seq_no = 0
                  AND ROWNUM = 1;
            END;
            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
            END;
            BEGIN
               SELECT DISTINCT b.item_no
                  INTO v_item_no
                  FROM clm_loss b
                  WHERE b.co_cd = c1_rec.co_cd
                  AND b.line_pref = c1_rec.line_pref
                  AND b.subline_pref = c1_rec.subline_pref
                  AND b.iss_pref = c1_rec.iss_pref
                  AND b.clm_yy = c1_rec.clm_yy
                  AND b.clm_seq_no = c1_rec.clm_seq_no
                  AND b.hist_seq_no = (SELECT MAX(x.hist_seq_no)
                                          FROM clm_hist x
                                          WHERE b.co_cd = x.co_cd
                                          AND b.line_pref = x.line_pref
                                          AND b.subline_pref = x.subline_pref
                                          AND b.iss_pref = x.iss_pref
                                          AND b.clm_yy = x.clm_yy
                                          AND b.clm_seq_no = x.clm_seq_no
                                          AND x.fla_stat_cd != 'C');
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_no := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: item_no '||v_policy||' '||v_claim);
            END;
            IF v_item_no IS NOT NULL THEN
            BEGIN
               SELECT SUM(NVL(b.tsi_amt,0))
                  INTO v_tsi_amt
                  FROM polbasic a,item b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = v_item_no
                  AND NVL(a.pol_stat,'X') != '5';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_tsi_amt := NULL;
            END;
            BEGIN
               SELECT b.item_title
                  INTO v_item_title
                  FROM polbasic a,item b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = v_item_no
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z,item y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.item_no = b.item_no
                                    AND y.item_title IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_title     := ' ';
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: item title '||v_policy);
            END;
            BEGIN
               SELECT b.item_desc
                  INTO v_item_desc
                  FROM polbasic a,item b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = v_item_no
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z,item y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.item_no = b.item_no
                                    AND y.item_desc IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_desc     := ' ';
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: item desc '||v_policy);
            END;
            BEGIN
               SELECT b.plate_no
                  INTO v_plate_no
                  FROM polbasic a,motcar b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND b.item_no = v_item_no
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u,motcar v
                                    WHERE u.pol_rec_no = v.pol_rec_no
                                 AND u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND v.item_no = b.item_no
                                    AND v.plate_no IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_plate_no:= NULL;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: plate_no '||v_policy);
            END;
            BEGIN
               SELECT b.model_year
                  INTO v_model_year
                  FROM polbasic a,dbadm_mc.motcar b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND b.item_no = v_item_no
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u,dbadm_mc.motcar v
                                    WHERE u.pol_rec_no = v.pol_rec_no
                                 AND u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND v.item_no = b.item_no
                                    AND v.model_year IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_model_year := NULL;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: model_year '||v_policy);
            END;
            BEGIN
               SELECT c.brand_desc
                  INTO v_brand_desc
                  FROM polbasic a,dbadm_mc.motcar b,dbadm_mc.mc_brand c
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c.co_cd
                  AND b.brand_cd = c.brand_cd
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND b.item_no = v_item_no
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM dbadm_mc.polbasic u,dbadm_mc.motcar v
                                    WHERE u.pol_rec_no = v.pol_rec_no
                                 AND u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND v.item_no = b.item_no
                                    AND v.brand_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_brand_desc:= NULL;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: brand_desc '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT c.brand_type_desc
                  INTO v_brand_type_desc
                  FROM polbasic a,dbadm_mc.motcar b,dbadm_mc.mc_brand_type c
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c.co_cd
                  AND b.brand_cd = c.brand_cd
                  AND b.brand_type = c.brand_type
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND b.item_no = v_item_no
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u,dbadm_mc.motcar v
                                    WHERE u.pol_rec_no = v.pol_rec_no
                                 AND u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND v.item_no = b.item_no
                                    AND v.brand_type IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_brand_type_desc:= NULL;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: brand_type_desc '||v_policy);
            END;
            BEGIN
               SELECT c.specific_desc
                  INTO v_specific_desc
                  FROM polbasic a,dbadm_mc.motcar b,dbadm_mc.mc_spec_type c
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c.co_cd
                  AND b.brand_cd = c.brand_cd
                  AND b.brand_type = c.brand_type
                  AND b.specific_type = c.specific_type
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND b.item_no = v_item_no
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u,dbadm_mc.motcar v
                                    WHERE u.pol_rec_no = v.pol_rec_no
                                 AND u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND v.item_no = b.item_no
                                    AND v.specific_type IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_specific_desc:= NULL;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: v_specific_desc '||v_policy);
            END;
            BEGIN
               SELECT MAX(rep_evaln_no)
                  INTO v_rep_evaln_no
                  FROM dbadm_mc.mc_rep_evaln a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.clm_yy = c1_rec.clm_yy
                  AND a.clm_seq_no = c1_rec.clm_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_rep_evaln_no := NULL;
            END;

            BEGIN
               SELECT b.prem_rate
                  INTO v_od_prem_rate
                  FROM polbasic a,itmperil b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = v_item_no
                  AND b.peril_cd = 2
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z,itmperil y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.item_no = b.item_no
                                    AND y.peril_cd = b.peril_cd);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_od_prem_rate     := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: od prem '||v_policy);
            END;
            BEGIN
               SELECT b.prem_rate
                  INTO v_aog_prem_rate
                  FROM polbasic a,itmperil b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = v_item_no
                  AND b.peril_cd IN (7,15)
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z,itmperil y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.item_no = b.item_no
                                    AND y.peril_cd = b.peril_cd);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_aog_prem_rate     := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: aog prem '||v_policy);
            END;
            END IF;
            IF c1_rec.pol_type = 'I' THEN
               BEGIN
                  SELECT (ri_comm_amt/ri_prem_amt) * 100
                     INTO v_od_comm_rate
                     FROM inri_comm b,polbasic a
                     WHERE b.pol_rec_no = a.pol_rec_no
                     AND a.co_cd = c1_rec.co_cd
                     AND a.line_pref = c1_rec.line_pref
                     AND a.subline_pref = c1_rec.subline_pref
                     AND a.iss_pref = c1_rec.iss_pref
                     AND a.pol_yy = c1_rec.pol_yy
                     AND a.pol_seq_no = c1_rec.pol_seq_no
                     AND a.ren_seq_no = c1_rec.ren_seq_no
                     AND a.endt_seq_no = 0;
               EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     v_od_comm_rate := null;
                  WHEN TOO_MANY_ROWS THEN
                     RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: od comm rt '||v_policy);
               END;

               v_aog_comm_rate := v_od_comm_rate;
            ELSE
               BEGIN
                  SELECT comm_rate
                     INTO v_od_comm_rate
                     FROM invoice b,comperl c
                     WHERE b.co_cd = c.co_cd
                     AND b.line_pref = c.line_pref
                     AND b.bill_yy = c.bill_yy
                     AND b.bill_seq_no = c.bill_seq_no
                     AND b.co_cd = c1_rec.co_cd
                     AND b.line_pref = c1_rec.line_pref
                     AND b.subline_pref = c1_rec.subline_pref
                     AND b.iss_pref = c1_rec.iss_pref
                     AND b.pol_yy = c1_rec.pol_yy
                     AND b.pol_seq_no = c1_rec.pol_seq_no
                     AND b.ren_seq_no = c1_rec.ren_seq_no
                     AND b.pol_type = c1_rec.pol_type
                     AND b.endt_seq_no = 0
                     AND c.peril_cd = 2;
               EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     v_od_comm_rate := null;
                  WHEN TOO_MANY_ROWS THEN
                     RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: od comm rt '||v_policy);
               END;


               BEGIN
                  SELECT comm_rate
                     INTO v_aog_comm_rate
                     FROM invoice b,comperl c
                     WHERE b.co_cd = c.co_cd
                     AND b.line_pref = c.line_pref
                     AND b.bill_yy = c.bill_yy
                     AND b.bill_seq_no = c.bill_seq_no
                     AND b.co_cd = c1_rec.co_cd
                     AND b.line_pref = c1_rec.line_pref
                     AND b.subline_pref = c1_rec.subline_pref
                     AND b.iss_pref = c1_rec.iss_pref
                     AND b.pol_yy = c1_rec.pol_yy
                     AND b.pol_seq_no = c1_rec.pol_seq_no
                     AND b.ren_seq_no = c1_rec.ren_seq_no
                     AND b.pol_type = c1_rec.pol_type
                     AND c.peril_cd IN (7,15)
                     AND rownum = 1;
               EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     v_aog_comm_rate := null;
                  WHEN TOO_MANY_ROWS THEN
                     RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: aog comm rt '||v_policy);
               END;
            END IF;

            BEGIN
               SELECT b.mort_cd
                  INTO v_mort_cd
                  FROM polbasic a,doctext b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z,doctext y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.mort_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_mort_cd     := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: mort_cd '||v_policy);
            END;
            BEGIN
               SELECT mort_name
                  INTO v_mort_name
                  FROM mortgagee
                  WHERE co_cd = c1_rec.co_cd
                  AND mort_cd = v_mort_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_mort_name     := NULL;
            END;
            BEGIN
               SELECT SUM(NVL(b.tsi_amt,0))
                  INTO v_od_tsi
                  FROM polbasic a,itmperil b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = v_item_no
                  AND b.peril_cd = 2
                  AND NVL(a.pol_stat,'X') != '5';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_od_tsi := NULL;
            END;
            BEGIN
               SELECT a.incept_dt
                  INTO v_incept_dt
                  FROM max_incept_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.incept_dt
                        INTO v_incept_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_incept_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: incept dt '||v_policy);
            END;

            BEGIN
               SELECT a.expiry_dt
                  INTO v_expiry_dt
                  FROM max_expiry_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.expiry_dt
                        INTO v_expiry_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_expiry_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_mc - TOO_MANY_ROWS: expiry dt '||v_policy);
            END;
            BEGIN
               SELECT SUM(NVL(a.loss_adv_amt,0))
                  INTO v_recovery
                  FROM clm_loss a,clm_hist b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.subline_pref = b.subline_pref
                  AND a.iss_pref = b.iss_pref
                  AND a.clm_yy = b.clm_yy
                  AND a.clm_seq_no = b.clm_seq_no
                  AND a.hist_seq_no = b.hist_seq_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.clm_yy = c1_rec.clm_yy
                  AND a.clm_seq_no = c1_rec.clm_seq_no
                  AND a.item_no = v_item_no
                  AND b.fla_stat_cd != 'C'
                  AND SIGN(a.loss_adv_amt) = -1;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_recovery := NULL;
            END;
            BEGIN
               SELECT COUNT(DISTINCT item_no)
                  INTO v_items
                  FROM polbasic a,item b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'X') != '5';
            END;
            IF v_items > 1 THEN
               v_fleet_tag := 'Y';
            ELSE
               v_fleet_tag := 'N';
            END IF;
            v_x_gratia_cnt := 0;
            BEGIN
               SELECT count(*)
                  INTO v_x_gratia_cnt
                  FROM clm_loss x,clm_hist y
                  WHERE x.co_cd = y.co_cd
                  AND x.line_pref = y.line_pref
                  AND x.subline_pref = y.subline_pref
                  AND x.iss_pref = y.iss_pref
                  AND x.clm_yy = y.clm_yy
                  AND x.clm_seq_no = y.clm_seq_no
                  AND x.hist_seq_no = y.hist_seq_no
                  AND x.co_cd = c1_rec.co_cd
                  AND x.line_pref = c1_rec.line_pref
                  AND x.subline_pref = c1_rec.subline_pref
                  AND x.iss_pref = c1_rec.iss_pref
                  AND x.clm_yy = c1_rec.clm_yy
                  AND x.clm_seq_no = c1_rec.clm_seq_no
                  AND y.fla_stat_cd != 'C'
                  AND (INSTR(UPPER(y.remarks),'GRATIA') <> 0 OR
                                 NVL(x.x_gratia_tag,'N') = 'Y');
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_x_gratia_cnt := 0;
            END;

            IF v_x_gratia_cnt = 0 THEN
               v_x_gratia := 'N';
            ELSE
               v_x_gratia := 'Y';
            END IF;
            
            IF c1_rec.org_type IS NULL THEN
               SELECT GET_ORG_TYPE_POL(c1_rec.co_cd, c1_rec.line_pref, c1_rec.subline_pref, c1_rec.iss_pref, c1_rec.pol_yy, 
                                c1_rec.pol_seq_no, c1_rec.ren_seq_no, c1_rec.pol_type)
                  INTO v_org_type
                  FROM dual;            
            ELSE
               v_org_type := c1_rec.org_type;
            END IF;

            UTL_FILE.PUT_LINE(out_file,
               '"'||v_name_to_appear||'"'||','||
               '"'||v_pol_addr1||'"'||','||
               '"'||v_pol_addr2||'"'||','||
               '"'||v_pol_addr3||'"'||','||
               v_policy||','||
               TO_CHAR(v_incept_dt,'MM/DD/YYYY')||','||
               TO_CHAR(v_expiry_dt,'MM/DD/YYYY')||','||
               TO_CHAR(v_intm_no,'09999')||','||
               '"'||v_intm_name||'"'||','||
               TO_CHAR(v_item_no,'09999')||','||
               '"'||v_item_title||'"'||','||
               '"'||v_item_desc||'"'||','||
               v_plate_no||','||
               v_model_year||','||
               v_brand_desc||','||
               v_brand_type_desc||','||
               v_specific_desc||','||
               v_claim           ||','||
               TO_CHAR(c1_rec.clm_file_dt,'MM/DD/YYYY')||','||
               TO_CHAR(c1_rec.loss_dt,'MM/DD/YYYY')    ||','||
               TO_CHAR(NVL(v_tsi_amt,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_od_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.loss_res_amt,0),'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.loss_pd_amt,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.exp_res_amt,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.exp_pd_amt,0)  ,'99999999990.00')||','||
               TO_CHAR(NVL(v_recovery,0)  ,'99999999990.00')||','||
               c1_rec.curr_cd||','||
               '"'||c1_rec.loss_desc ||'"'||','||
               '"'||c1_rec.loss_det  ||'"'||','||
               '"'||c1_rec.loss_location||'"'||','||
               v_clm_stat_des||','||
               TO_CHAR(v_rep_evaln_no)||','||
               TO_CHAR(NVL(v_od_prem_rate,0))||','||
               TO_CHAR(NVL(v_aog_prem_rate,0))||','||
               TO_CHAR(NVL(v_od_comm_rate,0))||','||
               TO_CHAR(NVL(v_aog_comm_rate,0))||','||
               TO_CHAR(v_mort_cd)||','||
               '"'||v_mort_name||'"'||','||
               v_fleet_tag||','||
               TO_CHAR(v_items)||','||
               v_x_gratia||','||
               v_org_type);
         END LOOP;

         IF UTL_FILE.is_open(out_file) THEN
            UTL_FILE.fclose(out_file);
         END IF;
   END cat_evt_file_mc;
-------------------------------------------------------------------------------------------------- cat_evt_file_mc

-------------------------------------------------------------------------------------------------- cat_evt_file_fi_itm
   PROCEDURE cat_evt_file_fi_itm IS
      v_policy               VARCHAR2(50);
      v_claim               VARCHAR2(50);
      v_assd_no               max_assured.assd_no%TYPE;
      v_item_no               item.item_no%TYPE;
      v_item_title            item.item_title%TYPE;
      v_item_desc             item.item_desc%TYPE;
      v_name_to_appear        polbasic.name_to_appear%TYPE;
      v_pol_addr1             polbasic.pol_addr1%TYPE;
      v_pol_addr2             polbasic.pol_addr2%TYPE;
      v_pol_addr3             polbasic.pol_addr3%TYPE;
      v_clm_stat_des          clm_stat.clm_stat_des%TYPE;
      v_intm_no               intrmdry.intm_no%TYPE;
      v_intm_name             VARCHAR2(100);
      v_tsi_amt               polbasic.tsi_amt%TYPE;
      v_prem_amt              polbasic.tsi_amt%TYPE;
      v_incept_dt             polbasic.incept_dt%TYPE;
      v_expiry_dt             polbasic.expiry_dt%TYPE;
      v_recovery              clm_loss.loss_adv_amt%TYPE;
      v_items                 NUMBER(5);
      v_fleet_tag             VARCHAR2(1);
      v_vessel                VARCHAR2(50);
      v_occ_cd                dbadm_fi.occupancy.occ_cd%TYPE;
      v_occ_title             dbadm_fi.occupancy.occ_title%TYPE;
      v_referring_inst_cd     polbasic.referring_inst_cd%TYPE;
      v_referring_name        rinsurer.ri_name%TYPE;

      v_netret_tsi            polbasic.tsi_amt%TYPE;
      v_netret_prem           polbasic.tsi_amt%TYPE;
      v_quota_tsi             polbasic.tsi_amt%TYPE;
      v_quota_prem            polbasic.tsi_amt%TYPE;
      v_surplus_tsi           polbasic.tsi_amt%TYPE;
      v_surplus_prem          polbasic.tsi_amt%TYPE;
      v_pmmsc_tsi             polbasic.tsi_amt%TYPE;
      v_pmmsc_prem            polbasic.tsi_amt%TYPE;
      v_facul_tsi             polbasic.tsi_amt%TYPE;
      v_facul_prem            polbasic.tsi_amt%TYPE;
      v_other_tsi             polbasic.tsi_amt%TYPE;
      v_other_prem            polbasic.tsi_amt%TYPE;

      v_itm_tsi_amt           polbasic.tsi_amt%TYPE;
      v_itm_prem_amt          polbasic.tsi_amt%TYPE;
      v_itm_netret_tsi        polbasic.tsi_amt%TYPE;
      v_itm_netret_prem       polbasic.tsi_amt%TYPE;
      v_itm_quota_tsi         polbasic.tsi_amt%TYPE;
      v_itm_quota_prem        polbasic.tsi_amt%TYPE;
      v_itm_surplus_tsi       polbasic.tsi_amt%TYPE;
      v_itm_surplus_prem      polbasic.tsi_amt%TYPE;
      v_itm_pmmsc_tsi         polbasic.tsi_amt%TYPE;
      v_itm_pmmsc_prem        polbasic.tsi_amt%TYPE;
      v_itm_facul_tsi         polbasic.tsi_amt%TYPE;
      v_itm_facul_prem        polbasic.tsi_amt%TYPE;
      v_itm_other_tsi         polbasic.tsi_amt%TYPE;
      v_itm_other_prem        polbasic.tsi_amt%TYPE;
      v_x_gratia_cnt          NUMBER(5);
      v_x_gratia              VARCHAR2(1);
      v_pisc_share            dbadm_fi.fire.pisc_share%TYPE; --REV-005393
      v_share_tag             dbadm_fi.fire.share_tag%TYPE; --REV-005393
      v_pol_pol_rec_no        dbadm_fi.fire.pol_rec_no%TYPE; --REV-005393
      v_max_pct               NUMBER; --REV-005393
      v_pisc_lead             VARCHAR2(1); --REV-005393
      v_org_type              VARCHAR2(1);

      out_file               UTL_FILE.file_type;

      CURSOR c1 IS
            SELECT  c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no,c.loss_dt,c.loss_desc, c.loss_det,
                    c.event_no,c.loss_cat_cd,c.curr_cd, c.clm_stat_cd,
                    a.item_no,c.org_type,
                    SUM(NVL(a.clm_shr_ramt,0)) clm_shr_ramt,
                    SUM(NVL(a.exp_shr_ramt,0)) exp_shr_ramt,
                    SUM(NVL(a.clm_pd_amt,0)) clm_shr_pd,
                    SUM(NVL(a.exp_pd_amt,0)) exp_shr_pd
               FROM clmprlds a, clm_hist b, claims c
               WHERE a.co_cd = b.co_cd
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
               AND c.co_cd = g_co_cd
               AND c.line_pref = g_line_pref
               AND c.iss_pref = NVL(g_iss_pref,c.iss_pref)
               AND c.event_no = NVL(g_event_no,c.event_no)
               AND a.hist_seq_no = (SELECT MAX(d.hist_seq_no)
                                       FROM clm_hist d, clmprlds c
                                       WHERE d.co_cd = c.co_cd
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
         ORDER BY c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no;
   BEGIN
      FOR c1_rec IN c1
         LOOP
            IF g_cnt = 0 THEN
               g_cnt          := 1;
               g_file_path    := 'CAT_EVT_FILE_'||g_line_pref||'_'||TO_CHAR(SYSDATE,'MMDDYYYYHH24MISS')||'.csv';
               g_dir_path     := get_directory('DDFDIR');
               out_file       := utl_file.fopen('DDFDIR', g_file_path, 'W');

               UTL_FILE.PUT_LINE(out_file,
                  'ASSURED NAME'  ||','||
                  'VESSEL'||','||
                  'POLICY NUMBER'||','||
                  'INCEPT DATE'||','||
                  'EXPIRY DATE'||','||
                  'ITEM NUMBER'||','||
                  'ITEM TITLE'||','||
                  'OCCUPANCY'||','||
                  'CLAIM NUMBER' ||','||
                  'DATE OF LOSS' ||','||
                  'CURRENCY'||','||
                  'POLICY GROSS SUM INSURED'||','||
                  'POLICY RETAINED SUM INSURED'||','||
                  'POLICY QS SUM INSURED'||','||
                  'POLICY SURPLUS SUM INSURED'||','||
                  'POLICY PMMSC SUM INSURED'||','||
                  'POLICY OTHER TREATY SUM INSURED'||','||
                  'POLICY FACUL SUM INSURED'||','||
                  'ITEM GROSS SUM INSURED'||','||
                  'ITEM RETAINED SUM INSURED'||','||
                  'ITEM QS SUM INSURED'||','||
                  'ITEM SURPLUS SUM INSURED'||','||
                  'ITEM PMMSC SUM INSURED'||','||
                  'ITEM OTHER TREATY SUM INSURED'||','||
                  'ITEM FACUL SUM INSURED'||','||
                  'LOSS RESERVE' ||','||
                  'LOSS PAID'    ||','||
                  'EXPENSE RESERVE'  ||','||
                  'EXPENSE PAID'     ||','||
                  'RECOVERIES'     ||','||
                  'LOSS DESCRIPTION'    ||','||
                  'LOSS DETAIL'  ||','||
                  'STATUS'||','||
                  'FRONTING/REFERRED'||','||
                  'INTM NUMBER'||','||
                  'INTERMEDIARY NAME'||','||
                  'WITH EX-GRATIA'||','||       --REV-004771
                  'CO INSURANCE'||','|| -- REV-005393
                  'PISC LEAD CO I'||','|| -- REV-005393
                  'ORG TYPE'); 
               UTL_FILE.NEW_LINE(out_file,1);
            END IF;
            v_item_no := NULL;
            v_item_title := NULL;
            v_item_desc := NULL;
            v_name_to_appear := NULL;
            v_clm_stat_des := NULL;
            v_intm_no := NULL;
            v_intm_name := NULL;
            v_tsi_amt := NULL;
            v_incept_dt := NULL;
            v_expiry_dt := NULL;
            v_recovery := NULL;
            v_vessel := NULL;
            v_org_type := NULL;

            v_policy := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.pol_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.pol_seq_no,'0999999')))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.ren_seq_no,'09')))||'-'||c1_rec.pol_type;

            v_claim  := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.clm_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.clm_seq_no,'09999')));

            BEGIN
               SELECT a.name_to_appear
                  INTO v_name_to_appear
                  FROM dbadm_fi.polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM dbadm_fi.polbasic u
                                    WHERE u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND u.name_to_appear IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.name_to_appear
                        INTO v_name_to_appear
                        FROM dbadm_fi.polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_name_to_appear := NULL;
                  END;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: name '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT b.intm_no, intm_name
                  INTO v_intm_no, v_intm_name
                  FROM invoice a, invcomm b, intrmdry c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.bill_yy = b.bill_yy
                  AND a.bill_seq_no = b.bill_seq_no
                  AND b.co_cd = c.co_cd
                  AND b.intm_no = c.intm_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.endt_seq_no = 0;
            EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     IF c1_rec.pol_type = 'I' THEN
                        BEGIN
                           SELECT b.ri_cd, ri_name
                              INTO v_intm_no, v_intm_name
                              FROM polbasic a, inpolicy b, rinsurer c
                              WHERE a.pol_rec_no = b.pol_rec_no
                              AND a.co_cd = c.co_cd
                              AND b.ri_cd = c.ri_cd
                              AND a.co_cd = c1_rec.co_cd
                              AND a.line_pref = c1_rec.line_pref
                              AND a.subline_pref = c1_rec.subline_pref
                              AND a.iss_pref = c1_rec.iss_pref
                              AND a.pol_yy = c1_rec.pol_yy
                              AND a.pol_seq_no = c1_rec.pol_seq_no
                              AND a.ren_seq_no = c1_rec.ren_seq_no
                              AND a.endt_seq_no = 0;
                        EXCEPTION
                           WHEN NO_DATA_FOUND THEN
                              v_intm_no := NULL;
                              v_intm_name := NULL;
                        END;
                  ELSE
                     v_intm_no := NULL;
                     v_intm_name := NULL;
                  END IF;
            WHEN TOO_MANY_ROWS THEN
         -- REV-005749 start
                     SELECT b.intm_no, intm_name
                        INTO v_intm_no, v_intm_name
                        FROM invoice a, invcomm b, intrmdry c
                        WHERE a.co_cd = b.co_cd
                        AND a.line_pref = b.line_pref
                        AND a.bill_yy = b.bill_yy
                        AND a.bill_seq_no = b.bill_seq_no
                        AND b.co_cd = c.co_cd
                        AND b.intm_no = c.intm_no
                        AND a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0
                        AND ROWNUM = 1;
         /*         RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: intm '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;*/
         -- REV-005749 end
            END;
            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT b.item_title
                  INTO v_item_title
                  FROM polbasic a, item b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = c1_rec.item_no
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z, item y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.item_no = b.item_no
                                    AND y.item_title IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_title     := ' ';
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: item title '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT b.item_desc
                  INTO v_item_desc
                  FROM polbasic a, item b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = c1_rec.item_no
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z, item y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.item_no = b.item_no
                                    AND y.item_desc IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_desc     := ' ';
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: item desc '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT b.occ_cd
                  INTO v_occ_cd
                  FROM polbasic a, dbadm_fi.fireitem b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND b.item_no = c1_rec.item_no
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u, dbadm_fi.fireitem v
                                    WHERE u.pol_rec_no = v.pol_rec_no
                                 AND u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND v.item_no = b.item_no
                                    AND v.occ_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_occ_cd := NULL;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: occ_cd '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            IF v_occ_cd IS NOT NULL THEN
               BEGIN
                  SELECT occ_title
                     INTO v_occ_title
                     FROM dbadm_fi.occupancy
                     WHERE co_cd = c1_rec.co_cd
                     AND occ_cd = v_occ_cd;
               EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     v_occ_title := NULL;
               END;
            END IF;


            BEGIN
               SELECT a.referring_inst_cd
                  INTO v_referring_inst_cd
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z
                                    WHERE z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND z.referring_inst_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_referring_inst_cd := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: referring_cd '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT ri_name
                  INTO v_referring_name
                  FROM rinsurer
                  WHERE co_cd = c1_rec.co_cd
                  AND ri_cd = v_referring_inst_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_referring_name := NULL;
            END;

            BEGIN
               SELECT a.incept_dt
                  INTO v_incept_dt
                  FROM max_incept_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.incept_dt
                        INTO v_incept_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_incept_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: incept dt '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT a.expiry_dt
                  INTO v_expiry_dt
                  FROM max_expiry_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.expiry_dt
                        INTO v_expiry_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_expiry_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: expiry dt '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
               --RAISE FORM_TRIGGER_FAILURE;
            END;


            BEGIN
               SELECT SUM(NVL(a.loss_adv_amt,0))
                  INTO v_recovery
                  FROM clm_loss a, clm_hist b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.subline_pref = b.subline_pref
                  AND a.iss_pref = b.iss_pref
                  AND a.clm_yy = b.clm_yy
                  AND a.clm_seq_no = b.clm_seq_no
                  AND a.hist_seq_no = b.hist_seq_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.clm_yy = c1_rec.clm_yy
                  AND a.clm_seq_no = c1_rec.clm_seq_no
                  AND a.item_no = c1_rec.item_no
                  AND b.fla_stat_cd != 'C'
                  AND SIGN(a.loss_adv_amt) = -1;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_recovery := NULL;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_prem_amt, v_tsi_amt
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_prem_amt := 0;
                  v_tsi_amt := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_netret_prem, v_netret_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND shr_type = 'N';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_netret_prem := 0;
                  v_netret_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_facul_prem, v_facul_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND shr_type = 'F';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_facul_prem := 0;
                  v_facul_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_surplus_prem, v_surplus_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type IN (1,2));

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_surplus_prem := 0;
                  v_surplus_tsi := 0;
            END;


            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_quota_prem, v_quota_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 3);

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_quota_prem := 0;
                  v_quota_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_pmmsc_prem, v_pmmsc_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 10);

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_pmmsc_prem := 0;
                  v_pmmsc_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_other_prem, v_other_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type NOT IN (1,2,3,10));

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_other_prem := 0;
                  v_other_tsi := 0;
            END;


            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_tsi_amt, v_itm_prem_amt
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_prem_amt := 0;
                  v_itm_tsi_amt := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_netret_tsi, v_itm_netret_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'N';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_netret_prem := 0;
                  v_itm_netret_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_facul_tsi, v_itm_facul_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'F';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_facul_prem := 0;
                  v_itm_facul_tsi := 0;
            END;


            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_surplus_tsi, v_itm_surplus_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type IN (1,2));
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_surplus_prem := 0;
                  v_itm_surplus_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_quota_tsi, v_itm_quota_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 3);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_quota_prem := 0;
                  v_itm_quota_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_pmmsc_tsi, v_itm_pmmsc_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 10);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_pmmsc_prem := 0;
                  v_itm_pmmsc_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_other_tsi, v_itm_other_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type NOT IN (1,2,3,10));
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_other_prem := 0;
                  v_itm_other_tsi := 0;
            END;

         --REV-004771 start
            v_x_gratia_cnt := 0;
            BEGIN
               SELECT count(*)
                  INTO v_x_gratia_cnt
                  FROM clm_loss x, clm_hist y
                  WHERE x.co_cd = y.co_cd
                  AND x.line_pref = y.line_pref
                  AND x.subline_pref = y.subline_pref
                  AND x.iss_pref = y.iss_pref
                  AND x.clm_yy = y.clm_yy
                  AND x.clm_seq_no = y.clm_seq_no
                  AND x.hist_seq_no = y.hist_seq_no
                  AND x.co_cd = c1_rec.co_cd
                  AND x.line_pref = c1_rec.line_pref
                  AND x.subline_pref = c1_rec.subline_pref
                  AND x.iss_pref = c1_rec.iss_pref
                  AND x.clm_yy = c1_rec.clm_yy
                  AND x.clm_seq_no = c1_rec.clm_seq_no
                  AND x.item_no = c1_rec.item_no
                  AND y.fla_stat_cd != 'C'
                  AND (INSTR(UPPER(y.remarks),'GRATIA') <> 0 OR
                                 NVL(x.x_gratia_tag,'N') = 'Y');
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_x_gratia_cnt := 0;
            END;
            IF v_x_gratia_cnt = 0 THEN
               v_x_gratia := 'N';
            ELSE
               v_x_gratia := 'Y';
            END IF;
         --REV-004771 end

         --REV-005393 start --
            BEGIN
               SELECT b.pisc_share, b.share_tag, b.pol_rec_no
                  INTO v_pisc_share, v_share_tag, v_pol_pol_rec_no
                  FROM polbasic a, dbadm_fi.fire b
                  WHERE NVL(a.endt_seq_no,0) = 0
                  AND a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
               v_pisc_share := NULL;
               v_share_tag := NULL;
               v_pol_pol_rec_no := NULL;
               WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_itm - TOO_MANY_ROWS: TOO MANY ENDT_SEQ_NO = 0'||TO_CHAR(V_POL_POL_REC_NO));
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            IF NVL(v_pisc_share,0) <> 0 THEN
               SELECT MAX(share_pct)
                  INTO v_max_pct
                  FROM dbadm_fi.coinsurer
                  WHERE pol_rec_no = v_pol_pol_rec_no
                  AND insurer_cd <> 99996;

               IF v_pisc_share > v_max_pct THEN
                  v_pisc_lead := 'Y';
               ELSIF v_pisc_share = v_max_pct AND v_pisc_share + v_max_pct = 100 THEN
                  v_pisc_lead := 'E';
               ELSE
                  v_pisc_lead := 'N';
               END IF;

            ELSE
               v_pisc_lead := 'N';
               v_share_tag := 'N';
            END IF;
         -- REV-005393 end --
            IF c1_rec.org_type IS NULL THEN
               SELECT GET_ORG_TYPE_POL(c1_rec.co_cd, c1_rec.line_pref, c1_rec.subline_pref, c1_rec.iss_pref, c1_rec.pol_yy, 
                                c1_rec.pol_seq_no, c1_rec.ren_seq_no, c1_rec.pol_type)
                  INTO v_org_type
                  FROM dual;
            ELSE
               v_org_type := c1_rec.org_type;
            END IF;

            UTL_FILE.PUT_LINE(out_file,
               '"'||REPLACE(v_name_to_appear,'"','')||'"'||','|| --REV-12885
               v_vessel||','||
               v_policy||','||
               TO_CHAR(v_incept_dt,'MM/DD/YYYY')||','||
               TO_CHAR(v_expiry_dt,'MM/DD/YYYY')||','||
               TO_CHAR(c1_rec.item_no,'09999')||','||
               '"'||v_item_title||'"'||','||
               '"'||v_occ_title||'"'||','||
               v_claim           ||','||
               TO_CHAR(c1_rec.loss_dt,'MM/DD/YYYY')    ||','||
               c1_rec.curr_cd||','||
               TO_CHAR(NVL(v_tsi_amt,0) ,'99999999999990.00')||','||  --REV-12885
               TO_CHAR(NVL(v_netret_tsi,0) ,'99999999999990.00')||','||  --REV-12885
               TO_CHAR(NVL(v_quota_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_surplus_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_pmmsc_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_other_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_facul_tsi,0) ,'99999999999990.00')||','||  --REV-12885
               TO_CHAR(NVL(v_itm_tsi_amt,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_netret_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_quota_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_surplus_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_pmmsc_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_other_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_facul_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.clm_shr_ramt,0),'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.clm_shr_pd,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.exp_shr_ramt,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.exp_shr_pd,0)  ,'99999999990.00')||','||
               TO_CHAR(NVL(v_recovery,0)  ,'99999999990.00')||','||
               '"'||c1_rec.loss_desc ||'"'||','||
               '"'||c1_rec.loss_det  ||'"'||','||
               '"'||v_clm_stat_des  ||'"'||','||
               '"'||v_referring_name  ||'"'||','||
               TO_CHAR(v_intm_no,'09999')||','||
               '"'||v_intm_name||'"'||','||
               v_x_gratia||','||                    --REV-004771
               v_share_tag||','|| --REV-5393
               v_pisc_lead||','||
               v_org_type); --REV-5393
         END LOOP;
         --
      IF UTL_FILE.is_open(out_file) THEN
         UTL_FILE.fclose(out_file);
      END IF;
   END cat_evt_file_fi_itm;
-------------------------------------------------------------------------------------------------- cat_evt_file_fi_itm

-------------------------------------------------------------------------------------------------- cat_evt_file_fi_pol
   PROCEDURE cat_evt_file_fi_pol IS
      v_policy                VARCHAR2(50);
      v_claim                 VARCHAR2(50);
      v_assd_no               max_assured.assd_no%TYPE;
      v_item_no               item.item_no%TYPE;
      v_item_title            item.item_title%TYPE;
      v_item_desc             item.item_desc%TYPE;
      v_name_to_appear        polbasic.name_to_appear%TYPE;
      v_pol_addr1             polbasic.pol_addr1%TYPE;
      v_pol_addr2             polbasic.pol_addr2%TYPE;
      v_pol_addr3             polbasic.pol_addr3%TYPE;
      v_clm_stat_des          clm_stat.clm_stat_des%TYPE;
      v_intm_no               intrmdry.intm_no%TYPE;
      v_intm_name             VARCHAR2(100);
      v_tsi_amt               polbasic.tsi_amt%TYPE;
      v_prem_amt              polbasic.tsi_amt%TYPE;
      v_incept_dt             polbasic.incept_dt%TYPE;
      v_expiry_dt             polbasic.expiry_dt%TYPE;
      v_recovery              clm_loss.loss_adv_amt%TYPE;
      v_items                 NUMBER(5);
      v_fleet_tag             VARCHAR2(1);
      v_vessel                VARCHAR2(50);
      v_occ_cd                dbadm_fi.occupancy.occ_cd%TYPE;
      v_occ_title             dbadm_fi.occupancy.occ_title%TYPE;
      v_referring_inst_cd     polbasic.referring_inst_cd%TYPE;
      v_referring_name        rinsurer.ri_name%TYPE;
      v_risk_loc_cd           risklocn.risk_loc_cd%TYPE;
      v_risk_loc_desc         risklocn.risk_loc_desc%TYPE;

      v_netret_tsi            polbasic.tsi_amt%TYPE;
      v_netret_prem           polbasic.tsi_amt%TYPE;
      v_quota_tsi             polbasic.tsi_amt%TYPE;
      v_quota_prem            polbasic.tsi_amt%TYPE;
      v_surplus_tsi           polbasic.tsi_amt%TYPE;
      v_surplus_prem          polbasic.tsi_amt%TYPE;
      v_pmmsc_tsi             polbasic.tsi_amt%TYPE;
      v_pmmsc_prem            polbasic.tsi_amt%TYPE;
      v_facul_tsi             polbasic.tsi_amt%TYPE;
      v_facul_prem            polbasic.tsi_amt%TYPE;
      v_other_tsi             polbasic.tsi_amt%TYPE;
      v_other_prem            polbasic.tsi_amt%TYPE;

      v_itm_tsi_amt           polbasic.tsi_amt%TYPE;
      v_itm_prem_amt          polbasic.tsi_amt%TYPE;
      v_itm_netret_tsi        polbasic.tsi_amt%TYPE;
      v_itm_netret_prem       polbasic.tsi_amt%TYPE;
      v_itm_quota_tsi         polbasic.tsi_amt%TYPE;
      v_itm_quota_prem        polbasic.tsi_amt%TYPE;
      v_itm_surplus_tsi       polbasic.tsi_amt%TYPE;
      v_itm_surplus_prem      polbasic.tsi_amt%TYPE;
      v_itm_pmmsc_tsi         polbasic.tsi_amt%TYPE;
      v_itm_pmmsc_prem        polbasic.tsi_amt%TYPE;
      v_itm_facul_tsi         polbasic.tsi_amt%TYPE;
      v_itm_facul_prem        polbasic.tsi_amt%TYPE;
      v_itm_other_tsi         polbasic.tsi_amt%TYPE;
      v_itm_other_prem        polbasic.tsi_amt%TYPE;
      v_x_gratia_cnt          NUMBER(5);
      v_x_gratia              VARCHAR2(1);
      v_pisc_share            dbadm_fi.fire.pisc_share%TYPE;    --REV-005393
      v_share_tag             dbadm_fi.fire.share_tag%TYPE;    --REV-005393
      v_pol_pol_rec_no        dbadm_fi.fire.pol_rec_no%TYPE;    --REV-005393
      v_max_pct               NUMBER;    --REV-005393
      v_pisc_lead             VARCHAR2(1);    --REV-005393
      v_org_type              VARCHAR2(1);

      out_file                UTL_FILE.file_type;

      CURSOR c1 IS
            SELECT  c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no,c.loss_dt,c.loss_desc, c.loss_det,
                    c.event_no,c.loss_cat_cd,c.curr_cd, c.clm_stat_cd, c.org_type,
                    SUM(NVL(a.clm_shr_ramt,0)) clm_shr_ramt,
                    SUM(NVL(a.exp_shr_ramt,0)) exp_shr_ramt,
                    SUM(NVL(a.clm_pd_amt,0)) clm_shr_pd,
                    SUM(NVL(a.exp_pd_amt,0)) exp_shr_pd
               FROM clmprlds a, clm_hist b, claims c
               WHERE a.co_cd = b.co_cd
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
               AND c.co_cd = g_co_cd
               AND c.line_pref = g_line_pref
               AND c.iss_pref = NVL(g_iss_pref,c.iss_pref)
               AND c.event_no = NVL(g_event_no,c.event_no)
               AND a.hist_seq_no = (SELECT MAX(d.hist_seq_no)
                                       FROM clm_hist d, clmprlds c
                                       WHERE d.co_cd = c.co_cd
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
                    c.event_no,c.loss_cat_cd,c.curr_cd,c.clm_stat_cd, c.org_type
         ORDER BY c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no;

   BEGIN
      FOR c1_rec IN c1
         LOOP
            IF g_cnt = 0 THEN
               g_cnt          := 1;
               g_file_path    := 'CAT_EVT_FILE_'||g_line_pref||'_'||TO_CHAR(SYSDATE,'MMDDYYYYHH24MISS')||'.csv';
               g_dir_path     := get_directory('DDFDIR');
               out_file       := utl_file.fopen('DDFDIR', g_file_path, 'W');

               UTL_FILE.PUT_LINE(out_file,
                  'ASSURED NAME'  ||','||
                  'VESSEL'||','||
                  'POLICY NUMBER'||','||
                  'INCEPT DATE'||','||
                  'EXPIRY DATE'||','||
                  'OCCUPANCY'||','||
                  'CLAIM NUMBER' ||','||
                  'DATE OF LOSS' ||','||
                  'CURRENCY'||','||
                  'POLICY GROSS SUM INSURED'||','||
                  'POLICY RETAINED SUM INSURED'||','||
                  'POLICY QS SUM INSURED'||','||
                  'POLICY SURPLUS SUM INSURED'||','||
                  'POLICY PMMSC SUM INSURED'||','||
                  'POLICY OTHER TREATY SUM INSURED'||','||
                  'POLICY FACUL SUM INSURED'||','||
                  'LOSS RESERVE' ||','||
                  'LOSS PAID'    ||','||
                  'EXPENSE RESERVE'  ||','||
                  'EXPENSE PAID'     ||','||
                  'RECOVERIES'     ||','||
                  'LOSS DESCRIPTION'    ||','||
                  'LOSS DETAIL'  ||','||
                  'STATUS'||','||
                  'FRONTING/REFERRED'||','||
                  'INTM NUMBER'||','||
                  'INTERMEDIARY NAME'||','||
                  'RISK LOC CODE'||','||
                  'RISK LOC DESC'||','||
                  'WITH EX-GRATIA'||','||          --REV-004771
                  'CO INSURANCE'||','|| -- REV-005393
                  'PISC LEAD CO I'||','||
                  'ORG_TYPE'); -- REV-005393
               UTL_FILE.NEW_LINE(out_file,1);
            END IF;

            v_item_no := NULL;
            v_item_title := NULL;
            v_item_desc := NULL;
            v_name_to_appear := NULL;
            v_clm_stat_des := NULL;
            v_intm_no := NULL;
            v_intm_name := NULL;
            v_tsi_amt := NULL;
            v_incept_dt := NULL;
            v_expiry_dt := NULL;
            v_recovery := NULL;
            v_vessel := NULL;
            v_risk_loc_cd := NULL;
            v_risk_loc_desc := NULL;
            v_org_type := NULL;

            v_policy := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.pol_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.pol_seq_no,'0999999')))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.ren_seq_no,'09')))||'-'||c1_rec.pol_type;

            v_claim  := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.clm_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.clm_seq_no,'09999')));
            BEGIN
               SELECT a.name_to_appear
                  INTO v_name_to_appear
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u
                                    WHERE u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND u.name_to_appear IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.name_to_appear
                        INTO v_name_to_appear
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_name_to_appear := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: name '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT b.intm_no, intm_name
                  INTO v_intm_no, v_intm_name
                  FROM invoice a, invcomm b, intrmdry c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.bill_yy = b.bill_yy
                  AND a.bill_seq_no = b.bill_seq_no
                  AND b.co_cd = c.co_cd
                  AND b.intm_no = c.intm_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.endt_seq_no = 0;
            EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     IF c1_rec.pol_type = 'I' THEN
                        BEGIN
                           SELECT b.ri_cd, ri_name
                              INTO v_intm_no, v_intm_name
                              FROM polbasic a, inpolicy b, rinsurer c
                              WHERE a.pol_rec_no = b.pol_rec_no
                              AND a.co_cd = c.co_cd
                              AND b.ri_cd = c.ri_cd
                              AND a.co_cd = c1_rec.co_cd
                              AND a.line_pref = c1_rec.line_pref
                              AND a.subline_pref = c1_rec.subline_pref
                              AND a.iss_pref = c1_rec.iss_pref
                              AND a.pol_yy = c1_rec.pol_yy
                              AND a.pol_seq_no = c1_rec.pol_seq_no
                              AND a.ren_seq_no = c1_rec.ren_seq_no
                              AND a.endt_seq_no = 0;
                        EXCEPTION
                           WHEN NO_DATA_FOUND THEN
                              v_intm_no := NULL;
                              v_intm_name := NULL;
                        END;
                  ELSE
                     v_intm_no := NULL;
                     v_intm_name := NULL;
                  END IF;
               WHEN TOO_MANY_ROWS THEN
   --    REV-005749 start
                  SELECT b.intm_no, intm_name
                     INTO v_intm_no, v_intm_name
                     FROM invoice a, invcomm b, intrmdry c
                     WHERE a.co_cd = b.co_cd
                     AND a.line_pref = b.line_pref
                     AND a.bill_yy = b.bill_yy
                     AND a.bill_seq_no = b.bill_seq_no
                     AND b.co_cd = c.co_cd
                     AND b.intm_no = c.intm_no
                     AND a.co_cd = c1_rec.co_cd
                     AND a.line_pref = c1_rec.line_pref
                     AND a.subline_pref = c1_rec.subline_pref
                     AND a.iss_pref = c1_rec.iss_pref
                     AND a.pol_yy = c1_rec.pol_yy
                     AND a.pol_seq_no = c1_rec.pol_seq_no
                     AND a.ren_seq_no = c1_rec.ren_seq_no
                     AND a.endt_seq_no = 0
                     AND ROWNUM = 1;
   /*                RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: intm '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;*/
   --    REV-005749 end
            END;
            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT MIN(b.item_no)
                  INTO v_item_no
                  FROM clm_loss b
                  WHERE b.co_cd = c1_rec.co_cd
                  AND b.line_pref = c1_rec.line_pref
                  AND b.subline_pref = c1_rec.subline_pref
                  AND b.iss_pref = c1_rec.iss_pref
                  AND b.clm_yy = c1_rec.clm_yy
                  AND b.clm_seq_no = c1_rec.clm_seq_no
                  AND b.hist_seq_no = (SELECT MAX(x.hist_seq_no)
                                          FROM clm_hist x
                                          WHERE b.co_cd = x.co_cd
                                          AND b.line_pref = x.line_pref
                                          AND b.subline_pref = x.subline_pref
                                          AND b.iss_pref = x.iss_pref
                                          AND b.clm_yy = x.clm_yy
                                          AND b.clm_seq_no = x.clm_seq_no
                                          AND x.fla_stat_cd != 'C');
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_no := NULL;
               WHEN TOO_MANY_ROWS THEN                               --REV-002860
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: item_no '||v_policy||' '||v_claim);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT b.occ_cd
                  INTO v_occ_cd
                  FROM polbasic a, dbadm_fi.fireitem b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND b.item_no = v_item_no
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u, dbadm_fi.fireitem v
                                    WHERE u.pol_rec_no = v.pol_rec_no
                                    AND u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND v.item_no = b.item_no
                                    AND v.occ_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_occ_cd := NULL;
            WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: occ_cd '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            IF v_occ_cd IS NOT NULL THEN
               BEGIN
                  SELECT occ_title
                     INTO v_occ_title
                     FROM dbadm_fi.occupancy
                     WHERE co_cd = c1_rec.co_cd
                     AND occ_cd = v_occ_cd;
               EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     v_occ_title := NULL;
               END;
            END IF;

            BEGIN
               SELECT b.risk_loc_cd
                  INTO v_risk_loc_cd
                  FROM polbasic a, dbadm_fi.fireitem b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND b.item_no = v_item_no
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u, dbadm_fi.fireitem v
                                    WHERE u.pol_rec_no = v.pol_rec_no
                                    AND u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND v.item_no = b.item_no
                                    AND v.risk_loc_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_risk_loc_cd := NULL;
            WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: risk_loc_cd '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            IF v_risk_loc_cd IS NOT NULL THEN
               BEGIN
                  SELECT risk_loc_desc
                     INTO v_risk_loc_desc
                     FROM dbadm.risklocn
                     WHERE co_cd = c1_rec.co_cd
                     AND risk_loc_cd = v_risk_loc_cd;
               EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     v_risk_loc_desc := NULL;
               END;
            END IF;

            BEGIN
               SELECT a.referring_inst_cd
                  INTO v_referring_inst_cd
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z
                                    WHERE z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND z.referring_inst_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_referring_inst_cd := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: referring_cd '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT ri_name
                  INTO v_referring_name
                  FROM rinsurer
                  WHERE co_cd = c1_rec.co_cd
                  AND ri_cd = v_referring_inst_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_referring_name := NULL;
            END;

            BEGIN
               SELECT a.incept_dt
                  INTO v_incept_dt
                  FROM max_incept_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.incept_dt
                        INTO v_incept_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_incept_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: incept dt '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT a.expiry_dt
                  INTO v_expiry_dt
                  FROM max_expiry_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.expiry_dt
                        INTO v_expiry_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_expiry_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: expiry dt '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
               --RAISE FORM_TRIGGER_FAILURE;
            END;


            BEGIN
               SELECT SUM(NVL(a.loss_adv_amt,0))
                  INTO v_recovery
                  FROM clm_loss a, clm_hist b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.subline_pref = b.subline_pref
                  AND a.iss_pref = b.iss_pref
                  AND a.clm_yy = b.clm_yy
                  AND a.clm_seq_no = b.clm_seq_no
                  AND a.hist_seq_no = b.hist_seq_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.clm_yy = c1_rec.clm_yy
                  AND a.clm_seq_no = c1_rec.clm_seq_no
                  AND b.fla_stat_cd != 'C'
                  AND SIGN(a.loss_adv_amt) = -1;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_recovery := NULL;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_prem_amt, v_tsi_amt
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_prem_amt := 0;
                  v_tsi_amt := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_netret_prem, v_netret_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND shr_type = 'N';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_netret_prem := 0;
                  v_netret_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_facul_prem, v_facul_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND shr_type = 'F';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_facul_prem := 0;
                  v_facul_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_surplus_prem, v_surplus_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type IN (1,2));

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_surplus_prem := 0;
                  v_surplus_tsi := 0;
            END;


            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_quota_prem, v_quota_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 3);

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_quota_prem := 0;
                  v_quota_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_pmmsc_prem, v_pmmsc_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 10);

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_pmmsc_prem := 0;
                  v_pmmsc_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_other_prem, v_other_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type NOT IN (1,2,3,10));

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_other_prem := 0;
                  v_other_tsi := 0;
            END;

         --REV-004771 start
            v_x_gratia_cnt := 0;
            BEGIN
               SELECT count(*)
                  INTO v_x_gratia_cnt
                  FROM clm_loss x, clm_hist y
                  WHERE x.co_cd = y.co_cd
                  AND x.line_pref = y.line_pref
                  AND x.subline_pref = y.subline_pref
                  AND x.iss_pref = y.iss_pref
                  AND x.clm_yy = y.clm_yy
                  AND x.clm_seq_no = y.clm_seq_no
                  AND x.hist_seq_no = y.hist_seq_no
                  AND x.co_cd = c1_rec.co_cd
                  AND x.line_pref = c1_rec.line_pref
                  AND x.subline_pref = c1_rec.subline_pref
                  AND x.iss_pref = c1_rec.iss_pref
                  AND x.clm_yy = c1_rec.clm_yy
                  AND x.clm_seq_no = c1_rec.clm_seq_no
                  AND y.fla_stat_cd != 'C'
                  AND (INSTR(UPPER(y.remarks),'GRATIA') <> 0 OR
                                 NVL(x.x_gratia_tag,'N') = 'Y');
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_x_gratia_cnt := 0;
            END;
            IF v_x_gratia_cnt = 0 THEN
               v_x_gratia := 'N';
            ELSE
               v_x_gratia := 'Y';
            END IF;
         --REV-004771 end

         -- REV-005393 start --
         BEGIN
            SELECT b.pisc_share, b.share_tag, b.pol_rec_no
               INTO v_pisc_share, v_share_tag, v_pol_pol_rec_no
               FROM polbasic a, dbadm_fi.fire b
               WHERE NVL(a.endt_seq_no,0) = 0
               AND a.pol_rec_no = b.pol_rec_no
               AND a.co_cd = c1_rec.co_cd
               AND a.line_pref = c1_rec.line_pref
               AND a.subline_pref = c1_rec.subline_pref
               AND a.iss_pref = c1_rec.iss_pref
               AND a.pol_yy = c1_rec.pol_yy
               AND a.pol_seq_no = c1_rec.pol_seq_no
               AND a.ren_seq_no = c1_rec.ren_seq_no
               AND a.pol_type = c1_rec.pol_type;

         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               v_pisc_share := NULL;
               v_share_tag := NULL;
               v_pol_pol_rec_no := NULL;

            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_fi_pol - TOO_MANY_ROWS: Too many ENDT_SEQ_NO = 0'||TO_CHAR(v_pol_pol_rec_no));
               --RAISE FORM_TRIGGER_FAILURE;
         END;

         IF NVL(v_pisc_share,0) <> 0 THEN
            SELECT MAX(share_pct)
               INTO v_max_pct
               FROM dbadm_fi.coinsurer
               WHERE pol_rec_no = v_pol_pol_rec_no
               AND insurer_cd <> 99996;

            IF v_pisc_share > v_max_pct THEN
               v_pisc_lead := 'Y';
            ELSIF v_pisc_share = v_max_pct AND v_pisc_share + v_max_pct = 100 THEN
               v_pisc_lead := 'E';
            ELSE
               v_pisc_lead := 'N';
            END IF;
         ELSE
            v_pisc_lead := 'N';
            v_share_tag := 'N';
         END IF;
         -- REV-005393 end --
         IF c1_rec.org_type IS NULL THEN
               SELECT GET_ORG_TYPE_POL(c1_rec.co_cd, c1_rec.line_pref, c1_rec.subline_pref, c1_rec.iss_pref, c1_rec.pol_yy, 
                                c1_rec.pol_seq_no, c1_rec.ren_seq_no, c1_rec.pol_type)
                  INTO v_org_type
                  FROM dual;
         ELSE
            v_org_type := c1_rec.org_type;
         END IF;

         UTL_FILE.PUT_LINE(out_file,
            '"'||REPLACE(v_name_to_appear,'"','')||'"'||','|| --REV-12885
            v_vessel||','||
            v_policy||','||
            TO_CHAR(v_incept_dt,'MM/DD/YYYY')||','||
            TO_CHAR(v_expiry_dt,'MM/DD/YYYY')||','||
            '"'||v_occ_title||'"'||','||
            v_claim           ||','||
            TO_CHAR(c1_rec.loss_dt,'MM/DD/YYYY')    ||','||
            c1_rec.curr_cd||','||
            TO_CHAR(NVL(v_tsi_amt,0) ,'99999999999990.00')||','||  --REV-12885
            TO_CHAR(NVL(v_netret_tsi,0) ,'99999999999990.00')||','||  --REV-12885
            TO_CHAR(NVL(v_quota_tsi,0) ,'99999999990.00')||','||
            TO_CHAR(NVL(v_surplus_tsi,0) ,'99999999990.00')||','||
            TO_CHAR(NVL(v_pmmsc_tsi,0) ,'99999999990.00')||','||
            TO_CHAR(NVL(v_other_tsi,0) ,'99999999990.00')||','||
            TO_CHAR(NVL(v_facul_tsi,0) ,'99999999999990.00')||','||  --REV-12885
            TO_CHAR(NVL(c1_rec.clm_shr_ramt,0),'99999999990.00')||','||
            TO_CHAR(NVL(c1_rec.clm_shr_pd,0) ,'99999999990.00')||','||
            TO_CHAR(NVL(c1_rec.exp_shr_ramt,0) ,'99999999990.00')||','||
            TO_CHAR(NVL(c1_rec.exp_shr_pd,0)  ,'99999999990.00')||','||
            TO_CHAR(NVL(v_recovery,0)  ,'99999999990.00')||','||
            '"'||c1_rec.loss_desc ||'"'||','||
            '"'||c1_rec.loss_det  ||'"'||','||
            '"'||v_clm_stat_des  ||'"'||','||
            '"'||v_referring_name  ||'"'||','||
            TO_CHAR(v_intm_no,'09999')||','||
            '"'||v_intm_name||'"'||','||
            '"'||v_risk_loc_cd||'"'||','||
            '"'||v_risk_loc_desc||'"'||','||
            v_x_gratia||','||                       --REV-004771
            v_share_tag||','||    --REV-5393
            v_pisc_lead||','||
            v_org_type);    --REV-5393
         END LOOP;
         --
      IF UTL_FILE.is_open(out_file) THEN
         UTL_FILE.fclose(out_file);
      END IF;
   END cat_evt_file_fi_pol;
-------------------------------------------------------------------------------------------------- cat_evt_file_fi_pol

-------------------------------------------------------------------------------------------------- cat_evt_file_cas_itm
   PROCEDURE cat_evt_file_cas_itm IS
      v_policy            VARCHAR2(50);
      v_claim            VARCHAR2(50);
      v_assd_no            max_assured.assd_no%TYPE;
      v_item_no            item.item_no%TYPE;
      v_item_title         item.item_title%TYPE;
      v_item_desc          item.item_desc%TYPE;
      v_name_to_appear     polbasic.name_to_appear%TYPE;
      v_pol_addr1          polbasic.pol_addr1%TYPE;
      v_pol_addr2          polbasic.pol_addr2%TYPE;
      v_pol_addr3          polbasic.pol_addr3%TYPE;
      v_clm_stat_des       clm_stat.clm_stat_des%TYPE;
      v_intm_no            intrmdry.intm_no%TYPE;
      v_intm_name          VARCHAR2(100);
      v_tsi_amt            polbasic.tsi_amt%TYPE;
      v_prem_amt           polbasic.tsi_amt%TYPE;
      v_incept_dt          polbasic.incept_dt%TYPE;
      v_expiry_dt          polbasic.expiry_dt%TYPE;
      v_recovery           clm_loss.loss_adv_amt%TYPE;
      v_items              NUMBER(5);
      v_fleet_tag          VARCHAR2(1);
      v_vessel             VARCHAR2(50);
      v_referring_inst_cd  dbadm_cas.polbasic.referring_inst_cd%TYPE;
      v_referring_name     rinsurer.ri_name%TYPE;

      v_netret_tsi         polbasic.tsi_amt%TYPE;
      v_netret_prem        polbasic.tsi_amt%TYPE;
      v_quota_tsi          polbasic.tsi_amt%TYPE;
      v_quota_prem         polbasic.tsi_amt%TYPE;
      v_surplus_tsi        polbasic.tsi_amt%TYPE;
      v_surplus_prem       polbasic.tsi_amt%TYPE;
      v_pmmsc_tsi          polbasic.tsi_amt%TYPE;
      v_pmmsc_prem         polbasic.tsi_amt%TYPE;
      v_facul_tsi          polbasic.tsi_amt%TYPE;
      v_facul_prem         polbasic.tsi_amt%TYPE;
      v_other_tsi          polbasic.tsi_amt%TYPE;
      v_other_prem         polbasic.tsi_amt%TYPE;

      v_itm_tsi_amt        polbasic.tsi_amt%TYPE;
      v_itm_prem_amt       polbasic.tsi_amt%TYPE;
      v_itm_netret_tsi     polbasic.tsi_amt%TYPE;
      v_itm_netret_prem    polbasic.tsi_amt%TYPE;
      v_itm_quota_tsi      polbasic.tsi_amt%TYPE;
      v_itm_quota_prem     polbasic.tsi_amt%TYPE;
      v_itm_surplus_tsi    polbasic.tsi_amt%TYPE;
      v_itm_surplus_prem   polbasic.tsi_amt%TYPE;
      v_itm_pmmsc_tsi      polbasic.tsi_amt%TYPE;
      v_itm_pmmsc_prem     polbasic.tsi_amt%TYPE;
      v_itm_facul_tsi      polbasic.tsi_amt%TYPE;
      v_itm_facul_prem     polbasic.tsi_amt%TYPE;
      v_itm_other_tsi      polbasic.tsi_amt%TYPE;
      v_itm_other_prem     polbasic.tsi_amt%TYPE;
      v_x_gratia_cnt       NUMBER(5);
      v_x_gratia           VARCHAR2(1);
      v_org_type           VARCHAR2(1);

      out_file            UTL_FILE.file_type;

      CURSOR c1 IS
            SELECT  c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no,c.loss_dt,c.loss_desc, c.loss_det,
                    c.event_no,c.loss_cat_cd,c.curr_cd, c.clm_stat_cd,
                    a.item_no, c.org_type,
                    SUM(NVL(a.clm_shr_ramt,0)) clm_shr_ramt,
                    SUM(NVL(a.exp_shr_ramt,0)) exp_shr_ramt,
                    SUM(NVL(a.clm_pd_amt,0)) clm_shr_pd,
                    SUM(NVL(a.exp_pd_amt,0)) exp_shr_pd
               FROM clmprlds a, clm_hist b, claims c
               WHERE a.co_cd = b.co_cd
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
               AND c.co_cd = g_co_cd
               AND c.line_pref = g_line_pref
               AND c.iss_pref = NVL(g_iss_pref,c.iss_pref)
               AND c.event_no = NVL(g_event_no,c.event_no)
               AND a.hist_seq_no = (SELECT MAX(d.hist_seq_no)
                                       FROM clm_hist d, clmprlds c
                                       WHERE d.co_cd = c.co_cd
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
         ORDER BY c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no;
   BEGIN
      FOR c1_rec IN c1
         LOOP
            IF g_cnt = 0 THEN
               g_cnt          := 1;
               g_file_path    := 'CAT_EVT_FILE_'||g_line_pref||'_'||TO_CHAR(SYSDATE,'MMDDYYYYHH24MISS')||'.csv';
               g_dir_path     := get_directory('DDFDIR');
               out_file       := utl_file.fopen('DDFDIR', g_file_path, 'W');

               UTL_FILE.PUT_LINE(out_file,
                  'ASSURED NAME'  ||','||
                  'VESSEL'||','||
                  'POLICY NUMBER'||','||
                  'INCEPT DATE'||','||
                  'EXPIRY DATE'||','||
                  'ITEM NUMBER'||','||
                  'ITEM TITLE'||','||
                  'CLAIM NUMBER' ||','||
                  'DATE OF LOSS' ||','||
                  'CURRENCY'||','||
                  'POLICY GROSS SUM INSURED'||','||
                  'POLICY RETAINED SUM INSURED'||','||
                  'POLICY QS SUM INSURED'||','||
                  'POLICY SURPLUS SUM INSURED'||','||
                  'POLICY PMMSC SUM INSURED'||','||
                  'POLICY OTHER TREATY SUM INSURED'||','||
                  'POLICY FACUL SUM INSURED'||','||
                  'ITEM GROSS SUM INSURED'||','||
                  'ITEM RETAINED SUM INSURED'||','||
                  'ITEM QS SUM INSURED'||','||
                  'ITEM SURPLUS SUM INSURED'||','||
                  'ITEM PMMSC SUM INSURED'||','||
                  'ITEM OTHER TREATY SUM INSURED'||','||
                  'ITEM FACUL SUM INSURED'||','||
                  'LOSS RESERVE' ||','||
                  'LOSS PAID'    ||','||
                  'EXPENSE RESERVE'  ||','||
                  'EXPENSE PAID'     ||','||
                  'RECOVERIES'     ||','||
                  'LOSS DESCRIPTION'    ||','||
                  'LOSS DETAIL'  ||','||
                  'STATUS'||','||
                  'FRONTING/REFERRED'||','||
                  'INTM NUMBER'||','||
                  'INTERMEDIARY NAME'||','||
                  'WITH EX-GRATIA'||','||
                  'ORG TYPE');
               UTL_FILE.NEW_LINE(out_file,1);
            END IF;

            v_item_no := NULL;
            v_item_title := NULL;
            v_item_desc := NULL;
            v_name_to_appear := NULL;
            v_clm_stat_des := NULL;
            v_intm_no := NULL;
            v_intm_name := NULL;
            v_tsi_amt := NULL;
            v_incept_dt := NULL;
            v_expiry_dt := NULL;
            v_recovery := NULL;
            v_vessel := NULL;
            v_org_type := NULL;

            v_policy := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.pol_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.pol_seq_no,'0999999')))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.ren_seq_no,'09')))||'-'||c1_rec.pol_type;

            v_claim  := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.clm_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.clm_seq_no,'09999')));

            BEGIN
               SELECT a.name_to_appear
                  INTO v_name_to_appear
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u
                                    WHERE u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND u.name_to_appear IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.name_to_appear
                        INTO v_name_to_appear
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_name_to_appear := NULL;
                  END;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: name '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT b.intm_no, intm_name
                  INTO v_intm_no, v_intm_name
                  FROM invoice a, invcomm b, intrmdry c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.bill_yy = b.bill_yy
                  AND a.bill_seq_no = b.bill_seq_no
                  AND b.co_cd = c.co_cd
                  AND b.intm_no = c.intm_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.endt_seq_no = 0;
            EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     IF c1_rec.pol_type = 'I' THEN
                        BEGIN
                           SELECT b.ri_cd, ri_name
                              INTO v_intm_no, v_intm_name
                              FROM polbasic a, inpolicy b, rinsurer c
                              WHERE a.pol_rec_no = b.pol_rec_no
                              AND a.co_cd = c.co_cd
                              AND b.ri_cd = c.ri_cd
                              AND a.co_cd = c1_rec.co_cd
                              AND a.line_pref = c1_rec.line_pref
                              AND a.subline_pref = c1_rec.subline_pref
                              AND a.iss_pref = c1_rec.iss_pref
                              AND a.pol_yy = c1_rec.pol_yy
                              AND a.pol_seq_no = c1_rec.pol_seq_no
                              AND a.ren_seq_no = c1_rec.ren_seq_no
                              AND a.endt_seq_no = 0;
                        EXCEPTION
                           WHEN NO_DATA_FOUND THEN
                              v_intm_no := NULL;
                              v_intm_name := NULL;
                        END;
                  ELSE
                     v_intm_no := NULL;
                     v_intm_name := NULL;
                  END IF;
            WHEN TOO_MANY_ROWS THEN
   --    REV-005749 start
                     SELECT b.intm_no, intm_name
                        INTO v_intm_no, v_intm_name
                        FROM invoice a, invcomm b, intrmdry c
                        WHERE a.co_cd = b.co_cd
                        AND a.line_pref = b.line_pref
                        AND a.bill_yy = b.bill_yy
                        AND a.bill_seq_no = b.bill_seq_no
                        AND b.co_cd = c.co_cd
                        AND b.intm_no = c.intm_no
                        AND a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0
                        AND ROWNUM = 1;
   /*                   v_intm_no := NULL;
                     v_intm_name := NULL;
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: intm '||v_policy);*/
   --    REV-005749 end
               ----RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT b.item_title
                  INTO v_item_title
                  FROM polbasic a, item b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = c1_rec.item_no
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z, item y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.item_no = b.item_no
                                    AND y.item_title IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_title     := ' ';
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: item title '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT b.item_desc
                  INTO v_item_desc
                  FROM polbasic a, item b
                  WHERE a.pol_rec_no = b.pol_rec_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND b.item_no = c1_rec.item_no
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z, item y
                                    WHERE z.pol_rec_no = y.pol_rec_no
                                    AND z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND y.item_no = b.item_no
                                    AND y.item_desc IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_desc     := ' ';
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: item desc '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT a.referring_inst_cd
                  INTO v_referring_inst_cd
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z
                                    WHERE z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND z.referring_inst_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_referring_inst_cd := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: referring_cd '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT ri_name
                  INTO v_referring_name
                  FROM rinsurer
                  WHERE co_cd = c1_rec.co_cd
                  AND ri_cd = v_referring_inst_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_referring_name := NULL;
            END;

            BEGIN
               SELECT a.incept_dt
                  INTO v_incept_dt
                  FROM max_incept_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.incept_dt
                        INTO v_incept_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_incept_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: incept dt '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT a.expiry_dt
                  INTO v_expiry_dt
                  FROM max_expiry_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.expiry_dt
                        INTO v_expiry_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_expiry_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: expiry dt '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_itm - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
               --RAISE FORM_TRIGGER_FAILURE;
            END;


            BEGIN
               SELECT SUM(NVL(a.loss_adv_amt,0))
                  INTO v_recovery
                  FROM clm_loss a, clm_hist b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.subline_pref = b.subline_pref
                  AND a.iss_pref = b.iss_pref
                  AND a.clm_yy = b.clm_yy
                  AND a.clm_seq_no = b.clm_seq_no
                  AND a.hist_seq_no = b.hist_seq_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.clm_yy = c1_rec.clm_yy
                  AND a.clm_seq_no = c1_rec.clm_seq_no
                  AND a.item_no = c1_rec.item_no
                  AND b.fla_stat_cd != 'C'
                  AND SIGN(a.loss_adv_amt) = -1;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_recovery := NULL;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_prem_amt, v_tsi_amt
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_prem_amt := 0;
                  v_tsi_amt := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_netret_prem, v_netret_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND shr_type = 'N';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_netret_prem := 0;
                  v_netret_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_facul_prem, v_facul_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND shr_type = 'F';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_facul_prem := 0;
                  v_facul_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_surplus_prem, v_surplus_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type IN (1,2));

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_surplus_prem := 0;
                  v_surplus_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_quota_prem, v_quota_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 3);

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_quota_prem := 0;
                  v_quota_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_pmmsc_prem, v_pmmsc_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 10);

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_pmmsc_prem := 0;
                  v_pmmsc_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_other_prem, v_other_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type NOT IN (1,2,3,10));

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_other_prem := 0;
                  v_other_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_tsi_amt, v_itm_prem_amt
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_prem_amt := 0;
                  v_itm_tsi_amt := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_netret_tsi, v_itm_netret_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'N';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_netret_prem := 0;
                  v_itm_netret_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_facul_tsi, v_itm_facul_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'F';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_facul_prem := 0;
                  v_itm_facul_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_surplus_tsi, v_itm_surplus_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type IN (1,2));
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_surplus_prem := 0;
                  v_itm_surplus_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_quota_tsi, v_itm_quota_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 3);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_quota_prem := 0;
                  v_itm_quota_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_pmmsc_tsi, v_itm_pmmsc_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 10);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_pmmsc_prem := 0;
                  v_itm_pmmsc_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(c.tsi_amt,0)*NVL(b.shr_pct,0)/100),
                     SUM(NVL(c.prem_amt,0)*NVL(b.shr_pct,0)/100)
                  INTO v_itm_other_tsi, v_itm_other_prem
                  FROM pol_dist a, policyds b, itemsrel c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c.co_cd
                  AND a.line_pref = c.line_pref
                  AND a.dist_no = c.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND c.item_no = c1_rec.item_no
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type NOT IN (1,2,3,10));
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_itm_other_prem := 0;
                  v_itm_other_tsi := 0;
            END;

   --R   EV-004771 start
            v_x_gratia_cnt := 0;
            BEGIN
               SELECT count(*)
                  INTO v_x_gratia_cnt
                  FROM clm_loss x, clm_hist y
                  WHERE x.co_cd = y.co_cd
                  AND x.line_pref = y.line_pref
                  AND x.subline_pref = y.subline_pref
                  AND x.iss_pref = y.iss_pref
                  AND x.clm_yy = y.clm_yy
                  AND x.clm_seq_no = y.clm_seq_no
                  AND x.hist_seq_no = y.hist_seq_no
                  AND x.co_cd = c1_rec.co_cd
                  AND x.line_pref = c1_rec.line_pref
                  AND x.subline_pref = c1_rec.subline_pref
                  AND x.iss_pref = c1_rec.iss_pref
                  AND x.clm_yy = c1_rec.clm_yy
                  AND x.clm_seq_no = c1_rec.clm_seq_no
                  AND x.item_no = c1_rec.item_no
                  AND y.fla_stat_cd != 'C'
                  AND (INSTR(UPPER(y.remarks),'GRATIA') <> 0 OR
                                 NVL(x.x_gratia_tag,'N') = 'Y');
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_x_gratia_cnt := 0;
            END;
            IF v_x_gratia_cnt = 0 THEN
               v_x_gratia := 'N';
            ELSE
               v_x_gratia := 'Y';
            END IF;
            IF c1_rec.org_type IS NULL THEN 
               SELECT GET_ORG_TYPE_POL(c1_rec.co_cd, c1_rec.line_pref, c1_rec.subline_pref, c1_rec.iss_pref, c1_rec.pol_yy, 
                                c1_rec.pol_seq_no, c1_rec.ren_seq_no, c1_rec.pol_type)
                  INTO v_org_type
                  FROM dual;
            ELSE
               v_org_type := c1_rec.org_type;
            END IF;            

            UTL_FILE.PUT_LINE(out_file,
               '"'||REPLACE(v_name_to_appear,'"','')||'"'||','|| --REV-12885
               v_vessel||','||
               v_policy||','||
               TO_CHAR(v_incept_dt,'MM/DD/YYYY')||','||
               TO_CHAR(v_expiry_dt,'MM/DD/YYYY')||','||
               TO_CHAR(c1_rec.item_no,'09999')||','||
               '"'||v_item_title||'"'||','||
               v_claim           ||','||
               TO_CHAR(c1_rec.loss_dt,'MM/DD/YYYY')    ||','||
               c1_rec.curr_cd||','||
               TO_CHAR(NVL(v_tsi_amt,0) ,'99999999999990.00')||','||  --REV-12885
               TO_CHAR(NVL(v_netret_tsi,0) ,'99999999999990.00')||','||  --REV-12885
               TO_CHAR(NVL(v_quota_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_surplus_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_pmmsc_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_other_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_facul_tsi,0) ,'99999999999990.00')||','||  --REV-12885
               TO_CHAR(NVL(v_itm_tsi_amt,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_netret_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_quota_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_surplus_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_pmmsc_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_other_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_itm_facul_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.clm_shr_ramt,0),'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.clm_shr_pd,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.exp_shr_ramt,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.exp_shr_pd,0)  ,'99999999990.00')||','||
               TO_CHAR(NVL(v_recovery,0)  ,'99999999990.00')||','||
               '"'||c1_rec.loss_desc ||'"'||','||
               '"'||c1_rec.loss_det  ||'"'||','||
               '"'||v_clm_stat_des  ||'"'||','||
               '"'||v_referring_name  ||'"'||','||
               TO_CHAR(v_intm_no,'09999')||','||
               '"'||v_intm_name||'"'||','||
               v_x_gratia||','||
               v_org_type);
         END LOOP;
       --
      IF UTL_FILE.is_open(out_file) THEN
          UTL_FILE.fclose(out_file);
      END IF;
   END cat_evt_file_cas_itm;
-------------------------------------------------------------------------------------------------- cat_evt_file_cas_itm

-------------------------------------------------------------------------------------------------- cat_evt_file_cas_pol
   PROCEDURE cat_evt_file_cas_pol IS
      v_policy               VARCHAR2(50);
      v_claim               VARCHAR2(50);
      v_assd_no               max_assured.assd_no%TYPE;
      v_item_no               item.item_no%TYPE;
      v_item_title            item.item_title%TYPE;
      v_item_desc             item.item_desc%TYPE;
      v_name_to_appear        polbasic.name_to_appear%TYPE;
      v_pol_addr1             polbasic.pol_addr1%TYPE;
      v_pol_addr2             polbasic.pol_addr2%TYPE;
      v_pol_addr3             polbasic.pol_addr3%TYPE;
      v_clm_stat_des          clm_stat.clm_stat_des%TYPE;
      v_intm_no               intrmdry.intm_no%TYPE;
      v_intm_name             VARCHAR2(100);
      v_tsi_amt               polbasic.tsi_amt%TYPE;
      v_prem_amt              polbasic.tsi_amt%TYPE;
      v_incept_dt             polbasic.incept_dt%TYPE;
      v_expiry_dt             polbasic.expiry_dt%TYPE;
      v_recovery              clm_loss.loss_adv_amt%TYPE;
      v_items                 NUMBER(5);
      v_fleet_tag             VARCHAR2(1);
      v_vessel                VARCHAR2(50);
      v_referring_inst_cd     polbasic.referring_inst_cd%TYPE;
      v_referring_name        rinsurer.ri_name%TYPE;
      v_risk_loc_cd           risklocn.risk_loc_cd%TYPE;
      v_risk_loc_desc         risklocn.risk_loc_desc%TYPE;

      v_netret_tsi            polbasic.tsi_amt%TYPE;
      v_netret_prem           polbasic.tsi_amt%TYPE;
      v_quota_tsi             polbasic.tsi_amt%TYPE;
      v_quota_prem            polbasic.tsi_amt%TYPE;
      v_surplus_tsi           polbasic.tsi_amt%TYPE;
      v_surplus_prem          polbasic.tsi_amt%TYPE;
      v_pmmsc_tsi             polbasic.tsi_amt%TYPE;
      v_pmmsc_prem            polbasic.tsi_amt%TYPE;
      v_facul_tsi             polbasic.tsi_amt%TYPE;
      v_facul_prem            polbasic.tsi_amt%TYPE;
      v_other_tsi             polbasic.tsi_amt%TYPE;
      v_other_prem            polbasic.tsi_amt%TYPE;

      v_itm_tsi_amt           polbasic.tsi_amt%TYPE;
      v_itm_prem_amt          polbasic.tsi_amt%TYPE;
      v_itm_netret_tsi        polbasic.tsi_amt%TYPE;
      v_itm_netret_prem       polbasic.tsi_amt%TYPE;
      v_itm_quota_tsi         polbasic.tsi_amt%TYPE;
      v_itm_quota_prem        polbasic.tsi_amt%TYPE;
      v_itm_surplus_tsi       polbasic.tsi_amt%TYPE;
      v_itm_surplus_prem      polbasic.tsi_amt%TYPE;
      v_itm_pmmsc_tsi         polbasic.tsi_amt%TYPE;
      v_itm_pmmsc_prem        polbasic.tsi_amt%TYPE;
      v_itm_facul_tsi         polbasic.tsi_amt%TYPE;
      v_itm_facul_prem        polbasic.tsi_amt%TYPE;
      v_itm_other_tsi         polbasic.tsi_amt%TYPE;
      v_itm_other_prem        polbasic.tsi_amt%TYPE;
      v_x_gratia_cnt          NUMBER(5);
      v_x_gratia              VARCHAR2(1);
      v_org_type              VARCHAR2(1);

      out_file               UTL_FILE.file_type;

      CURSOR c1 IS
            SELECT  c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no,c.loss_dt,c.loss_desc, c.loss_det,
                    c.event_no,c.loss_cat_cd,c.curr_cd, c.clm_stat_cd, c.org_type,
                    SUM(NVL(a.clm_shr_ramt,0)) clm_shr_ramt,
                    SUM(NVL(a.exp_shr_ramt,0)) exp_shr_ramt,
                    SUM(NVL(a.clm_pd_amt,0)) clm_shr_pd,
                    SUM(NVL(a.exp_pd_amt,0)) exp_shr_pd
               FROM clmprlds a, clm_hist b, claims c
               WHERE a.co_cd = b.co_cd
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
               AND c.co_cd = g_co_cd
               AND c.line_pref = g_line_pref
               AND c.iss_pref = NVL(g_iss_pref,c.iss_pref)
               AND c.event_no = NVL(g_event_no,c.event_no)
               AND a.hist_seq_no = (SELECT MAX(d.hist_seq_no)
                                       FROM clm_hist d, clmprlds c
                                       WHERE d.co_cd = c.co_cd
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
                    c.event_no,c.loss_cat_cd,c.curr_cd,c.clm_stat_cd, c.org_type
         ORDER BY c.co_cd,c.line_pref,c.subline_pref,c.iss_pref,
                    c.pol_yy,c.pol_seq_no,c.ren_seq_no,c.pol_type,
                    c.clm_yy,c.clm_seq_no;
   BEGIN
      FOR c1_rec IN c1
         LOOP
            IF g_cnt = 0 THEN
               g_cnt          := 1;
               g_file_path    := 'CAT_EVT_FILE_'||g_line_pref||'_'||TO_CHAR(SYSDATE,'MMDDYYYYHH24MISS')||'.csv';
               g_dir_path     := get_directory('DDFDIR');
               out_file       := utl_file.fopen('DDFDIR', g_file_path, 'W');

               UTL_FILE.PUT_LINE(out_file,
                  'ASSURED NAME'  ||','||
                  'VESSEL'||','||
                  'POLICY NUMBER'||','||
                  'INCEPT DATE'||','||
                  'EXPIRY DATE'||','||
                  'CLAIM NUMBER' ||','||
                  'DATE OF LOSS' ||','||
                  'CURRENCY'||','||
                  'POLICY GROSS SUM INSURED'||','||
                  'POLICY RETAINED SUM INSURED'||','||
                  'POLICY QS SUM INSURED'||','||
                  'POLICY SURPLUS SUM INSURED'||','||
                  'POLICY PMMSC SUM INSURED'||','||
                  'POLICY OTHER TREATY SUM INSURED'||','||
                  'POLICY FACUL SUM INSURED'||','||
                  'LOSS RESERVE' ||','||
                  'LOSS PAID'    ||','||
                  'EXPENSE RESERVE'  ||','||
                  'EXPENSE PAID'     ||','||
                  'RECOVERIES'     ||','||
                  'LOSS DESCRIPTION'    ||','||
                  'LOSS DETAIL'  ||','||
                  'STATUS'||','||
                  'FRONTING/REFERRED'||','||
                  'INTM NUMBER'||','||
                  'INTERMEDIARY NAME'||','||
                  'RISK LOC CODE'||','||
                  'RISK LOC DESC'||','||
                  'WITH EX-GRATIA'||','||
                  'ORG TYPE');
               UTL_FILE.NEW_LINE(out_file,1);
            END IF;
               v_item_no := NULL;
               v_item_title := NULL;
               v_item_desc := NULL;
               v_name_to_appear := NULL;
               v_clm_stat_des := NULL;
               v_intm_no := NULL;
               v_intm_name := NULL;
               v_tsi_amt := NULL;
               v_incept_dt := NULL;
               v_expiry_dt := NULL;
               v_recovery := NULL;
               v_vessel := NULL;
               v_risk_loc_cd := NULL;
               v_risk_loc_desc := NULL;
               v_org_type := NULL;

            v_policy := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.pol_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.pol_seq_no,'0999999')))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.ren_seq_no,'09')))||'-'||c1_rec.pol_type;

            v_claim  := c1_rec.line_pref||'-'||c1_rec.subline_pref||'-'||
                        c1_rec.iss_pref||'-'||LTRIM(RTRIM(SUBSTR(TO_CHAR(c1_rec.clm_yy),3,2)))||'-'||
                        LTRIM(RTRIM(TO_CHAR(c1_rec.clm_seq_no,'09999')));

            BEGIN
               SELECT a.name_to_appear
                  INTO v_name_to_appear
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND NVL(a.pol_stat,'0') != '5'
                  AND a.eff_dt =(SELECT MAX(u.eff_dt)
                                    FROM polbasic u
                                    WHERE u.co_cd = a.co_cd
                                    AND u.line_pref = a.line_pref
                                    AND u.subline_pref = a.subline_pref
                                    AND u.iss_pref = a.iss_pref
                                    AND u.pol_yy = a.pol_yy
                                    AND u.pol_seq_no = a.pol_seq_no
                                    AND u.ren_seq_no = a.ren_seq_no
                                    AND NVL(u.pol_stat,'0') != '5'
                                    AND u.name_to_appear IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.name_to_appear
                        INTO v_name_to_appear
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_name_to_appear := NULL;
                  END;
            WHEN TOO_MANY_ROWS THEN
               RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_pol - TOO_MANY_ROWS: name '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT b.intm_no, intm_name
                  INTO v_intm_no, v_intm_name
                  FROM invoice a, invcomm b, intrmdry c
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.bill_yy = b.bill_yy
                  AND a.bill_seq_no = b.bill_seq_no
                  AND b.co_cd = c.co_cd
                  AND b.intm_no = c.intm_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.endt_seq_no = 0;
            EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     IF c1_rec.pol_type = 'I' THEN
                        BEGIN
                           SELECT b.ri_cd, ri_name
                              INTO v_intm_no, v_intm_name
                              FROM polbasic a, inpolicy b, rinsurer c
                              WHERE a.pol_rec_no = b.pol_rec_no
                              AND a.co_cd = c.co_cd
                              AND b.ri_cd = c.ri_cd
                              AND a.co_cd = c1_rec.co_cd
                              AND a.line_pref = c1_rec.line_pref
                              AND a.subline_pref = c1_rec.subline_pref
                              AND a.iss_pref = c1_rec.iss_pref
                              AND a.pol_yy = c1_rec.pol_yy
                              AND a.pol_seq_no = c1_rec.pol_seq_no
                              AND a.ren_seq_no = c1_rec.ren_seq_no
                              AND a.endt_seq_no = 0;
                        EXCEPTION
                           WHEN NO_DATA_FOUND THEN
                              v_intm_no := NULL;
                              v_intm_name := NULL;
                        END;
                  ELSE
                     v_intm_no := NULL;
                     v_intm_name := NULL;
                  END IF;
            WHEN TOO_MANY_ROWS THEN
         -- REV-005749 start
                     SELECT b.intm_no, intm_name
                        INTO v_intm_no, v_intm_name
                        FROM invoice a, invcomm b, intrmdry c
                        WHERE a.co_cd = b.co_cd
                        AND a.line_pref = b.line_pref
                        AND a.bill_yy = b.bill_yy
                        AND a.bill_seq_no = b.bill_seq_no
                        AND b.co_cd = c.co_cd
                        AND b.intm_no = c.intm_no
                        AND a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0
                        AND ROWNUM = 1;
         /*                v_intm_no := NULL;
                        v_intm_name := NULL;
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_pol - TOO_MANY_ROWS: intm '||v_policy);*/
         -- REV-005749 end
                  ----RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_pol - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT MIN(b.item_no)
                  INTO v_item_no
                  FROM clm_loss b
                  WHERE b.co_cd = c1_rec.co_cd
                  AND b.line_pref = c1_rec.line_pref
                  AND b.subline_pref = c1_rec.subline_pref
                  AND b.iss_pref = c1_rec.iss_pref
                  AND b.clm_yy = c1_rec.clm_yy
                  AND b.clm_seq_no = c1_rec.clm_seq_no
                  AND b.hist_seq_no = (SELECT MAX(x.hist_seq_no)
                                          FROM clm_hist x
                                          WHERE b.co_cd = x.co_cd
                                          AND b.line_pref = x.line_pref
                                          AND b.subline_pref = x.subline_pref
                                          AND b.iss_pref = x.iss_pref
                                          AND b.clm_yy = x.clm_yy
                                          AND b.clm_seq_no = x.clm_seq_no
                                          AND x.fla_stat_cd != 'C');
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_item_no := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_pol - TOO_MANY_ROWS: item_no '||v_policy||' '||v_claim);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT a.referring_inst_cd
                  INTO v_referring_inst_cd
                  FROM polbasic a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.eff_dt = (SELECT MAX(z.eff_dt)
                                    FROM polbasic z
                                    WHERE z.co_cd = a.co_cd
                                    AND z.line_pref = a.line_pref
                                    AND z.subline_pref = a.subline_pref
                                    AND z.iss_pref = a.iss_pref
                                    AND z.pol_yy = a.pol_yy
                                    AND z.pol_seq_no = a.pol_seq_no
                                    AND z.ren_seq_no = a.ren_seq_no
                                    AND z.pol_type = a.pol_type
                                    AND NVL(z.pol_stat, 'X') != '5'
                                    AND z.referring_inst_cd IS NOT NULL);
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_referring_inst_cd := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_pol - TOO_MANY_ROWS: referring_cd '||v_policy);
               --RAISE FORM_TRIGGER_FAILURE;
            END;
            BEGIN
               SELECT ri_name
                  INTO v_referring_name
                  FROM rinsurer
                  WHERE co_cd = c1_rec.co_cd
                  AND ri_cd = v_referring_inst_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_referring_name := NULL;
            END;

            BEGIN
               SELECT a.incept_dt
                  INTO v_incept_dt
                  FROM max_incept_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.incept_dt
                        INTO v_incept_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_incept_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_pol - TOO_MANY_ROWS: incept dt '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT a.expiry_dt
                  INTO v_expiry_dt
                  FROM max_expiry_dt a
                  WHERE a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  BEGIN
                     SELECT a.expiry_dt
                        INTO v_expiry_dt
                        FROM polbasic a
                        WHERE a.co_cd = c1_rec.co_cd
                        AND a.line_pref = c1_rec.line_pref
                        AND a.subline_pref = c1_rec.subline_pref
                        AND a.iss_pref = c1_rec.iss_pref
                        AND a.pol_yy = c1_rec.pol_yy
                        AND a.pol_seq_no = c1_rec.pol_seq_no
                        AND a.ren_seq_no = c1_rec.ren_seq_no
                        AND a.endt_seq_no = 0;
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_expiry_dt := NULL;
                  END;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_pol - TOO_MANY_ROWS: expiry dt '||v_policy);
                  --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT clm_stat_des
                  INTO v_clm_stat_des
                  FROM clm_stat
                  WHERE co_cd = c1_rec.co_cd
                  AND clm_stat_cd = c1_rec.clm_stat_cd;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_clm_stat_des := NULL;
               WHEN TOO_MANY_ROWS THEN
                  RAISE_APPLICATION_ERROR(-20001, 'cat_evt_file_cas_pol - TOO_MANY_ROWS: clm stat '||v_policy||' '||v_claim);
               --RAISE FORM_TRIGGER_FAILURE;
            END;

            BEGIN
               SELECT SUM(NVL(a.loss_adv_amt,0))
                  INTO v_recovery
                  FROM clm_loss a, clm_hist b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.subline_pref = b.subline_pref
                  AND a.iss_pref = b.iss_pref
                  AND a.clm_yy = b.clm_yy
                  AND a.clm_seq_no = b.clm_seq_no
                  AND a.hist_seq_no = b.hist_seq_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.clm_yy = c1_rec.clm_yy
                  AND a.clm_seq_no = c1_rec.clm_seq_no
                  AND b.fla_stat_cd != 'C'
                  AND SIGN(a.loss_adv_amt) = -1;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_recovery := NULL;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_prem_amt, v_tsi_amt
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_prem_amt := 0;
                  v_tsi_amt := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_netret_prem, v_netret_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND shr_type = 'N';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_netret_prem := 0;
                  v_netret_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_facul_prem, v_facul_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND shr_type = 'F';
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_facul_prem := 0;
                  v_facul_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_surplus_prem, v_surplus_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type IN (1,2));

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_surplus_prem := 0;
                  v_surplus_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_quota_prem, v_quota_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 3);

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_quota_prem := 0;
                  v_quota_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_pmmsc_prem, v_pmmsc_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type = 10);

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_pmmsc_prem := 0;
                  v_pmmsc_tsi := 0;
            END;

            BEGIN
               SELECT SUM(NVL(b.shr_prem,0)), SUM(NVL(b.shr_tsi,0))
                  INTO v_other_prem, v_other_tsi
                  FROM pol_dist a, policyds b
                  WHERE a.co_cd = b.co_cd
                  AND a.line_pref = b.line_pref
                  AND a.dist_no = b.dist_no
                  AND a.co_cd = c1_rec.co_cd
                  AND a.line_pref = c1_rec.line_pref
                  AND a.subline_pref = c1_rec.subline_pref
                  AND a.iss_pref = c1_rec.iss_pref
                  AND a.pol_yy = c1_rec.pol_yy
                  AND a.pol_seq_no = c1_rec.pol_seq_no
                  AND a.ren_seq_no = c1_rec.ren_seq_no
                  AND a.pol_type = c1_rec.pol_type
                  AND a.dist_stat = '3'
                  AND a.redist_stat in (1,3)
                  AND a.acct_neg_dt IS NULL
                  AND b.shr_type = 'T'
                  AND b.ri_cd IN (SELECT DISTINCT ri_cd
                                       FROM outreaty x
                                       WHERE b.co_cd = x.co_cd
                                       AND b.line_pref = x.line_pref
                                       AND x.ca_trty_type NOT IN (1,2,3,10));

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_other_prem := 0;
                  v_other_tsi := 0;
            END;

            v_x_gratia_cnt := 0;
            BEGIN
               SELECT count(*)
                  INTO v_x_gratia_cnt
                  FROM clm_loss x, clm_hist y
                  WHERE x.co_cd = y.co_cd
                  AND x.line_pref = y.line_pref
                  AND x.subline_pref = y.subline_pref
                  AND x.iss_pref = y.iss_pref
                  AND x.clm_yy = y.clm_yy
                  AND x.clm_seq_no = y.clm_seq_no
                  AND x.hist_seq_no = y.hist_seq_no
                  AND x.co_cd = c1_rec.co_cd
                  AND x.line_pref = c1_rec.line_pref
                  AND x.subline_pref = c1_rec.subline_pref
                  AND x.iss_pref = c1_rec.iss_pref
                  AND x.clm_yy = c1_rec.clm_yy
                  AND x.clm_seq_no = c1_rec.clm_seq_no
                  AND y.fla_stat_cd != 'C'
                  AND (INSTR(UPPER(y.remarks),'GRATIA') <> 0 OR
                                 NVL(x.x_gratia_tag,'N') = 'Y');
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  v_x_gratia_cnt := 0;
            END;
            IF v_x_gratia_cnt = 0 THEN
               v_x_gratia := 'N';
            ELSE
               v_x_gratia := 'Y';
            END IF;
            IF c1_rec.org_type IS NULL THEN 
               SELECT GET_ORG_TYPE_POL(c1_rec.co_cd, c1_rec.line_pref, c1_rec.subline_pref, c1_rec.iss_pref, c1_rec.pol_yy, 
                                c1_rec.pol_seq_no, c1_rec.ren_seq_no, c1_rec.pol_type)
                  INTO v_org_type
                  FROM dual;
            ELSE
               v_org_type := c1_rec.org_type;
            END IF;            

            UTL_FILE.PUT_LINE(out_file,
               '"'||REPLACE(v_name_to_appear,'"','')||'"'||','||  --REV-12885
               v_vessel||','||
               v_policy||','||
               TO_CHAR(v_incept_dt,'MM/DD/YYYY')||','||
               TO_CHAR(v_expiry_dt,'MM/DD/YYYY')||','||
               v_claim           ||','||
               TO_CHAR(c1_rec.loss_dt,'MM/DD/YYYY')    ||','||
               c1_rec.curr_cd||','||
               TO_CHAR(NVL(v_tsi_amt,0) ,'99999999999990.00')||','|| --REV-12885
               TO_CHAR(NVL(v_netret_tsi,0) ,'99999999999990.00')||','|| --REV-12885
               TO_CHAR(NVL(v_quota_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_surplus_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_pmmsc_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_other_tsi,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(v_facul_tsi,0) ,'99999999999990.00')||','||  --REV-12885
               TO_CHAR(NVL(c1_rec.clm_shr_ramt,0),'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.clm_shr_pd,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.exp_shr_ramt,0) ,'99999999990.00')||','||
               TO_CHAR(NVL(c1_rec.exp_shr_pd,0)  ,'99999999990.00')||','||
               TO_CHAR(NVL(v_recovery,0)  ,'99999999990.00')||','||
               '"'||c1_rec.loss_desc ||'"'||','||
               '"'||c1_rec.loss_det  ||'"'||','||
               '"'||v_clm_stat_des  ||'"'||','||
               '"'||v_referring_name  ||'"'||','||
               TO_CHAR(v_intm_no,'09999')||','||
               '"'||v_intm_name||'"'||','||
               '"'||v_risk_loc_cd||'"'||','||
               '"'||v_risk_loc_desc||'"'||','||
               v_x_gratia||','||
               v_org_type);
         END LOOP;
         --
      IF UTL_FILE.is_open(out_file) THEN
         UTL_FILE.fclose(out_file);
      END IF;
   END cat_evt_file_cas_pol;
-------------------------------------------------------------------------------------------------- cat_evt_file_cas_pol

-------------------------------------------------------------------------------------------------- main
   PROCEDURE main(ip_co_cd          IN     NUMBER,
                  ip_line_pref      IN     VARCHAR2,
                  ip_iss_pref       IN     VARCHAR2,
                  ip_event_no       IN     NUMBER,
                  ip_extract_type   IN     VARCHAR2,
                  op_file_path         OUT VARCHAR2) AS
   BEGIN
      g_co_cd        := ip_co_cd;
      g_line_pref    := ip_line_pref;
      g_iss_pref     := ip_iss_pref;
      g_event_no     := ip_event_no;
      g_cnt          := 0;

      IF ip_line_pref = 'MC' THEN
         cat_evt_file_mc;
      ELSIF ip_line_pref = 'FI' THEN
         IF ip_extract_type = 'ITM' THEN
            cat_evt_file_fi_itm;
         ELSE
            cat_evt_file_fi_pol;
         END IF;
--      ELSIF ip_line_pref IN ('GA','EN') THEN
     ELSE
         IF ip_extract_type = 'ITM' THEN
            cat_evt_file_cas_itm;
         ELSE
            cat_evt_file_cas_pol;
         END IF;
      END IF;
      IF g_file_path IS NOT NULL THEN
         op_file_path := g_file_path;
      ELSE
         APEX_API.ADD_ERROR('No data found using the given parameters.');
      END IF;
   END main;
-------------------------------------------------------------------------------------------------- main
END nidl3714_pkg;