CREATE OR REPLACE FUNCTION DBADM_FI.GET_ORG_TYPE_POL (P_CO_CD   NUMBER,
                                             P_LINE_PREF  VARCHAR2,
                                             P_SUBLINE_PREF  VARCHAR2,
                                             P_ISS_PREF   VARCHAR2,
                                             P_POL_YY  NUMBER,
                                             P_POL_SEQ_NO   NUMBER,
                                             P_REN_SEQ_NO   NUMBER,
                                             P_POL_TYPE   VARCHAR2)
    RETURN VARCHAR2
    IS
        v_org_type     VARCHAR2(1 CHAR);

    BEGIN
        IF p_line_pref = 'AC' THEN
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM dbadm_ac.polbasic
                 WHERE co_cd = p_co_cd
                 AND line_pref = p_line_pref
                 AND subline_pref = p_subline_pref
                 AND iss_pref = p_iss_pref
                 AND pol_yy = p_pol_yy
                 AND pol_seq_no = p_pol_seq_no
                 AND ren_seq_no = p_ren_seq_no
                 AND endt_seq_no = 0;
           END;
        ELSIF p_line_pref = 'BD' THEN         
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM dbadm_bd.polbasic
                 WHERE co_cd = p_co_cd
                 AND line_pref = p_line_pref
                 AND subline_pref = p_subline_pref
                 AND iss_pref = p_iss_pref
                 AND pol_yy = p_pol_yy
                 AND pol_seq_no = p_pol_seq_no
                 AND ren_seq_no = p_ren_seq_no
                 AND endt_seq_no = 0;
           END;
        ELSIF p_line_pref IN ('EN','EC','GA','LV') THEN         
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM dbadm_cas.polbasic
                 WHERE co_cd = p_co_cd
                 AND line_pref = p_line_pref
                 AND subline_pref = p_subline_pref
                 AND iss_pref = p_iss_pref
                 AND pol_yy = p_pol_yy
                 AND pol_seq_no = p_pol_seq_no
                 AND ren_seq_no = p_ren_seq_no
                 AND endt_seq_no = 0;
           END;
        ELSIF p_line_pref = 'FI' THEN         
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM dbadm_fi.polbasic
                 WHERE co_cd = p_co_cd
                 AND line_pref = p_line_pref
                 AND subline_pref = p_subline_pref
                 AND iss_pref = p_iss_pref
                 AND pol_yy = p_pol_yy
                 AND pol_seq_no = p_pol_seq_no
                 AND ren_seq_no = p_ren_seq_no
                 AND endt_seq_no = 0;
           END;
        ELSIF p_line_pref = 'MC' THEN         
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM dbadm_mc.polbasic
                 WHERE co_cd = p_co_cd
                 AND line_pref = p_line_pref
                 AND subline_pref = p_subline_pref
                 AND iss_pref = p_iss_pref
                 AND pol_yy = p_pol_yy
                 AND pol_seq_no = p_pol_seq_no
                 AND ren_seq_no = p_ren_seq_no
                 AND endt_seq_no = 0;
           END;
        ELSIF p_line_pref IN ('AV','MH') THEN         
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM dbadm_mh.polbasic
                 WHERE co_cd = p_co_cd
                 AND line_pref = p_line_pref
                 AND subline_pref = p_subline_pref
                 AND iss_pref = p_iss_pref
                 AND pol_yy = p_pol_yy
                 AND pol_seq_no = p_pol_seq_no
                 AND ren_seq_no = p_ren_seq_no
                 AND endt_seq_no = 0;
           END;
        ELSIF p_line_pref = 'MN' THEN         
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM dbadm_mn.polbasic
                 WHERE co_cd = p_co_cd
                 AND line_pref = p_line_pref
                 AND subline_pref = p_subline_pref
                 AND iss_pref = p_iss_pref
                 AND pol_yy = p_pol_yy
                 AND pol_seq_no = p_pol_seq_no
                 AND ren_seq_no = p_ren_seq_no
                 AND endt_seq_no = 0;
           END;
        ELSIF p_line_pref = 'PP' THEN         
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM dbadm.pp_basic
                 WHERE co_cd = p_co_cd
                 AND pline_pref = p_line_pref
                 AND psubline_pref = p_subline_pref
                 AND piss_pref = p_iss_pref
                 AND ppol_yy = p_pol_yy
                 AND ppol_seq_no = p_pol_seq_no
                 AND pren_seq_no = p_ren_seq_no
                 AND pendt_seq_no = 0;
           END;
        ELSE       
           BEGIN    
              SELECT org_type
                 INTO v_org_type
                 FROM polbasic
                 WHERE co_cd = p_co_cd
                 AND line_pref = p_line_pref
                 AND subline_pref = p_subline_pref
                 AND iss_pref = p_iss_pref
                 AND pol_yy = p_pol_yy
                 AND pol_seq_no = p_pol_seq_no
                 AND ren_seq_no = p_ren_seq_no
                 AND endt_seq_no = 0;
           END;
        END IF;


        RETURN(v_org_type);
    END;
/
