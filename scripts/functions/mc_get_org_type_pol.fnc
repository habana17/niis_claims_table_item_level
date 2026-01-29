CREATE OR REPLACE FUNCTION DBADM_MC.GET_ORG_TYPE_POL (P_CO_CD   NUMBER,
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

        RETURN(v_org_type);
    END;
/
