-- ========================================
-- 정규화된 테이블 생성 및 데이터 마이그레이션 스크립트
-- ========================================

-- 1. 정당(Political Party) 정규화 테이블
CREATE TABLE parties_normalized (
    party_id INT IDENTITY(1,1) PRIMARY KEY,
    party_name NVARCHAR(100) NOT NULL UNIQUE,
    party_code NVARCHAR(50),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- 2. 지역(Region) 정규화 테이블
CREATE TABLE regions_normalized (
    region_id INT IDENTITY(1,1) PRIMARY KEY,
    region_name NVARCHAR(100) NOT NULL UNIQUE,
    region_code NVARCHAR(50),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- 3. 위원회(Committee) 정규화 테이블
CREATE TABLE committees_normalized (
    committee_id INT IDENTITY(1,1) PRIMARY KEY,
    committee_name NVARCHAR(200) NOT NULL UNIQUE,
    committee_code NVARCHAR(50),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- 4. 의원(Members) 정규화 테이블
CREATE TABLE assembly_members_normalized (
    member_id INT IDENTITY(1,1) PRIMARY KEY,
    mona_cd NVARCHAR(50) UNIQUE,
    naas_cd NVARCHAR(50),
    
    -- 기본 정보
    hg_nm NVARCHAR(100) NOT NULL,  -- 한글명
    hj_nm NVARCHAR(100),           -- 한자명
    eng_nm NVARCHAR(200),          -- 영문명
    
    -- 개인 정보
    birth_date NVARCHAR(20),
    birth_type NVARCHAR(50),
    age NVARCHAR(10),
    gender NVARCHAR(20),
    
    -- 연락처 정보
    tel_no NVARCHAR(50),
    email NVARCHAR(100),
    homepage NVARCHAR(200),
    office_addr NVARCHAR(300),
    
    -- 직원 정보
    staff NVARCHAR(500),
    secretary NVARCHAR(200),
    secretary2 NVARCHAR(500),
    
    -- 기타
    job_title NVARCHAR(200),
    education NTEXT,
    career NTEXT,
    profile_pic NVARCHAR(500),
    mem_title NTEXT,
    
    created_at DATETIME2 DEFAULT GETDATE()
);

-- 5. 의원 임기 정보 정규화 테이블
CREATE TABLE assembly_member_terms_normalized (
    term_id INT IDENTITY(1,1) PRIMARY KEY,
    member_id INT NOT NULL,
    party_id INT,
    region_id INT,
    committee_id INT,
    
    daesu INT NOT NULL,              -- 국회 회기
    election_type NVARCHAR(100),     -- 선거구분
    reelection_count NVARCHAR(100),  -- 당선횟수
    units NVARCHAR(100),             -- 소속
    
    term_start_date DATE,
    term_end_date DATE,
    is_current BIT DEFAULT 0,
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (member_id) REFERENCES assembly_members_normalized(member_id),
    FOREIGN KEY (party_id) REFERENCES parties_normalized(party_id),
    FOREIGN KEY (region_id) REFERENCES regions_normalized(region_id),
    FOREIGN KEY (committee_id) REFERENCES committees_normalized(committee_id)
);

-- 6. 법안(Bills) 정규화 테이블
CREATE TABLE assembly_bills_normalized (
    bill_id INT IDENTITY(1,1) PRIMARY KEY,
    original_bill_id NVARCHAR(50) UNIQUE,
    bill_no NVARCHAR(50),
    bill_name NVARCHAR(500) NOT NULL,
    
    committee_id INT,
    age_number INT,
    
    -- 제안 정보
    proposer NVARCHAR(500),
    member_list NVARCHAR(1000),
    publ_proposer NVARCHAR(MAX),
    rst_proposer NVARCHAR(200),
    
    -- 날짜 정보
    propose_dt DATE,
    law_proc_dt DATE,
    law_present_dt DATE,
    law_submit_dt DATE,
    cmt_proc_dt DATE,
    cmt_present_dt DATE,
    committee_dt DATE,
    proc_dt DATE,
    
    -- 처리 결과
    proc_result NVARCHAR(100),
    cmt_proc_result_cd NVARCHAR(100),
    law_proc_result_cd NVARCHAR(100),
    
    -- 기타
    detail_link NVARCHAR(1000),
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (committee_id) REFERENCES committees_normalized(committee_id)
);

-- 7. 법안 제안자 관계 테이블 (다대다 관계)
CREATE TABLE bill_proposers_normalized (
    id INT IDENTITY(1,1) PRIMARY KEY,
    bill_id INT NOT NULL,
    member_id INT NOT NULL,
    proposer_type NVARCHAR(50), -- 'main', 'co', 'public' 등
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (bill_id) REFERENCES assembly_bills_normalized(bill_id),
    FOREIGN KEY (member_id) REFERENCES assembly_members_normalized(member_id),
    
    UNIQUE(bill_id, member_id, proposer_type)
);

-- 8. 투표 세션 정규화 테이블
CREATE TABLE vote_sessions_normalized (
    session_id INT IDENTITY(1,1) PRIMARY KEY,
    session_cd INT,
    currents_cd INT,
    age INT,
    dept_cd NVARCHAR(50),
    vote_date NVARCHAR(50),
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    UNIQUE(session_cd, currents_cd, age, dept_cd, vote_date)
);

-- 9. 투표 기록 정규화 테이블
CREATE TABLE assembly_votes_normalized (
    vote_id INT IDENTITY(1,1) PRIMARY KEY,
    session_id INT NOT NULL,
    bill_id INT,
    member_id INT NOT NULL,
    
    vote_result NVARCHAR(50), -- 찬성, 반대, 기권, 불참 등
    disp_order INT,
    
    -- URL 정보
    bill_url NVARCHAR(1000),
    bill_name_url NVARCHAR(1000),
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (session_id) REFERENCES vote_sessions_normalized(session_id),
    FOREIGN KEY (bill_id) REFERENCES assembly_bills_normalized(bill_id),
    FOREIGN KEY (member_id) REFERENCES assembly_members_normalized(member_id)
);

-- ========================================
-- 데이터 마이그레이션
-- ========================================

-- 1. 정당 데이터 마이그레이션
INSERT INTO parties_normalized (party_name, party_code)
SELECT DISTINCT 
    COALESCE(POLY_NM, '무소속') as party_name,
    POLY_CD as party_code
FROM (
    SELECT POLY_NM, POLY_CD FROM assembly_members_history WHERE POLY_NM IS NOT NULL
    UNION
    SELECT POLY_NM, NULL FROM assembly_members_profile WHERE POLY_NM IS NOT NULL
    UNION
    SELECT POLY_NM, POLY_CD FROM assembly_plenary_session_vote WHERE POLY_NM IS NOT NULL
    UNION
    SELECT PLPT_NM, NULL FROM assembly_members_integrated WHERE PLPT_NM IS NOT NULL
) unified_parties
WHERE party_name IS NOT NULL AND party_name != '';

-- 2. 지역 데이터 마이그레이션  
INSERT INTO regions_normalized (region_name, region_code)
SELECT DISTINCT 
    region_name,
    region_code
FROM (
    SELECT ORIG_NM as region_name, NULL as region_code FROM assembly_members_history WHERE ORIG_NM IS NOT NULL
    UNION
    SELECT ORIG_NM as region_name, NULL as region_code FROM assembly_members_profile WHERE ORIG_NM IS NOT NULL
    UNION
    SELECT ORIG_NM as region_name, ORIG_CD as region_code FROM assembly_plenary_session_vote WHERE ORIG_NM IS NOT NULL
    UNION
    SELECT ELECD_NM as region_name, NULL as region_code FROM assembly_members_integrated WHERE ELECD_NM IS NOT NULL
) unified_regions
WHERE region_name IS NOT NULL AND region_name != '';

-- 3. 위원회 데이터 마이그레이션
INSERT INTO committees_normalized (committee_name, committee_code)
SELECT DISTINCT 
    committee_name,
    committee_code
FROM (
    SELECT CMIT_NM as committee_name, NULL as committee_code FROM assembly_members_history WHERE CMIT_NM IS NOT NULL
    UNION
    SELECT CMIT_NM as committee_name, NULL as committee_code FROM assembly_members_profile WHERE CMIT_NM IS NOT NULL
    UNION
    SELECT COMMITTEE as committee_name, COMMITTEE_ID as committee_code FROM assembly_bills WHERE COMMITTEE IS NOT NULL
    UNION
    SELECT CURR_COMMITTEE as committee_name, CURR_COMMITTEE_ID as committee_code FROM assembly_plenary_session_vote WHERE CURR_COMMITTEE IS NOT NULL
    UNION
    SELECT CMIT_NM as committee_name, NULL as committee_code FROM assembly_members_integrated WHERE CMIT_NM IS NOT NULL
) unified_committees
WHERE committee_name IS NOT NULL AND committee_name != '';

-- 4. 의원 데이터 마이그레이션 (중복 제거 및 통합)
WITH unified_members AS (
    SELECT 
        MONA_CD,
        NULL as naas_cd,
        HG_NM,
        HJ_NM, 
        ENG_NM,
        BTH_DATE as birth_date,
        BTH_GBN_NM as birth_type,
        AGED as age,
        SEX_GBN_NM as gender,
        TEL_NO as tel_no,
        E_MAIL as email,
        HOMEPAGE as homepage,
        ASSEM_ADDR as office_addr,
        STAFF as staff,
        SECRETARY as secretary,
        SECRETARY2 as secretary2,
        JOB_RES_NM as job_title,
        NULL as education,
        NULL as career,
        NULL as profile_pic,
        MEM_TITLE as mem_title
    FROM assembly_members_history
    WHERE MONA_CD IS NOT NULL
    
    UNION ALL
    
    SELECT 
        MONA_CD,
        NULL as naas_cd,
        HG_NM,
        HJ_NM,
        ENG_NM,
        BTH_DATE as birth_date,
        BTH_GBN_NM as birth_type,
        NULL as age,
        SEX_GBN_NM as gender,
        TEL_NO as tel_no,
        E_MAIL as email,
        HOMEPAGE as homepage,
        ASSEM_ADDR as office_addr,
        STAFF as staff,
        SECRETARY as secretary,
        SECRETARY2 as secretary2,
        JOB_RES_NM as job_title,
        NULL as education,
        NULL as career,
        NULL as profile_pic,
        MEM_TITLE as mem_title
    FROM assembly_members_profile
    WHERE MONA_CD IS NOT NULL
    
    UNION ALL
    
    SELECT 
        NULL as MONA_CD,
        NAAS_CD as naas_cd,
        NAAS_NM as HG_NM,
        NAAS_CH_NM as HJ_NM,
        NAAS_EN_NM as ENG_NM,
        BIRDY_DT as birth_date,
        BIRDY_DIV_CD as birth_type,
        NULL as age,
        NTR_DIV as gender,
        NAAS_TEL_NO as tel_no,
        NAAS_EMAIL_ADDR as email,
        NAAS_HP_URL as homepage,
        NULL as office_addr,
        AIDE_NM as staff,
        CHF_SCRT_NM as secretary,
        SCRT_NM as secretary2,
        DTY_NM as job_title,
        NULL as education,
        BRF_HST as career,
        NAAS_PIC as profile_pic,
        NULL as mem_title
    FROM assembly_members_integrated
    WHERE NAAS_CD IS NOT NULL
),
deduplicated_members AS (
    SELECT 
        COALESCE(MONA_CD, naas_cd) as unique_id,
        MONA_CD,
        naas_cd,
        HG_NM,
        HJ_NM,
        ENG_NM,
        birth_date,
        birth_type,
        age,
        gender,
        tel_no,
        email,
        homepage,
        office_addr,
        staff,
        secretary,
        secretary2,
        job_title,
        education,
        career,
        profile_pic,
        mem_title,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(MONA_CD, naas_cd) ORDER BY CASE WHEN MONA_CD IS NOT NULL THEN 1 ELSE 2 END) as rn
    FROM unified_members
    WHERE COALESCE(MONA_CD, naas_cd) IS NOT NULL
)
INSERT INTO assembly_members_normalized (
    mona_cd, naas_cd, hg_nm, hj_nm, eng_nm, birth_date, birth_type, age, gender,
    tel_no, email, homepage, office_addr, staff, secretary, secretary2, job_title,
    education, career, profile_pic, mem_title
)
SELECT 
    MONA_CD, naas_cd, HG_NM, HJ_NM, ENG_NM, birth_date, birth_type, age, gender,
    tel_no, email, homepage, office_addr, staff, secretary, secretary2, job_title,
    education, career, profile_pic, mem_title
FROM deduplicated_members
WHERE rn = 1;

-- 5. 의원 임기 정보 마이그레이션
INSERT INTO assembly_member_terms_normalized (
    member_id, party_id, region_id, committee_id, daesu, election_type, 
    reelection_count, units, is_current
)
SELECT DISTINCT
    m.member_id,
    p.party_id,
    r.region_id,
    c.committee_id,
    h.DAESU,
    h.ELECT_GBN_NM,
    h.REELE_GBN_NM,
    h.UNITS,
    CASE WHEN h.DAESU >= 21 THEN 1 ELSE 0 END as is_current
FROM assembly_members_history h
JOIN assembly_members_normalized m ON h.MONA_CD = m.mona_cd
LEFT JOIN parties_normalized p ON h.POLY_NM = p.party_name
LEFT JOIN regions_normalized r ON h.ORIG_NM = r.region_name
LEFT JOIN committees_normalized c ON h.CMIT_NM = c.committee_name
WHERE h.DAESU IS NOT NULL;

-- 6. 법안 데이터 마이그레이션
INSERT INTO assembly_bills_normalized (
    original_bill_id, bill_no, bill_name, committee_id, age_number,
    proposer, member_list, publ_proposer, rst_proposer,
    propose_dt, law_proc_dt, law_present_dt, law_submit_dt,
    cmt_proc_dt, cmt_present_dt, committee_dt, proc_dt,
    proc_result, cmt_proc_result_cd, law_proc_result_cd, detail_link
)
SELECT 
    b.BILL_ID,
    b.BILL_NO,
    b.BILL_NAME,
    c.committee_id,
    b.age_number,
    b.PROPOSER,
    b.MEMBER_LIST,
    b.PUBL_PROPOSER,
    b.RST_PROPOSER,
    b.PROPOSE_DT,
    b.LAW_PROC_DT,
    b.LAW_PRESENT_DT,
    b.LAW_SUBMIT_DT,
    b.CMT_PROC_DT,
    b.CMT_PRESENT_DT,
    b.COMMITTEE_DT,
    b.PROC_DT,
    b.PROC_RESULT,
    b.CMT_PROC_RESULT_CD,
    b.LAW_PROC_RESULT_CD,
    b.DETAIL_LINK
FROM assembly_bills b
LEFT JOIN committees_normalized c ON b.COMMITTEE = c.committee_name;

-- 7. 투표 세션 데이터 마이그레이션
INSERT INTO vote_sessions_normalized (session_cd, currents_cd, age, dept_cd, vote_date)
SELECT DISTINCT 
    SESSION_CD, CURRENTS_CD, AGE, DEPT_CD, VOTE_DATE
FROM assembly_plenary_session_vote
WHERE SESSION_CD IS NOT NULL;

-- 8. 투표 기록 데이터 마이그레이션
INSERT INTO assembly_votes_normalized (
    session_id, bill_id, member_id, vote_result, disp_order, bill_url, bill_name_url
)
SELECT 
    vs.session_id,
    bn.bill_id,
    mn.member_id,
    v.RESULT_VOTE_MOD,
    v.DISP_ORDER,
    v.BILL_URL,
    v.BILL_NAME_URL
FROM assembly_plenary_session_vote v
JOIN vote_sessions_normalized vs ON v.SESSION_CD = vs.session_cd 
    AND v.CURRENTS_CD = vs.currents_cd 
    AND v.AGE = vs.age
    AND COALESCE(v.DEPT_CD, '') = COALESCE(vs.dept_cd, '')
    AND v.VOTE_DATE = vs.vote_date
LEFT JOIN assembly_bills_normalized bn ON v.BILL_ID = bn.original_bill_id
LEFT JOIN assembly_members_normalized mn ON v.MONA_CD = mn.mona_cd
WHERE mn.member_id IS NOT NULL;

-- ========================================
-- 인덱스 생성 (성능 최적화)
-- ========================================

-- 의원 테이블 인덱스
CREATE INDEX IX_assembly_members_normalized_mona_cd ON assembly_members_normalized(mona_cd);
CREATE INDEX IX_assembly_members_normalized_naas_cd ON assembly_members_normalized(naas_cd);
CREATE INDEX IX_assembly_members_normalized_hg_nm ON assembly_members_normalized(hg_nm);

-- 의원 임기 테이블 인덱스
CREATE INDEX IX_assembly_member_terms_normalized_member_id ON assembly_member_terms_normalized(member_id);
CREATE INDEX IX_assembly_member_terms_normalized_daesu ON assembly_member_terms_normalized(daesu);
CREATE INDEX IX_assembly_member_terms_normalized_is_current ON assembly_member_terms_normalized(is_current);

-- 법안 테이블 인덱스
CREATE INDEX IX_assembly_bills_normalized_original_bill_id ON assembly_bills_normalized(original_bill_id);
CREATE INDEX IX_assembly_bills_normalized_bill_no ON assembly_bills_normalized(bill_no);
CREATE INDEX IX_assembly_bills_normalized_age_number ON assembly_bills_normalized(age_number);

-- 투표 테이블 인덱스
CREATE INDEX IX_assembly_votes_normalized_session_id ON assembly_votes_normalized(session_id);
CREATE INDEX IX_assembly_votes_normalized_bill_id ON assembly_votes_normalized(bill_id);
CREATE INDEX IX_assembly_votes_normalized_member_id ON assembly_votes_normalized(member_id);

-- 복합 인덱스
CREATE INDEX IX_assembly_votes_normalized_session_member ON assembly_votes_normalized(session_id, member_id);
CREATE INDEX IX_assembly_member_terms_normalized_member_daesu ON assembly_member_terms_normalized(member_id, daesu);

-- ========================================
-- 통계 정보 업데이트
-- ========================================

UPDATE STATISTICS parties_normalized;
UPDATE STATISTICS regions_normalized;
UPDATE STATISTICS committees_normalized;
UPDATE STATISTICS assembly_members_normalized;
UPDATE STATISTICS assembly_member_terms_normalized;
UPDATE STATISTICS assembly_bills_normalized;
UPDATE STATISTICS bill_proposers_normalized;
UPDATE STATISTICS vote_sessions_normalized;
UPDATE STATISTICS assembly_votes_normalized;

PRINT '정규화된 테이블 생성 및 데이터 마이그레이션이 완료되었습니다.';