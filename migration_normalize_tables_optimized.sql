-- ========================================
-- 최적화된 정규화 테이블 생성 및 데이터 마이그레이션 스크립트
-- 과도한 JOIN 방지를 위한 실용적 정규화 접근
-- ========================================

-- 1. 의원(Members) 통합 정규화 테이블 (핵심 엔티티)
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

-- 2. 의원 임기 정보 정규화 테이블 (정당/지역/위원회 정보 직접 포함)
CREATE TABLE assembly_member_terms_normalized (
    term_id INT IDENTITY(1,1) PRIMARY KEY,
    member_id INT NOT NULL,
    
    -- 임기 기본 정보
    daesu INT NOT NULL,              -- 국회 회기
    election_type NVARCHAR(100),     -- 선거구분
    reelection_count NVARCHAR(100),  -- 당선횟수
    units NVARCHAR(100),             -- 소속
    
    -- 정당 정보 (정규화하지 않고 직접 저장 - 자주 조회되고 변경 빈도 낮음)
    party_name NVARCHAR(100),
    party_code NVARCHAR(50),
    
    -- 지역 정보 (정규화하지 않고 직접 저장)
    region_name NVARCHAR(100),
    region_code NVARCHAR(50),
    
    -- 위원회 정보 (정규화하지 않고 직접 저장)
    committee_name NVARCHAR(200),
    committee_code NVARCHAR(50),
    
    -- 임기 날짜
    term_start_date DATE,
    term_end_date DATE,
    is_current BIT DEFAULT 0,
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (member_id) REFERENCES assembly_members_normalized(member_id),
    
    -- 복합 인덱스로 성능 최적화
    INDEX IX_member_daesu (member_id, daesu),
    INDEX IX_party_daesu (party_name, daesu),
    INDEX IX_region_daesu (region_name, daesu)
);

-- 3. 법안(Bills) 정규화 테이블 (위원회 정보 직접 포함)
CREATE TABLE assembly_bills_normalized (
    bill_id INT IDENTITY(1,1) PRIMARY KEY,
    original_bill_id NVARCHAR(50) UNIQUE,
    bill_no NVARCHAR(50),
    bill_name NVARCHAR(500) NOT NULL,
    
    -- 위원회 정보 (정규화하지 않고 직접 저장)
    committee_name NVARCHAR(200),
    committee_code NVARCHAR(50),
    
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
    
    -- 성능 최적화 인덱스
    INDEX IX_bill_no (bill_no),
    INDEX IX_committee_age (committee_name, age_number),
    INDEX IX_propose_date (propose_dt)
);

-- 4. 법안 제안자 관계 테이블 (다대다 관계 - 필수)
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

-- 5. 투표 기록 정규화 테이블 (세션 정보 직접 포함)
CREATE TABLE assembly_votes_normalized (
    vote_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- 세션 정보 (정규화하지 않고 직접 저장)
    session_cd INT,
    currents_cd INT,
    age INT,
    dept_cd NVARCHAR(50),
    vote_date NVARCHAR(50),
    
    -- 관계 정보
    bill_id INT,
    member_id INT NOT NULL,
    
    -- 투표 결과
    vote_result NVARCHAR(50), -- 찬성, 반대, 기권, 불참 등
    disp_order INT,
    
    -- URL 정보
    bill_url NVARCHAR(1000),
    bill_name_url NVARCHAR(1000),
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (bill_id) REFERENCES assembly_bills_normalized(bill_id),
    FOREIGN KEY (member_id) REFERENCES assembly_members_normalized(member_id),
    
    -- 성능 최적화 인덱스
    INDEX IX_session_member (session_cd, member_id),
    INDEX IX_bill_vote (bill_id, vote_result),
    INDEX IX_member_vote_date (member_id, vote_date)
);

-- ========================================
-- 참조용 뷰 테이블 (자주 사용되는 JOIN 결과 미리 계산)
-- ========================================

-- 6. 현재 의원 정보 뷰 (자주 조회되는 정보)
CREATE VIEW current_members_view AS
SELECT 
    m.member_id,
    m.mona_cd,
    m.hg_nm,
    m.hj_nm,
    m.eng_nm,
    m.birth_date,
    m.gender,
    m.tel_no,
    m.email,
    t.party_name,
    t.region_name,
    t.committee_name,
    t.daesu,
    t.election_type
FROM assembly_members_normalized m
JOIN assembly_member_terms_normalized t ON m.member_id = t.member_id
WHERE t.is_current = 1;

-- 7. 법안 상세 정보 뷰
CREATE VIEW bills_detail_view AS
SELECT 
    b.bill_id,
    b.original_bill_id,
    b.bill_no,
    b.bill_name,
    b.committee_name,
    b.age_number,
    b.propose_dt,
    b.proc_result,
    COUNT(bp.member_id) as proposer_count,
    STRING_AGG(m.hg_nm, ', ') as proposer_names
FROM assembly_bills_normalized b
LEFT JOIN bill_proposers_normalized bp ON b.bill_id = bp.bill_id
LEFT JOIN assembly_members_normalized m ON bp.member_id = m.member_id
GROUP BY b.bill_id, b.original_bill_id, b.bill_no, b.bill_name, 
         b.committee_name, b.age_number, b.propose_dt, b.proc_result;

-- ========================================
-- 데이터 마이그레이션
-- ========================================

-- 1. 의원 데이터 마이그레이션 (중복 제거 및 통합)
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

-- 2. 의원 임기 정보 마이그레이션 (정당/지역/위원회 정보 포함)
INSERT INTO assembly_member_terms_normalized (
    member_id, daesu, election_type, reelection_count, units,
    party_name, party_code, region_name, region_code, 
    committee_name, committee_code, is_current
)
SELECT DISTINCT
    m.member_id,
    h.DAESU,
    h.ELECT_GBN_NM,
    h.REELE_GBN_NM,
    h.UNITS,
    COALESCE(h.POLY_NM, '무소속') as party_name,
    NULL as party_code,
    h.ORIG_NM as region_name,
    NULL as region_code,
    h.CMIT_NM as committee_name,
    NULL as committee_code,
    CASE WHEN h.DAESU >= 21 THEN 1 ELSE 0 END as is_current
FROM assembly_members_history h
JOIN assembly_members_normalized m ON h.MONA_CD = m.mona_cd
WHERE h.DAESU IS NOT NULL;

-- 3. 법안 데이터 마이그레이션 (위원회 정보 포함)
INSERT INTO assembly_bills_normalized (
    original_bill_id, bill_no, bill_name, committee_name, committee_code, age_number,
    proposer, member_list, publ_proposer, rst_proposer,
    propose_dt, law_proc_dt, law_present_dt, law_submit_dt,
    cmt_proc_dt, cmt_present_dt, committee_dt, proc_dt,
    proc_result, cmt_proc_result_cd, law_proc_result_cd, detail_link
)
SELECT 
    b.BILL_ID,
    b.BILL_NO,
    b.BILL_NAME,
    b.COMMITTEE as committee_name,
    b.COMMITTEE_ID as committee_code,
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
FROM assembly_bills b;

-- 4. 투표 기록 데이터 마이그레이션 (세션 정보 포함)
INSERT INTO assembly_votes_normalized (
    session_cd, currents_cd, age, dept_cd, vote_date,
    bill_id, member_id, vote_result, disp_order, bill_url, bill_name_url
)
SELECT 
    v.SESSION_CD,
    v.CURRENTS_CD,
    v.AGE,
    v.DEPT_CD,
    v.VOTE_DATE,
    bn.bill_id,
    mn.member_id,
    v.RESULT_VOTE_MOD,
    v.DISP_ORDER,
    v.BILL_URL,
    v.BILL_NAME_URL
FROM assembly_plenary_session_vote v
LEFT JOIN assembly_bills_normalized bn ON v.BILL_ID = bn.original_bill_id
LEFT JOIN assembly_members_normalized mn ON v.MONA_CD = mn.mona_cd
WHERE mn.member_id IS NOT NULL;

-- ========================================
-- 추가 성능 최적화 인덱스
-- ========================================

-- 의원 테이블 인덱스
CREATE INDEX IX_assembly_members_normalized_mona_cd ON assembly_members_normalized(mona_cd);
CREATE INDEX IX_assembly_members_normalized_naas_cd ON assembly_members_normalized(naas_cd);
CREATE INDEX IX_assembly_members_normalized_hg_nm ON assembly_members_normalized(hg_nm);

-- 복합 인덱스 (자주 함께 조회되는 컬럼들)
CREATE INDEX IX_member_terms_party_region ON assembly_member_terms_normalized(party_name, region_name, daesu);
CREATE INDEX IX_bills_committee_age ON assembly_bills_normalized(committee_name, age_number, propose_dt);
CREATE INDEX IX_votes_session_date ON assembly_votes_normalized(session_cd, vote_date, vote_result);

-- ========================================
-- 통계 정보 업데이트
-- ========================================

UPDATE STATISTICS assembly_members_normalized;
UPDATE STATISTICS assembly_member_terms_normalized;
UPDATE STATISTICS assembly_bills_normalized;
UPDATE STATISTICS bill_proposers_normalized;
UPDATE STATISTICS assembly_votes_normalized;

PRINT '최적화된 정규화 테이블 생성 및 데이터 마이그레이션이 완료되었습니다.';
PRINT '- 과도한 JOIN 방지를 위해 자주 조회되는 정보는 비정규화 상태로 유지';
PRINT '- 핵심 관계만 정규화하여 성능과 데이터 무결성의 균형 확보';
PRINT '- 자주 사용되는 쿼리를 위한 뷰와 인덱스 최적화 완료';