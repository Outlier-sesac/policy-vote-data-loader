-- ========================================
-- 한국 국회 데이터베이스 정규화 테이블 생성 및 데이터 마이그레이션 스크립트
-- 명확하고 이해하기 쉬운 네이밍 컨벤션 적용
-- ========================================

-- 1. 국회의원 기본정보 테이블 (National Assembly Members)
CREATE TABLE national_assembly_members (
    member_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- 고유 식별자 (Legacy System Codes)
    mona_system_code NVARCHAR(50),    -- MONA: Member of National Assembly 시스템 코드
    naas_system_code NVARCHAR(50),           -- NAAS: National Assembly Administration System 코드
    
    -- 의원 이름 정보
    korean_name NVARCHAR(100) COLLATE Korean_Wansung_CI_AS NOT NULL,      -- 한글 이름 (기존: hg_nm)
    chinese_name NVARCHAR(100) COLLATE Korean_Wansung_CI_AS,              -- 한자 이름 (기존: hj_nm)
    english_name NVARCHAR(200),              -- 영문 이름 (기존: eng_nm)
    
    -- 개인 정보
    birth_date NVARCHAR(20),                 -- 생년월일
    birth_type NVARCHAR(50),                 -- 생일 구분 (양력/음력)
    gender NVARCHAR(20),                     -- 성별
    
    -- 연락처 정보
    phone_number NVARCHAR(50),               -- 전화번호 (기존: tel_no)
    email_address NVARCHAR(100),             -- 이메일 주소
    personal_homepage NVARCHAR(200),         -- 개인 홈페이지
    assembly_office_address NVARCHAR(300),   -- 국회사무실 주소 (기존: office_addr)
    
    -- 보좌진 정보
    staff_members NVARCHAR(500),             -- 보좌진 목록
    chief_secretary NVARCHAR(200),           -- 비서관 (기존: secretary)
    assistant_secretary NVARCHAR(500),       -- 보조 비서관 (기존: secretary2)
    
    -- 경력 정보
    job_before_election NVARCHAR(200), -- 당선 전 직업 (기존: job_title)
    educational_background NTEXT,            -- 학력 사항
    career_history NTEXT,                    -- 경력 사항
    profile_image_url NVARCHAR(500),         -- 프로필 사진 URL
    additional_titles NTEXT,                 -- 추가 직책 및 역할 (기존: mem_title)
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    -- 기본 제약조건은 CREATE TABLE 후 별도로 추가
);

-- 조건부 유니크 제약조건을 위한 필터링된 인덱스 (NULL이 아닌 경우만)
CREATE UNIQUE INDEX UQ_mona_system_code ON national_assembly_members(mona_system_code) WHERE mona_system_code IS NOT NULL;
CREATE UNIQUE INDEX UQ_naas_system_code ON national_assembly_members(naas_system_code) WHERE naas_system_code IS NOT NULL;

-- 2. 현재 국회의원 정보 테이블 (Current National Assembly Members)
CREATE TABLE current_national_assembly_members (
    term_id INT IDENTITY(1,1) PRIMARY KEY,
    member_id INT NOT NULL,
    
    -- 국회 회기 정보
    assembly_session_number INT NOT NULL,    -- 국회 회기 번호 (기존: daesu - 예: 21대 국회)
    election_district_type NVARCHAR(100),    -- 선거구 구분 (지역구/비례대표 등)
    reelection_count NVARCHAR(100),          -- 당선 횟수 (초선/재선/3선 등)
    assembly_unit NVARCHAR(100),             -- 소속 단위 (기존: units)
    
    -- 정당 정보 (자주 조회되므로 비정규화)
    political_party_name NVARCHAR(100) COLLATE Korean_Wansung_CI_AS,      -- 소속 정당명
    
    -- 지역구 정보 (자주 조회되므로 비정규화)
    electoral_district_name NVARCHAR(100) COLLATE Korean_Wansung_CI_AS,   -- 선거구명 (예: 서울 강남구 갑)
    
    -- 소속 위원회 정보 (자주 조회되므로 비정규화)
    primary_committee_name NVARCHAR(200) COLLATE Korean_Wansung_CI_AS,    -- 상임위원회명
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (member_id) REFERENCES national_assembly_members(member_id),
    
    -- 성능 최적화 인덱스
    INDEX IX_member_session (member_id, assembly_session_number),
    INDEX IX_party_session (political_party_name, assembly_session_number),
    INDEX IX_district_session (electoral_district_name, assembly_session_number)
);

-- 3. 국회 법안정보 테이블 (Legislative Bills)
CREATE TABLE legislative_bills (
    bill_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- 법안 식별 정보
    original_bill_system_id NVARCHAR(50) UNIQUE, -- 원본 시스템의 법안 ID
    bill_number NVARCHAR(50),                     -- 의안번호 (기존: bill_no)
    bill_title NVARCHAR(500) COLLATE Korean_Wansung_CI_AS NOT NULL,           -- 법안명 (기존: bill_name)
    
    -- 소관 위원회 정보 (자주 조회되므로 비정규화)
    responsible_committee_name NVARCHAR(200) COLLATE Korean_Wansung_CI_AS,     -- 소관 위원회명
    responsible_committee_code NVARCHAR(50),      -- 소관 위원회 코드
    
    assembly_session_number INT,                  -- 제출된 국회 회기 (기존: age_number)
    
    -- 법안 제안자 정보
    main_proposer NVARCHAR(500) COLLATE Korean_Wansung_CI_AS,                 -- 대표 발의자
    coproposer_list NVARCHAR(1000) COLLATE Korean_Wansung_CI_AS,              -- 공동 발의자 목록 (기존: member_list)
    government_proposer NVARCHAR(MAX),           -- 정부 제출자 (기존: publ_proposer)
    final_proposer NVARCHAR(200),                -- 최종 제출자 (기존: rst_proposer)
    
    -- 법안 처리 일정
    proposal_date DATE,                          -- 발의일 (기존: propose_dt)
    law_processing_date DATE,                    -- 법률안 처리일 (기존: law_proc_dt)
    law_presentation_date DATE,                  -- 법률안 상정일 (기존: law_present_dt)
    law_submission_date DATE,                    -- 법률안 제출일 (기존: law_submit_dt)
    committee_processing_date DATE,              -- 위원회 처리일 (기존: cmt_proc_dt)
    committee_presentation_date DATE,            -- 위원회 상정일 (기존: cmt_present_dt)
    committee_meeting_date DATE,                 -- 위원회 회의일 (기존: committee_dt)
    final_processing_date DATE,                  -- 최종 처리일 (기존: proc_dt)
    
    -- 법안 처리 결과
    processing_result NVARCHAR(100) COLLATE Korean_Wansung_CI_AS,             -- 처리 결과 (가결/부결/계류 등)
    committee_result_code NVARCHAR(100),         -- 위원회 처리 결과 코드
    law_processing_result_code NVARCHAR(100),    -- 법률안 처리 결과 코드
    
    -- 참조 링크
    bill_detail_url NVARCHAR(1000),             -- 법안 상세 정보 URL
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    -- 성능 최적화 인덱스
    INDEX IX_bill_number (bill_number),
    INDEX IX_committee_session (responsible_committee_name, assembly_session_number),
    INDEX IX_proposal_date (proposal_date)
);

-- 4. 법안 발의자 관계 테이블 (Bill Proposer Relationships)
CREATE TABLE bill_proposer_relationships (
    relationship_id INT IDENTITY(1,1) PRIMARY KEY,
    bill_id INT NOT NULL,
    member_id INT NOT NULL,
    
    -- proposer_role NVARCHAR(50) 컬럼 제거됨 - 단순한 발의자-법안 관계만 추적
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (bill_id) REFERENCES legislative_bills(bill_id),
    FOREIGN KEY (member_id) REFERENCES national_assembly_members(member_id),
    
    UNIQUE(bill_id, member_id)
);

-- 5. 국회 본회의 투표기록 테이블 (Plenary Session Voting Records)
CREATE TABLE plenary_voting_records (
    vote_record_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- 본회의 세션 정보 (자주 조회되므로 비정규화)
    session_code INT,                        -- 세션 코드
    current_session_code INT,                -- 현재 세션 코드 (기존: currents_cd)
    assembly_session_number INT,             -- 국회 회기 번호
    department_code NVARCHAR(50),            -- 부서 코드 (기존: dept_cd)
    voting_date DATE,                        -- 투표 실시일
    
    -- 관련 법안 및 의원 정보
    bill_id INT,                            -- 관련 법안 ID
    member_id INT NOT NULL,                 -- 투표한 의원 ID
    
    -- 투표 결과
    vote_decision NVARCHAR(50) COLLATE Korean_Wansung_CI_AS,             -- 투표 결정 (찬성/반대/기권/불참 등)
    
    -- 참조 링크
    bill_detail_url NVARCHAR(1000),         -- 해당 법안 상세 URL
    bill_name_url NVARCHAR(1000),           -- 법안명 링크 URL
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (bill_id) REFERENCES legislative_bills(bill_id),
    FOREIGN KEY (member_id) REFERENCES national_assembly_members(member_id),
    
    -- 성능 최적화 인덱스
    INDEX IX_session_member (session_code, member_id),
    INDEX IX_bill_vote_decision (bill_id, vote_decision),
    INDEX IX_member_voting_date (member_id, voting_date)
);

-- ========================================
-- 자주 사용되는 데이터 조회를 위한 뷰 (Frequently Used Views)
-- ========================================


-- 7. 법안 상세 정보 뷰 (Bill Details View)
GO
CREATE VIEW legislative_bills_detail_view AS
SELECT 
    b.bill_id,
    b.original_bill_system_id,
    b.bill_number,
    b.bill_title,
    b.responsible_committee_name,
    b.assembly_session_number,
    b.proposal_date,
    b.processing_result,
    COUNT(bp.member_id) as total_proposer_count
FROM legislative_bills b
LEFT JOIN bill_proposer_relationships bp ON b.bill_id = bp.bill_id
LEFT JOIN national_assembly_members m ON bp.member_id = m.member_id
GROUP BY b.bill_id, b.original_bill_system_id, b.bill_number, b.bill_title, 
         b.responsible_committee_name, b.assembly_session_number, b.proposal_date, b.processing_result;

-- 8. 투표 통계 뷰 (Voting Statistics View)
GO
CREATE VIEW voting_statistics_view AS
SELECT 
    v.bill_id,
    b.bill_title,
    v.assembly_session_number,
    v.voting_date,
    COUNT(CASE WHEN v.vote_decision = '찬성' THEN 1 END) as votes_for,
    COUNT(CASE WHEN v.vote_decision = '반대' THEN 1 END) as votes_against,
    COUNT(CASE WHEN v.vote_decision = '기권' THEN 1 END) as abstentions,
    COUNT(CASE WHEN v.vote_decision = '불참' THEN 1 END) as absences,
    COUNT(*) as total_votes
FROM plenary_voting_records v
LEFT JOIN legislative_bills b ON v.bill_id = b.bill_id
GROUP BY v.bill_id, b.bill_title, v.assembly_session_number, v.voting_date;

-- ========================================
-- 데이터 마이그레이션 (Data Migration)
-- ========================================

-- 1. 국회의원 기본정보 마이그레이션
GO
WITH unified_members AS (
    SELECT 
        NULL as mona_system_code,  -- assembly_members_history doesn't have MONA_CD
        NULL as naas_system_code,
        NAME as korean_name,       -- DDL has NAME, not HG_NM
        NAME_HAN as chinese_name,  -- DDL has NAME_HAN, not HJ_NM
        NULL as english_name,      -- No english name in history table
        BIRTH as birth_date,       -- DDL has BIRTH, not BTH_DATE
        NULL as birth_type,        -- No birth type in history table
        NULL as gender,            -- No gender in history table
        NULL as phone_number,      -- No phone in history table
        NULL as email_address,     -- No email in history table
        URL as personal_homepage,  -- DDL has URL, not HOMEPAGE
        NULL as assembly_office_address, -- No address in history table
        NULL as staff_members,     -- No staff in history table
        NULL as chief_secretary,   -- No secretary in history table
        NULL as assistant_secretary, -- No secretary2 in history table
        NULL as job_before_election, -- No job info in history table
        HAK as educational_background, -- DDL has HAK for education
        SANG as career_history,    -- DDL has SANG for career
        NULL as profile_image_url, -- No image URL in history table
        POSI as additional_titles  -- DDL has POSI for position/titles
    FROM assembly_members_history
    WHERE NAME IS NOT NULL
    
    UNION ALL
    
    SELECT 
        MONA_CD as mona_system_code,
        NULL as naas_system_code,
        HG_NM as korean_name,
        HJ_NM as chinese_name,
        ENG_NM as english_name,
        BTH_DATE as birth_date,
        BTH_GBN_NM as birth_type,
        SEX_GBN_NM as gender,
        TEL_NO as phone_number,
        E_MAIL as email_address,
        HOMEPAGE as personal_homepage,
        ASSEM_ADDR as assembly_office_address,
        STAFF as staff_members,
        SECRETARY as chief_secretary,
        SECRETARY2 as assistant_secretary,
        JOB_RES_NM as job_before_election,
        NULL as educational_background,
        NULL as career_history,
        NULL as profile_image_url,
        MEM_TITLE as additional_titles
    FROM assembly_members_profile
    WHERE MONA_CD IS NOT NULL
    
    UNION ALL
    
    SELECT 
        NULL as mona_system_code,
        NAAS_CD as naas_system_code,
        NAAS_NM as korean_name,
        NAAS_CH_NM as chinese_name,
        NAAS_EN_NM as english_name,
        BIRDY_DT as birth_date,
        BIRDY_DIV_CD as birth_type,
        NTR_DIV as gender,
        NAAS_TEL_NO as phone_number,
        NAAS_EMAIL_ADDR as email_address,
        NAAS_HP_URL as personal_homepage,
        NULL as assembly_office_address,
        AIDE_NM as staff_members,
        CHF_SCRT_NM as chief_secretary,
        SCRT_NM as assistant_secretary,
        DTY_NM as job_before_election,
        NULL as educational_background,
        BRF_HST as career_history,
        NAAS_PIC as profile_image_url,
        NULL as additional_titles
    FROM assembly_members_integrated
    WHERE NAAS_CD IS NOT NULL
),
deduplicated_members AS (
    SELECT 
        CASE 
            WHEN mona_system_code IS NOT NULL THEN 'M_' + mona_system_code
            WHEN naas_system_code IS NOT NULL THEN 'N_' + naas_system_code
            ELSE 'K_' + korean_name
        END as unique_id,
        mona_system_code,
        naas_system_code,
        korean_name,
        chinese_name,
        english_name,
        birth_date,
        birth_type,
        gender,
        phone_number,
        email_address,
        personal_homepage,
        assembly_office_address,
        staff_members,
        chief_secretary,
        assistant_secretary,
        job_before_election,
        educational_background,
        career_history,
        profile_image_url,
        additional_titles,
        ROW_NUMBER() OVER (
            PARTITION BY korean_name 
            ORDER BY 
                CASE WHEN mona_system_code IS NOT NULL THEN 1 
                     WHEN naas_system_code IS NOT NULL THEN 2 
                     ELSE 3 END
        ) as rn
    FROM unified_members
    WHERE korean_name IS NOT NULL AND LEN(LTRIM(RTRIM(korean_name))) > 0
)
INSERT INTO national_assembly_members (
    mona_system_code, naas_system_code, korean_name, chinese_name, english_name, 
    birth_date, birth_type, gender, phone_number, email_address, 
    personal_homepage, assembly_office_address, staff_members, chief_secretary, 
    assistant_secretary, job_before_election, educational_background, 
    career_history, profile_image_url, additional_titles
)
SELECT 
    mona_system_code, naas_system_code, korean_name, chinese_name, english_name,
    birth_date, birth_type, gender, phone_number, email_address,
    personal_homepage, assembly_office_address, staff_members, chief_secretary,
    assistant_secretary, job_before_election, educational_background,
    career_history, profile_image_url, additional_titles
FROM deduplicated_members
WHERE rn = 1;

-- 2. 현재 국회의원 정보 마이그레이션 (Profile 테이블 사용)
INSERT INTO current_national_assembly_members (
    member_id, assembly_session_number, election_district_type, reelection_count, assembly_unit,
    political_party_name, electoral_district_name, 
    primary_committee_name
)
SELECT DISTINCT
    m.member_id,
    22 as assembly_session_number,  -- Profile table contains current session data
    p.ELECT_GBN_NM as election_district_type,
    p.REELE_GBN_NM as reelection_count,
    p.UNITS as assembly_unit,
    COALESCE(p.POLY_NM, '무소속') as political_party_name,
    p.ORIG_NM as electoral_district_name,
    p.CMIT_NM as primary_committee_name
FROM assembly_members_profile p
JOIN national_assembly_members m ON p.MONA_CD = m.mona_system_code
WHERE p.MONA_CD IS NOT NULL;

-- 3. 법안정보 마이그레이션
INSERT INTO legislative_bills (
    original_bill_system_id, bill_number, bill_title, responsible_committee_name, 
    responsible_committee_code, assembly_session_number, main_proposer, coproposer_list, 
    government_proposer, final_proposer, proposal_date, law_processing_date, 
    law_presentation_date, law_submission_date, committee_processing_date, 
    committee_presentation_date, committee_meeting_date, final_processing_date,
    processing_result, committee_result_code, law_processing_result_code, bill_detail_url
)
SELECT 
    b.BILL_ID as original_bill_system_id,
    b.BILL_NO as bill_number,
    b.BILL_NAME as bill_title,
    b.COMMITTEE as responsible_committee_name,
    b.COMMITTEE_ID as responsible_committee_code,
    b.AGE as assembly_session_number,
    CASE 
        WHEN b.PROPOSER IS NOT NULL AND CHARINDEX('의원', b.PROPOSER) > 0
        THEN LEFT(b.PROPOSER, CHARINDEX('의원', b.PROPOSER) - 1)
        ELSE b.PROPOSER
    END as main_proposer,
    b.MEMBER_LIST as coproposer_list,
    b.PUBL_PROPOSER as government_proposer,
    b.RST_PROPOSER as final_proposer,
    b.PROPOSE_DT as proposal_date,
    b.LAW_PROC_DT as law_processing_date,
    b.LAW_PRESENT_DT as law_presentation_date,
    b.LAW_SUBMIT_DT as law_submission_date,
    b.CMT_PROC_DT as committee_processing_date,
    b.CMT_PRESENT_DT as committee_presentation_date,
    b.COMMITTEE_DT as committee_meeting_date,
    b.PROC_DT as final_processing_date,
    b.PROC_RESULT as processing_result,
    b.CMT_PROC_RESULT_CD as committee_result_code,
    b.LAW_PROC_RESULT_CD as law_processing_result_code,
    b.DETAIL_LINK as bill_detail_url
FROM assembly_bills b;

-- 4. 법안 발의자 관계 마이그레이션 (대표발의자만)
INSERT INTO bill_proposer_relationships (
    bill_id, member_id
)
SELECT DISTINCT
    b.bill_id,
    m.member_id
    -- proposer_role 컬럼 제거됨
FROM legislative_bills b
JOIN national_assembly_members m ON b.main_proposer = m.korean_name
WHERE b.main_proposer IS NOT NULL;

-- 5. 본회의 투표기록 마이그레이션
INSERT INTO plenary_voting_records (
    session_code, current_session_code, assembly_session_number, department_code, voting_date,
    bill_id, member_id, vote_decision, bill_detail_url, bill_name_url
)
SELECT 
    v.SESSION_CD as session_code,
    v.CURRENTS_CD as current_session_code,
    v.AGE as assembly_session_number,
    v.DEPT_CD as department_code,
    CASE 
        WHEN v.VOTE_DATE IS NOT NULL AND LEN(TRIM(v.VOTE_DATE)) >= 8
        THEN TRY_CAST(LEFT(TRIM(v.VOTE_DATE), 4) + '-' + SUBSTRING(TRIM(v.VOTE_DATE), 5, 2) + '-' + SUBSTRING(TRIM(v.VOTE_DATE), 7, 2) AS DATE)
        ELSE NULL
    END as voting_date,
    bn.bill_id,
    mn.member_id,
    v.RESULT_VOTE_MOD as vote_decision,
    v.BILL_URL as bill_detail_url,
    v.BILL_NAME_URL as bill_name_url
FROM assembly_plenary_session_vote v
LEFT JOIN legislative_bills bn ON v.BILL_ID = bn.original_bill_system_id
LEFT JOIN national_assembly_members mn ON v.MONA_CD = mn.mona_system_code
WHERE mn.member_id IS NOT NULL;

-- ========================================
-- 성능 최적화 인덱스 생성
-- ========================================

-- 국회의원 테이블 인덱스
CREATE INDEX IX_members_mona_code ON national_assembly_members(mona_system_code);
CREATE INDEX IX_members_naas_code ON national_assembly_members(naas_system_code);
CREATE INDEX IX_members_korean_name ON national_assembly_members(korean_name);

-- 복합 인덱스 (자주 함께 조회되는 컬럼들)
CREATE INDEX IX_terms_party_district ON current_national_assembly_members(political_party_name, electoral_district_name, assembly_session_number);
CREATE INDEX IX_bills_committee_session ON legislative_bills(responsible_committee_name, assembly_session_number, proposal_date);
-- 투표 기록 인덱스 (날짜 기반 검색 최적화)
CREATE INDEX IX_votes_date_session ON plenary_voting_records(voting_date, session_code);
CREATE INDEX IX_votes_member_date ON plenary_voting_records(member_id, voting_date);
CREATE INDEX IX_votes_bill_decision ON plenary_voting_records(bill_id, vote_decision);

-- ========================================
-- 트리거 생성 (MS-SQL 네이밍 컨벤션: TR_테이블명_동작)
-- ========================================

-- 국회의원 기본정보 테이블 감사 트리거
GO
CREATE TRIGGER TR_national_assembly_members_audit
ON national_assembly_members
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 변경 이력을 audit 테이블에 기록하는 로직
    -- (실제 구현시 audit 테이블 필요)
    PRINT 'Member data change recorded';
END;

-- 투표 기록 검증 트리거 (SQL Server는 BEFORE 트리거 미지원, AFTER 트리거로 변경)
GO
CREATE TRIGGER TR_plenary_voting_records_validation
ON plenary_voting_records
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 투표 결과 값 검증
    IF EXISTS (
        SELECT 1 FROM inserted 
        WHERE vote_decision NOT IN ('찬성', '반대', '기권', '불참')
    )
    BEGIN
        RAISERROR('Invalid vote decision value', 16, 1);
        ROLLBACK TRANSACTION;
    END
END;

-- ========================================
-- 저장 프로시저 생성 (MS-SQL 네이밍 컨벤션: SP_기능명 또는 USP_기능명)
-- ========================================

-- 현재 국회의원 조회 저장 프로시저
GO
CREATE PROCEDURE SP_GetCurrentAssemblyMembers
    @AssemblySessionNumber INT = NULL,
    @PoliticalPartyName NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        t.member_id,
        m.korean_name,
        t.political_party_name,
        t.electoral_district_name,
        t.primary_committee_name,
        t.assembly_session_number
    FROM current_national_assembly_members t
    JOIN national_assembly_members m ON t.member_id = m.member_id
    WHERE (@AssemblySessionNumber IS NULL OR t.assembly_session_number = @AssemblySessionNumber)
      AND (@PoliticalPartyName IS NULL OR t.political_party_name = @PoliticalPartyName)
    ORDER BY m.korean_name;
END;

-- 법안별 투표 결과 조회 저장 프로시저
GO
CREATE PROCEDURE SP_GetBillVotingResults
    @BillId INT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        b.bill_title,
        b.proposal_date,
        v.votes_for,
        v.votes_against,
        v.abstentions,
        v.absences,
        v.total_votes
    FROM voting_statistics_view v
    INNER JOIN legislative_bills b ON v.bill_id = b.bill_id
    WHERE b.bill_id = @BillId;
END;

-- 의원별 투표 이력 조회 저장 프로시저
GO
CREATE PROCEDURE SP_GetMemberVotingHistory
    @MemberId INT,
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        b.bill_title,
        b.proposal_date,
        v.vote_decision,
        v.voting_date,
        b.processing_result
    FROM plenary_voting_records v
    INNER JOIN legislative_bills b ON v.bill_id = b.bill_id
    WHERE v.member_id = @MemberId
      AND (@StartDate IS NULL OR v.voting_date >= @StartDate)
      AND (@EndDate IS NULL OR v.voting_date <= @EndDate)
    ORDER BY v.voting_date DESC;
END;

-- 정당별 투표 통계 저장 프로시저
GO
CREATE PROCEDURE SP_GetPartyVotingStatistics
    @AssemblySessionNumber INT,
    @BillId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        t.political_party_name,
        COUNT(CASE WHEN v.vote_decision = '찬성' THEN 1 END) as party_votes_for,
        COUNT(CASE WHEN v.vote_decision = '반대' THEN 1 END) as party_votes_against,
        COUNT(CASE WHEN v.vote_decision = '기권' THEN 1 END) as party_abstentions,
        COUNT(CASE WHEN v.vote_decision = '불참' THEN 1 END) as party_absences,
        COUNT(*) as party_total_votes
    FROM plenary_voting_records v
    INNER JOIN current_national_assembly_members t ON v.member_id = t.member_id 
        AND t.assembly_session_number = v.assembly_session_number
    WHERE v.assembly_session_number = @AssemblySessionNumber
      AND (@BillId IS NULL OR v.bill_id = @BillId)
    GROUP BY t.political_party_name
    ORDER BY t.political_party_name;
END;

-- ========================================
-- 사용자 정의 함수 (MS-SQL 네이밍 컨벤션: FN_기능명 또는 UDF_기능명)
-- ========================================

-- 의원의 현재 나이 계산 함수
GO
CREATE FUNCTION FN_CalculateMemberAge(@BirthDate NVARCHAR(20))
RETURNS INT
AS
BEGIN
    DECLARE @Age INT;
    
    -- 생년월일 문자열을 DATE로 변환하여 나이 계산
    IF @BirthDate IS NOT NULL AND LEN(@BirthDate) >= 8
    BEGIN
        DECLARE @Birth DATE;
        SET @Birth = TRY_CAST(LEFT(@BirthDate, 4) + '-' + 
                              SUBSTRING(@BirthDate, 5, 2) + '-' + 
                              SUBSTRING(@BirthDate, 7, 2) AS DATE);
        
        IF @Birth IS NOT NULL
            SET @Age = DATEDIFF(YEAR, @Birth, GETDATE()) - 
                      CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, @Birth, GETDATE()), @Birth) > GETDATE() 
                           THEN 1 ELSE 0 END;
    END
    
    RETURN @Age;
END;

-- 투표 참여율 계산 함수 (current_session_code 고려)
GO
CREATE FUNCTION FN_CalculateVotingParticipationRate(
    @MemberId INT, 
    @AssemblySessionNumber INT,
    @CurrentSessionCode INT = NULL
)
RETURNS DECIMAL(5,2)
AS
BEGIN
    DECLARE @ParticipationRate DECIMAL(5,2);
    DECLARE @TotalVotes INT;
    DECLARE @ParticipatedVotes INT;
    
    -- current_session_code가 제공된 경우 세션별로 필터링
    IF @CurrentSessionCode IS NOT NULL
    BEGIN
        SELECT @TotalVotes = COUNT(*)
        FROM plenary_voting_records
        WHERE member_id = @MemberId 
          AND assembly_session_number = @AssemblySessionNumber
          AND current_session_code = @CurrentSessionCode;
        
        SELECT @ParticipatedVotes = COUNT(*)
        FROM plenary_voting_records
        WHERE member_id = @MemberId 
          AND assembly_session_number = @AssemblySessionNumber
          AND current_session_code = @CurrentSessionCode
          AND vote_decision IN ('찬성', '반대', '기권');
    END
    ELSE
    BEGIN
        -- current_session_code가 없으면 기존 로직 사용
        SELECT @TotalVotes = COUNT(*)
        FROM plenary_voting_records
        WHERE member_id = @MemberId AND assembly_session_number = @AssemblySessionNumber;
        
        SELECT @ParticipatedVotes = COUNT(*)
        FROM plenary_voting_records
        WHERE member_id = @MemberId 
          AND assembly_session_number = @AssemblySessionNumber
          AND vote_decision IN ('찬성', '반대', '기권');
    END
    
    IF @TotalVotes > 0
        SET @ParticipationRate = (CAST(@ParticipatedVotes AS DECIMAL(5,2)) / @TotalVotes) * 100;
    ELSE
        SET @ParticipationRate = 0;
    
    RETURN @ParticipationRate;
END;

-- ========================================
-- 통계 정보 업데이트
-- ========================================
GO
UPDATE STATISTICS national_assembly_members;
UPDATE STATISTICS current_national_assembly_members;
UPDATE STATISTICS legislative_bills;
UPDATE STATISTICS bill_proposer_relationships;
UPDATE STATISTICS plenary_voting_records;

PRINT '한국 국회 데이터베이스 정규화 테이블 생성 및 데이터 마이그레이션이 완료되었습니다.';
PRINT '- 명확하고 이해하기 쉬운 영문 네이밍 컨벤션 적용';
PRINT '- 한국 국회 시스템에 익숙하지 않은 개발자도 쉽게 이해할 수 있는 구조';
PRINT '- 자주 사용되는 쿼리를 위한 뷰와 성능 최적화 인덱스 완료';