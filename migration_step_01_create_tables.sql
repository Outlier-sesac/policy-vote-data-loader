-- ========================================
-- 한국 국회 데이터베이스 정규화 - STEP 1: 테이블 생성
-- 실행 시간: 약 1-2분 예상
-- ========================================

USE [database_name]; -- 실제 데이터베이스 이름으로 변경 필요
-- GO -- 필요시 주석 해제

PRINT '========================================';
PRINT 'STEP 1: 테이블 생성 시작';
PRINT '========================================';

-- 1. 국회의원 기본정보 테이블 (National Assembly Members)
PRINT '1. 국회의원 기본정보 테이블 생성 중...';
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
    
    created_at DATETIME2 DEFAULT GETDATE()
);
PRINT '   - national_assembly_members 테이블 생성 완료';

-- 조건부 유니크 제약조건을 위한 필터링된 인덱스 (NULL이 아닌 경우만)
CREATE UNIQUE INDEX UQ_mona_system_code ON national_assembly_members(mona_system_code) WHERE mona_system_code IS NOT NULL;
CREATE UNIQUE INDEX UQ_naas_system_code ON national_assembly_members(naas_system_code) WHERE naas_system_code IS NOT NULL;
PRINT '   - 고유 인덱스 생성 완료';

-- 2. 현재 국회의원 정보 테이블 (Current National Assembly Members)
PRINT '2. 현재 국회의원 정보 테이블 생성 중...';
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
PRINT '   - current_national_assembly_members 테이블 생성 완료';

-- 3. 국회 법안정보 테이블 (Legislative Bills)
PRINT '3. 국회 법안정보 테이블 생성 중...';
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
PRINT '   - legislative_bills 테이블 생성 완료';

-- 4. 법안 발의자 관계 테이블 (Bill Proposer Relationships)
PRINT '4. 법안 발의자 관계 테이블 생성 중...';
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
PRINT '   - bill_proposer_relationships 테이블 생성 완료';

-- 5. 국회 본회의 투표기록 테이블 (Plenary Session Voting Records)
PRINT '5. 국회 본회의 투표기록 테이블 생성 중...';
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
PRINT '   - plenary_voting_records 테이블 생성 완료';

PRINT '========================================';
PRINT 'STEP 1: 테이블 생성 완료';
PRINT '총 5개 테이블이 성공적으로 생성되었습니다.';
PRINT '- national_assembly_members';
PRINT '- current_national_assembly_members';
PRINT '- legislative_bills';
PRINT '- bill_proposer_relationships';
PRINT '- plenary_voting_records';
PRINT '========================================';