-- ========================================
-- 한국 국회 데이터베이스 정규화 - STEP 2: 국회의원 기본정보 마이그레이션
-- 실행 시간: 약 5-10분 예상 (데이터 양에 따라)
-- ========================================

USE [database_name]; -- 실제 데이터베이스 이름으로 변경 필요
-- GO -- 필요시 주석 해제

PRINT '========================================';
PRINT 'STEP 2: 국회의원 기본정보 마이그레이션 시작';
PRINT '========================================';

-- 기존 데이터 확인
DECLARE @SourceHistoryCount INT, @SourceProfileCount INT, @SourceIntegratedCount INT;
SELECT @SourceHistoryCount = COUNT(*) FROM assembly_members_history WHERE NAME IS NOT NULL;
SELECT @SourceProfileCount = COUNT(*) FROM assembly_members_profile WHERE MONA_CD IS NOT NULL;
SELECT @SourceIntegratedCount = COUNT(*) FROM assembly_members_integrated WHERE NAAS_CD IS NOT NULL;

PRINT '원본 데이터 현황:';
PRINT '- assembly_members_history: ' + CAST(@SourceHistoryCount AS NVARCHAR(10)) + '건';
PRINT '- assembly_members_profile: ' + CAST(@SourceProfileCount AS NVARCHAR(10)) + '건';
PRINT '- assembly_members_integrated: ' + CAST(@SourceIntegratedCount AS NVARCHAR(10)) + '건';
PRINT '';

-- 배치 크기 설정 (메모리 사용량 제어)
DECLARE @BatchSize INT = 1000;
DECLARE @ProcessedCount INT = 0;
DECLARE @TotalBatches INT;

PRINT '국회의원 기본정보 통합 및 중복 제거 중...';

-- 1단계: 통합 데이터 생성 (배치 처리)
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

-- 마이그레이션 결과 확인
DECLARE @MigratedCount INT;
SELECT @MigratedCount = COUNT(*) FROM national_assembly_members;

PRINT '';
PRINT '마이그레이션 결과:';
PRINT '- 최종 마이그레이션된 의원 수: ' + CAST(@MigratedCount AS NVARCHAR(10)) + '명';

-- 데이터 품질 검증
DECLARE @ValidNameCount INT, @ValidMONACount INT, @ValidNAASCount INT;
SELECT @ValidNameCount = COUNT(*) FROM national_assembly_members WHERE korean_name IS NOT NULL AND LEN(LTRIM(RTRIM(korean_name))) > 0;
SELECT @ValidMONACount = COUNT(*) FROM national_assembly_members WHERE mona_system_code IS NOT NULL;
SELECT @ValidNAASCount = COUNT(*) FROM national_assembly_members WHERE naas_system_code IS NOT NULL;

PRINT '';
PRINT '데이터 품질 검증:';
PRINT '- 유효한 한글 이름: ' + CAST(@ValidNameCount AS NVARCHAR(10)) + '명';
PRINT '- MONA 시스템 코드 보유: ' + CAST(@ValidMONACount AS NVARCHAR(10)) + '명';
PRINT '- NAAS 시스템 코드 보유: ' + CAST(@ValidNAASCount AS NVARCHAR(10)) + '명';

-- 중복 검사
DECLARE @DuplicateNames INT;
SELECT @DuplicateNames = COUNT(*) 
FROM (
    SELECT korean_name, COUNT(*) as cnt 
    FROM national_assembly_members 
    GROUP BY korean_name 
    HAVING COUNT(*) > 1
) duplicates;

IF @DuplicateNames > 0
BEGIN
    PRINT '';
    PRINT '경고: 중복된 이름이 ' + CAST(@DuplicateNames AS NVARCHAR(10)) + '건 발견되었습니다.';
    PRINT '중복 이름 목록:';
    SELECT korean_name, COUNT(*) as count_occurrences
    FROM national_assembly_members 
    GROUP BY korean_name 
    HAVING COUNT(*) > 1
    ORDER BY count_occurrences DESC;
END
ELSE
BEGIN
    PRINT '✓ 중복 이름 없음 - 데이터 정합성 확인';
END

PRINT '';
PRINT '========================================';
PRINT 'STEP 2: 국회의원 기본정보 마이그레이션 완료';
PRINT '========================================';