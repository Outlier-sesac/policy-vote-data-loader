-- ========================================
-- 한국 국회 데이터베이스 정규화 - STEP 5: 투표기록 및 발의자 관계 마이그레이션
-- 실행 시간: 약 20-60분 예상 (투표 데이터 양에 따라)
-- ========================================

USE [database_name]; -- 실제 데이터베이스 이름으로 변경 필요
-- GO -- 필요시 주석 해제

PRINT '========================================';
PRINT 'STEP 5: 투표기록 및 발의자 관계 마이그레이션 시작';
PRINT '========================================';

-- 기존 데이터 확인
DECLARE @SourceVoteCount INT, @BillCount INT, @MemberCount INT;
SELECT @SourceVoteCount = COUNT(*) FROM assembly_plenary_session_vote;
SELECT @BillCount = COUNT(*) FROM legislative_bills;
SELECT @MemberCount = COUNT(*) FROM national_assembly_members;

PRINT '원본 데이터 현황:';
PRINT '- assembly_plenary_session_vote: ' + CAST(@SourceVoteCount AS NVARCHAR(10)) + '건';
PRINT '- legislative_bills: ' + CAST(@BillCount AS NVARCHAR(10)) + '건';
PRINT '- national_assembly_members: ' + CAST(@MemberCount AS NVARCHAR(10)) + '명';
PRINT '';

-- ========================================
-- 1단계: 법안 발의자 관계 마이그레이션
-- ========================================
PRINT '1단계: 법안 발의자 관계 마이그레이션 중...';

-- 배치 크기 설정
DECLARE @BatchSize INT = 2000;
DECLARE @ProcessedCount INT = 0;

-- 대표발의자 관계 생성
INSERT INTO bill_proposer_relationships (
    bill_id, member_id
)
SELECT DISTINCT
    b.bill_id,
    m.member_id,
    -- proposer_role 컬럼 제거됨
FROM legislative_bills b
JOIN national_assembly_members m ON 
    CASE 
        WHEN CHARINDEX('의원', b.main_proposer) > 0 
        THEN LEFT(b.main_proposer, CHARINDEX('의원', b.main_proposer) - 1)
        ELSE b.main_proposer
    END = m.korean_name
WHERE b.main_proposer IS NOT NULL
    AND LEN(LTRIM(RTRIM(b.main_proposer))) > 0
    AND m.korean_name IS NOT NULL
    AND LEN(LTRIM(RTRIM(m.korean_name))) > 1
    AND CHARINDEX('의원', b.main_proposer) > 0;

DECLARE @ProposerRelationCount INT;
SELECT @ProposerRelationCount = COUNT(*) FROM bill_proposer_relationships;

PRINT '- 법안 발의자 관계 생성 완료: ' + CAST(@ProposerRelationCount AS NVARCHAR(10)) + '건';

-- ========================================
-- 2단계: 본회의 투표기록 마이그레이션
-- ========================================
PRINT '';
PRINT '2단계: 본회의 투표기록 마이그레이션 중...';

-- 총 처리할 레코드 수 계산
DECLARE @TotalVoteRows INT;
SELECT @TotalVoteRows = COUNT(*) FROM assembly_plenary_session_vote;
DECLARE @TotalBatches INT = CEILING(CAST(@TotalVoteRows AS FLOAT) / @BatchSize);

PRINT '총 ' + CAST(@TotalBatches AS NVARCHAR(10)) + '개 배치로 처리 예정 (배치 크기: ' + CAST(@BatchSize AS NVARCHAR(10)) + ')';

-- 배치 처리로 투표기록 마이그레이션
DECLARE @CurrentBatch INT = 1;
SET @ProcessedCount = 0;

WHILE @ProcessedCount < @TotalVoteRows
BEGIN
    PRINT '배치 ' + CAST(@CurrentBatch AS NVARCHAR(10)) + '/' + CAST(@TotalBatches AS NVARCHAR(10)) + ' 처리 중...';
    
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
    FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY SESSION_CD, MONA_CD) as rn
        FROM assembly_plenary_session_vote
    ) v
    LEFT JOIN legislative_bills bn ON v.BILL_ID = bn.original_bill_system_id
    LEFT JOIN national_assembly_members mn ON v.MONA_CD = mn.mona_system_code
    WHERE v.rn > @ProcessedCount 
        AND v.rn <= (@ProcessedCount + @BatchSize)
        AND mn.member_id IS NOT NULL;  -- 유효한 의원 ID가 있는 경우만
    
    SET @ProcessedCount = @ProcessedCount + @BatchSize;
    SET @CurrentBatch = @CurrentBatch + 1;
    
    -- 진행률 표시
    DECLARE @ProgressPercent DECIMAL(5,2) = (CAST(@ProcessedCount AS FLOAT) / @TotalVoteRows) * 100;
    IF @ProgressPercent > 100 SET @ProgressPercent = 100;
    PRINT '   진행률: ' + CAST(@ProgressPercent AS NVARCHAR(10)) + '%';
    
    -- 메모리 정리를 위한 짧은 대기
    IF @CurrentBatch % 10 = 0
    BEGIN
        PRINT '   메모리 정리 중...';
        WAITFOR DELAY '00:00:02';  -- 2초 대기
    END
END

-- 마이그레이션 결과 확인
DECLARE @MigratedVoteCount INT;
SELECT @MigratedVoteCount = COUNT(*) FROM plenary_voting_records;

PRINT '';
PRINT '마이그레이션 결과:';
PRINT '- 원본 투표기록: ' + CAST(@SourceVoteCount AS NVARCHAR(10)) + '건';
PRINT '- 마이그레이션된 투표기록: ' + CAST(@MigratedVoteCount AS NVARCHAR(10)) + '건';

-- 데이터 품질 검증
PRINT '';
PRINT '데이터 품질 검증 중...';

-- 투표 결정 유형별 분포
PRINT '';
PRINT '투표 결정 유형별 분포:';
SELECT 
    COALESCE(vote_decision, '결정 없음') as decision_type, 
    COUNT(*) as vote_count,
    CAST((COUNT(*) * 100.0 / @MigratedVoteCount) AS DECIMAL(5,2)) as percentage
FROM plenary_voting_records 
GROUP BY vote_decision
ORDER BY COUNT(*) DESC;

-- 국회 회기별 투표기록 분포
PRINT '';
PRINT '국회 회기별 투표기록 분포:';
SELECT 
    COALESCE(CAST(assembly_session_number AS NVARCHAR(10)), '회기 없음') as session_number, 
    COUNT(*) as vote_count
FROM plenary_voting_records 
GROUP BY assembly_session_number
ORDER BY assembly_session_number DESC;

-- 연도별 투표 현황 (최근 5년)
PRINT '';
PRINT '연도별 투표 현황 (최근 5년):';
SELECT 
    YEAR(voting_date) as voting_year,
    COUNT(*) as vote_count
FROM plenary_voting_records 
WHERE voting_date IS NOT NULL 
    AND voting_date >= DATEADD(YEAR, -5, GETDATE())
GROUP BY YEAR(voting_date)
ORDER BY voting_year DESC;

-- 외래키 제약조건 검증
DECLARE @OrphanedVotes INT, @OrphanedProposers INT;

SELECT @OrphanedVotes = COUNT(*) 
FROM plenary_voting_records v
LEFT JOIN national_assembly_members m ON v.member_id = m.member_id
WHERE m.member_id IS NULL;

SELECT @OrphanedProposers = COUNT(*) 
FROM bill_proposer_relationships bp
LEFT JOIN legislative_bills b ON bp.bill_id = b.bill_id
LEFT JOIN national_assembly_members m ON bp.member_id = m.member_id
WHERE b.bill_id IS NULL OR m.member_id IS NULL;

PRINT '';
IF @OrphanedVotes > 0 OR @OrphanedProposers > 0
BEGIN
    PRINT '참조 무결성 경고:';
    IF @OrphanedVotes > 0 
        PRINT '- Orphaned 투표기록: ' + CAST(@OrphanedVotes AS NVARCHAR(10)) + '건';
    IF @OrphanedProposers > 0 
        PRINT '- Orphaned 발의자 관계: ' + CAST(@OrphanedProposers AS NVARCHAR(10)) + '건';
END
ELSE
BEGIN
    PRINT '✓ 참조 무결성 검증 통과';
END

-- 매칭 실패 분석
DECLARE @UnmatchedVotes INT, @UnmatchedBills INT;

SELECT @UnmatchedVotes = COUNT(*) 
FROM assembly_plenary_session_vote v
LEFT JOIN national_assembly_members m ON v.MONA_CD = m.mona_system_code
WHERE m.member_id IS NULL AND v.MONA_CD IS NOT NULL;

SELECT @UnmatchedBills = COUNT(*) 
FROM assembly_plenary_session_vote v
LEFT JOIN legislative_bills b ON v.BILL_ID = b.original_bill_system_id
WHERE b.bill_id IS NULL AND v.BILL_ID IS NOT NULL;

PRINT '';
PRINT '매칭 실패 분석:';
PRINT '- 매칭되지 않은 의원: ' + CAST(@UnmatchedVotes AS NVARCHAR(10)) + '건';
PRINT '- 매칭되지 않은 법안: ' + CAST(@UnmatchedBills AS NVARCHAR(10)) + '건';

-- 성공률 계산
DECLARE @SuccessRate DECIMAL(5,2) = (CAST(@MigratedVoteCount AS FLOAT) / @SourceVoteCount) * 100;
PRINT '';
PRINT '마이그레이션 성공률: ' + CAST(@SuccessRate AS NVARCHAR(10)) + '%';

PRINT '';
PRINT '========================================';
PRINT 'STEP 5: 투표기록 및 발의자 관계 마이그레이션 완료';
PRINT '========================================';