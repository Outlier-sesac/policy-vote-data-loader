-- ========================================
-- 한국 국회 데이터베이스 - voting_date 및 proposer_role 수정 스크립트
-- 이미 마이그레이션된 데이터에 대한 UPDATE 전용 스크립트
-- 실행 시간: 약 5-15분 예상
-- ========================================

PRINT '========================================';
PRINT 'voting_date 및 proposer_role 수정 시작';
PRINT '========================================';

-- 기존 데이터 현황 확인
DECLARE @TotalVoteRecords INT, @NullVotingDates INT, @TotalProposerRecords INT, @NullProposerRoles INT;

SELECT @TotalVoteRecords = COUNT(*) FROM plenary_voting_records;
SELECT @NullVotingDates = COUNT(*) FROM plenary_voting_records WHERE voting_date IS NULL;
SELECT @TotalProposerRecords = COUNT(*) FROM bill_proposer_relationships;
SELECT @NullProposerRoles = COUNT(*) FROM bill_proposer_relationships WHERE proposer_role IS NULL OR proposer_role = '';

PRINT '수정 전 데이터 현황:';
PRINT '- 전체 투표기록: ' + CAST(@TotalVoteRecords AS NVARCHAR(10)) + '건';
PRINT '- voting_date가 NULL인 레코드: ' + CAST(@NullVotingDates AS NVARCHAR(10)) + '건';
PRINT '- 전체 발의자 관계: ' + CAST(@TotalProposerRecords AS NVARCHAR(10)) + '건';
PRINT '- proposer_role이 NULL/빈값인 레코드: ' + CAST(@NullProposerRoles AS NVARCHAR(10)) + '건';
PRINT '';

-- ========================================
-- 1단계: voting_date 수정
-- ========================================
PRINT '1단계: voting_date 수정 중...';

-- 배치 크기 설정 (메모리 사용량 제어)
DECLARE @BatchSize INT = 5000;
DECLARE @ProcessedCount INT = 0;
DECLARE @TotalBatches INT = CEILING(CAST(@TotalVoteRecords AS FLOAT) / @BatchSize);

PRINT '총 ' + CAST(@TotalBatches AS NVARCHAR(10)) + '개 배치로 처리 예정 (배치 크기: ' + CAST(@BatchSize AS NVARCHAR(10)) + ')';

-- 원본 데이터와 조인하여 voting_date 업데이트
DECLARE @CurrentBatch INT = 1;
SET @ProcessedCount = 0;

WHILE @ProcessedCount < @TotalVoteRecords
BEGIN
    PRINT '배치 ' + CAST(@CurrentBatch AS NVARCHAR(10)) + '/' + CAST(@TotalBatches AS NVARCHAR(10)) + ' 처리 중...';
    
    -- 배치별로 voting_date 업데이트
    WITH BatchedRecords AS (
        SELECT 
            pvr.vote_record_id,
            v.VOTE_DATE,
            ROW_NUMBER() OVER (ORDER BY pvr.vote_record_id) as rn
        FROM plenary_voting_records pvr
        JOIN assembly_plenary_session_vote v ON pvr.session_code = v.SESSION_CD 
            AND pvr.member_id IN (
                SELECT m.member_id 
                FROM national_assembly_members m 
                WHERE m.mona_system_code = v.MONA_CD
            )
        WHERE pvr.voting_date IS NULL  -- NULL인 것만 업데이트
    )
    UPDATE pvr
    SET voting_date = CASE 
        WHEN br.VOTE_DATE IS NOT NULL AND LEN(TRIM(br.VOTE_DATE)) >= 8
        THEN TRY_CAST(LEFT(TRIM(br.VOTE_DATE), 4) + '-' + SUBSTRING(TRIM(br.VOTE_DATE), 5, 2) + '-' + SUBSTRING(TRIM(br.VOTE_DATE), 7, 2) AS DATE)
        ELSE NULL
    END
    FROM plenary_voting_records pvr
    JOIN BatchedRecords br ON pvr.vote_record_id = br.vote_record_id
    WHERE br.rn > @ProcessedCount AND br.rn <= (@ProcessedCount + @BatchSize);
    
    SET @ProcessedCount = @ProcessedCount + @BatchSize;
    SET @CurrentBatch = @CurrentBatch + 1;
    
    -- 진행률 표시
    DECLARE @ProgressPercent DECIMAL(5,2) = (CAST(@ProcessedCount AS FLOAT) / @TotalVoteRecords) * 100;
    IF @ProgressPercent > 100 SET @ProgressPercent = 100;
    PRINT '   진행률: ' + CAST(@ProgressPercent AS NVARCHAR(10)) + '%';
END

-- voting_date 수정 결과 확인
DECLARE @UpdatedVotingDates INT;
SELECT @UpdatedVotingDates = COUNT(*) FROM plenary_voting_records WHERE voting_date IS NOT NULL;

PRINT '';
PRINT 'voting_date 수정 결과:';
PRINT '- 수정 후 유효한 voting_date: ' + CAST(@UpdatedVotingDates AS NVARCHAR(10)) + '건';
PRINT '- 개선된 레코드 수: ' + CAST((@UpdatedVotingDates - (@TotalVoteRecords - @NullVotingDates)) AS NVARCHAR(10)) + '건';

-- ========================================
-- 2단계: proposer_role 관련 코드 제거됨 (컬럼 삭제로 인해)
-- ========================================
PRINT '';
PRINT '2단계: proposer_role 컬럼이 제거되어 수정 불필요';

-- proposer_role 컬럼이 제거되어 수정 불필요
DECLARE @UpdatedProposerRoles INT = 0;

PRINT '';
PRINT 'proposer_role 수정 결과:';
PRINT '- proposer_role 컬럼이 제거되어 수정 불필요';

-- ========================================
-- 3단계: 추가 데이터 품질 개선
-- ========================================
PRINT '';
PRINT '3단계: 추가 데이터 품질 개선 중...';

-- 누락된 발의자 관계 추가 (기존에 매칭되지 않았던 것들)
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
    AND CHARINDEX('의원', b.main_proposer) > 0
    AND NOT EXISTS (
        SELECT 1 FROM bill_proposer_relationships bp 
        WHERE bp.bill_id = b.bill_id AND bp.member_id = m.member_id
    );

DECLARE @NewProposerRelations INT = @@ROWCOUNT;
PRINT '- 새로 추가된 발의자 관계: ' + CAST(@NewProposerRelations AS NVARCHAR(10)) + '건';

-- ========================================
-- 4단계: 최종 검증
-- ========================================
PRINT '';
PRINT '4단계: 최종 데이터 검증 중...';

-- 최종 통계
DECLARE @FinalVotingDates INT, @FinalNullVotingDates INT;
DECLARE @FinalProposerRoles INT, @FinalNullProposerRoles INT;

SELECT @FinalVotingDates = COUNT(*) FROM plenary_voting_records WHERE voting_date IS NOT NULL;
SELECT @FinalNullVotingDates = COUNT(*) FROM plenary_voting_records WHERE voting_date IS NULL;
SELECT @FinalProposerRoles = COUNT(*) FROM bill_proposer_relationships WHERE proposer_role IS NOT NULL AND proposer_role != '';
SELECT @FinalNullProposerRoles = COUNT(*) FROM bill_proposer_relationships WHERE proposer_role IS NULL OR proposer_role = '';

PRINT '';
PRINT '최종 데이터 현황:';
PRINT '- 유효한 voting_date: ' + CAST(@FinalVotingDates AS NVARCHAR(10)) + '건';
PRINT '- NULL voting_date: ' + CAST(@FinalNullVotingDates AS NVARCHAR(10)) + '건';
PRINT '- 유효한 proposer_role: ' + CAST(@FinalProposerRoles AS NVARCHAR(10)) + '건';
PRINT '- NULL/빈 proposer_role: ' + CAST(@FinalNullProposerRoles AS NVARCHAR(10)) + '건';

-- 개선 정도 계산
DECLARE @VotingDateImprovement DECIMAL(5,2) = 
    CASE WHEN @TotalVoteRecords > 0 
    THEN (CAST((@FinalVotingDates - (@TotalVoteRecords - @NullVotingDates)) AS FLOAT) / @NullVotingDates) * 100
    ELSE 0 END;

DECLARE @ProposerRoleImprovement DECIMAL(5,2) = 
    CASE WHEN @TotalProposerRecords > 0 
    THEN (CAST((@FinalProposerRoles - (@TotalProposerRecords - @NullProposerRoles)) AS FLOAT) / @NullProposerRoles) * 100
    ELSE 0 END;

PRINT '';
PRINT '개선 정도:';
PRINT '- voting_date 수정률: ' + CAST(@VotingDateImprovement AS NVARCHAR(10)) + '%';
PRINT '- proposer_role 수정률: ' + CAST(@ProposerRoleImprovement AS NVARCHAR(10)) + '%';

-- 샘플 데이터 확인
PRINT '';
PRINT '샘플 데이터 확인:';
PRINT '수정된 voting_date 샘플 (상위 5개):';
SELECT TOP 5 
    vote_record_id, 
    voting_date, 
    vote_decision,
    assembly_session_number
FROM plenary_voting_records 
WHERE voting_date IS NOT NULL
ORDER BY voting_date DESC;

PRINT '';
PRINT '수정된 proposer_role 샘플 (상위 5개):';
SELECT TOP 5 
    bp.relationship_id,
    'proposer' as role,
    m.korean_name,
    b.bill_title
FROM bill_proposer_relationships bp
JOIN national_assembly_members m ON bp.member_id = m.member_id
JOIN legislative_bills b ON bp.bill_id = b.bill_id
ORDER BY bp.relationship_id;

PRINT '';
PRINT '========================================';
PRINT 'voting_date 및 proposer_role 수정 완료!';
PRINT '========================================';

-- 권장 사항
PRINT '';
PRINT '권장 후속 작업:';
PRINT '1. 통계 정보 업데이트:';
PRINT '   UPDATE STATISTICS plenary_voting_records;';
PRINT '   UPDATE STATISTICS bill_proposer_relationships;';
PRINT '';
PRINT '2. 인덱스 재구성 (필요시):';
PRINT '   ALTER INDEX IX_votes_date_session ON plenary_voting_records REBUILD;';
PRINT '';
PRINT '3. 데이터 검증 쿼리 실행으로 결과 확인';
PRINT '========================================';