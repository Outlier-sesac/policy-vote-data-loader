-- ========================================
-- 한국 국회 데이터베이스 정규화 - STEP 4: 법안정보 마이그레이션
-- 실행 시간: 약 10-30분 예상 (법안 데이터 양에 따라)
-- ========================================

USE [database_name]; -- 실제 데이터베이스 이름으로 변경 필요
-- GO -- 필요시 주석 해제

PRINT '========================================';
PRINT 'STEP 4: 법안정보 마이그레이션 시작';
PRINT '========================================';

-- 기존 데이터 확인
DECLARE @SourceBillCount INT;
SELECT @SourceBillCount = COUNT(*) FROM assembly_bills;

PRINT '원본 데이터 현황:';
PRINT '- assembly_bills: ' + CAST(@SourceBillCount AS NVARCHAR(10)) + '건';
PRINT '';

-- 배치 크기 설정 (대용량 데이터 처리를 위한 메모리 최적화)
DECLARE @BatchSize INT = 5000;
DECLARE @ProcessedCount INT = 0;
DECLARE @CurrentBatch INT = 1;

PRINT '법안정보 마이그레이션 중...';
PRINT '배치 크기: ' + CAST(@BatchSize AS NVARCHAR(10)) + '건씩 처리';

-- 법안정보 마이그레이션 (배치 처리)
DECLARE @TotalRows INT;
SELECT @TotalRows = COUNT(*) FROM assembly_bills;
DECLARE @TotalBatches INT = CEILING(CAST(@TotalRows AS FLOAT) / @BatchSize);

PRINT '총 ' + CAST(@TotalBatches AS NVARCHAR(10)) + '개 배치로 처리 예정';
PRINT '';

WHILE @ProcessedCount < @TotalRows
BEGIN
    PRINT '배치 ' + CAST(@CurrentBatch AS NVARCHAR(10)) + '/' + CAST(@TotalBatches AS NVARCHAR(10)) + ' 처리 중...';
    
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
        b.PROPOSER as main_proposer,
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
    FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY BILL_ID) as rn
        FROM assembly_bills
    ) b
    WHERE b.rn > @ProcessedCount AND b.rn <= (@ProcessedCount + @BatchSize);
    
    SET @ProcessedCount = @ProcessedCount + @BatchSize;
    SET @CurrentBatch = @CurrentBatch + 1;
    
    -- 진행률 표시
    DECLARE @ProgressPercent DECIMAL(5,2) = (CAST(@ProcessedCount AS FLOAT) / @TotalRows) * 100;
    IF @ProgressPercent > 100 SET @ProgressPercent = 100;
    PRINT '   진행률: ' + CAST(@ProgressPercent AS NVARCHAR(10)) + '%';
END

-- 마이그레이션 결과 확인
DECLARE @MigratedBillCount INT;
SELECT @MigratedBillCount = COUNT(*) FROM legislative_bills;

PRINT '';
PRINT '마이그레이션 결과:';
PRINT '- 원본 법안 수: ' + CAST(@SourceBillCount AS NVARCHAR(10)) + '건';
PRINT '- 마이그레이션된 법안 수: ' + CAST(@MigratedBillCount AS NVARCHAR(10)) + '건';

-- 데이터 품질 검증
DECLARE @ValidTitleCount INT, @ValidProposerCount INT, @ValidCommitteeCount INT;
SELECT @ValidTitleCount = COUNT(*) FROM legislative_bills WHERE bill_title IS NOT NULL AND LEN(LTRIM(RTRIM(bill_title))) > 0;
SELECT @ValidProposerCount = COUNT(*) FROM legislative_bills WHERE main_proposer IS NOT NULL AND LEN(LTRIM(RTRIM(main_proposer))) > 0;
SELECT @ValidCommitteeCount = COUNT(*) FROM legislative_bills WHERE responsible_committee_name IS NOT NULL AND LEN(LTRIM(RTRIM(responsible_committee_name))) > 0;

PRINT '';
PRINT '데이터 품질 검증:';
PRINT '- 유효한 법안명: ' + CAST(@ValidTitleCount AS NVARCHAR(10)) + '건';
PRINT '- 유효한 대표발의자: ' + CAST(@ValidProposerCount AS NVARCHAR(10)) + '건';
PRINT '- 유효한 소관위원회: ' + CAST(@ValidCommitteeCount AS NVARCHAR(10)) + '건';

-- 국회 회기별 법안 분포
PRINT '';
PRINT '국회 회기별 법안 분포:';
SELECT 
    COALESCE(CAST(assembly_session_number AS NVARCHAR(10)), '회기 없음') as session_number, 
    COUNT(*) as bill_count
FROM legislative_bills 
GROUP BY assembly_session_number
ORDER BY assembly_session_number DESC;

-- 소관위원회별 법안 분포 (상위 10개)
PRINT '';
PRINT '소관위원회별 법안 분포 (상위 10개):';
SELECT TOP 10
    COALESCE(responsible_committee_name, '위원회 없음') as committee_name, 
    COUNT(*) as bill_count
FROM legislative_bills 
GROUP BY responsible_committee_name
ORDER BY COUNT(*) DESC;

-- 처리결과별 분포
PRINT '';
PRINT '법안 처리결과별 분포:';
SELECT 
    COALESCE(processing_result, '결과 없음') as result_status, 
    COUNT(*) as bill_count
FROM legislative_bills 
GROUP BY processing_result
ORDER BY COUNT(*) DESC;

-- 연도별 발의 현황 (최근 5년)
PRINT '';
PRINT '연도별 법안 발의 현황 (최근 5년):';
SELECT 
    YEAR(proposal_date) as proposal_year,
    COUNT(*) as bill_count
FROM legislative_bills 
WHERE proposal_date IS NOT NULL 
    AND proposal_date >= DATEADD(YEAR, -5, GETDATE())
GROUP BY YEAR(proposal_date)
ORDER BY proposal_year DESC;

-- 중복 법안 검사
DECLARE @DuplicateBills INT;
SELECT @DuplicateBills = COUNT(*) 
FROM (
    SELECT original_bill_system_id, COUNT(*) as cnt 
    FROM legislative_bills 
    WHERE original_bill_system_id IS NOT NULL
    GROUP BY original_bill_system_id 
    HAVING COUNT(*) > 1
) duplicates;

IF @DuplicateBills > 0
BEGIN
    PRINT '';
    PRINT '경고: 중복된 법안 ID가 ' + CAST(@DuplicateBills AS NVARCHAR(10)) + '건 발견되었습니다.';
END
ELSE
BEGIN
    PRINT '';
    PRINT '✓ 중복 법안 없음 - 데이터 정합성 확인';
END

PRINT '';
PRINT '========================================';
PRINT 'STEP 4: 법안정보 마이그레이션 완료';
PRINT '========================================';