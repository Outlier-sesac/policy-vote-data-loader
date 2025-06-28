-- ========================================
-- 한국 국회 데이터베이스 정규화 - STEP 6: 인덱스 생성, 뷰 생성, 최종화
-- 실행 시간: 약 10-20분 예상
-- ========================================

USE [database_name]; -- 실제 데이터베이스 이름으로 변경 필요
-- GO -- 필요시 주석 해제

PRINT '========================================';
PRINT 'STEP 6: 인덱스 생성, 뷰 생성, 최종화 시작';
PRINT '========================================';

-- ========================================
-- 1단계: 성능 최적화 인덱스 생성
-- ========================================
PRINT '1단계: 성능 최적화 인덱스 생성 중...';

-- 국회의원 테이블 인덱스
PRINT '- 국회의원 테이블 인덱스 생성 중...';
CREATE INDEX IX_members_mona_code ON national_assembly_members(mona_system_code);
CREATE INDEX IX_members_naas_code ON national_assembly_members(naas_system_code);
CREATE INDEX IX_members_korean_name ON national_assembly_members(korean_name);

-- 복합 인덱스 (자주 함께 조회되는 컬럼들)
PRINT '- 복합 인덱스 생성 중...';
CREATE INDEX IX_terms_party_district ON current_national_assembly_members(political_party_name, electoral_district_name, assembly_session_number);
CREATE INDEX IX_bills_committee_session ON legislative_bills(responsible_committee_name, assembly_session_number, proposal_date);

-- 투표 기록 인덱스 (날짜 기반 검색 최적화)
PRINT '- 투표 기록 인덱스 생성 중...';
CREATE INDEX IX_votes_date_session ON plenary_voting_records(voting_date, session_code);
CREATE INDEX IX_votes_member_date ON plenary_voting_records(member_id, voting_date);
CREATE INDEX IX_votes_bill_decision ON plenary_voting_records(bill_id, vote_decision);

PRINT '✓ 성능 최적화 인덱스 생성 완료';

-- ========================================
-- 2단계: 자주 사용되는 뷰 생성
-- ========================================
PRINT '';
PRINT '2단계: 자주 사용되는 뷰 생성 중...';

-- 현재 국회의원 정보 뷰 (Current Assembly Members View)
PRINT '- 현재 국회의원 정보 뷰 생성 중...';
GO
CREATE VIEW current_assembly_members_view AS
SELECT 
    m.member_id,
    m.mona_system_code,
    m.korean_name,
    m.chinese_name,
    m.english_name,
    m.birth_date,
    m.gender,
    m.phone_number,
    m.email_address,
    t.political_party_name,
    t.electoral_district_name,
    t.primary_committee_name,
    t.assembly_session_number,
    t.election_district_type
FROM national_assembly_members m
JOIN current_national_assembly_members t ON m.member_id = t.member_id;

-- 법안 상세 정보 뷰 (Bill Details View)
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
    COUNT(bp.member_id) as total_proposer_count,
    STRING_AGG(m.korean_name, ', ') as proposer_names_list
FROM legislative_bills b
LEFT JOIN bill_proposer_relationships bp ON b.bill_id = bp.bill_id
LEFT JOIN national_assembly_members m ON bp.member_id = m.member_id
GROUP BY b.bill_id, b.original_bill_system_id, b.bill_number, b.bill_title, 
         b.responsible_committee_name, b.assembly_session_number, b.proposal_date, b.processing_result;

-- 투표 통계 뷰 (Voting Statistics View)
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

GO
PRINT '✓ 뷰 생성 완료';

-- ========================================
-- 3단계: 통계 정보 업데이트
-- ========================================
PRINT '';
PRINT '3단계: 통계 정보 업데이트 중...';

UPDATE STATISTICS national_assembly_members;
UPDATE STATISTICS current_national_assembly_members;
UPDATE STATISTICS legislative_bills;
UPDATE STATISTICS bill_proposer_relationships;
UPDATE STATISTICS plenary_voting_records;

PRINT '✓ 통계 정보 업데이트 완료';

-- ========================================
-- 4단계: 데이터 무결성 최종 검증
-- ========================================
PRINT '';
PRINT '4단계: 데이터 무결성 최종 검증 중...';

-- 테이블별 레코드 수 확인
DECLARE @MemberCount INT, @TermCount INT, @BillCount INT, @ProposerCount INT, @VoteCount INT;
SELECT @MemberCount = COUNT(*) FROM national_assembly_members;
SELECT @TermCount = COUNT(*) FROM current_national_assembly_members;
SELECT @BillCount = COUNT(*) FROM legislative_bills;
SELECT @ProposerCount = COUNT(*) FROM bill_proposer_relationships;
SELECT @VoteCount = COUNT(*) FROM plenary_voting_records;

PRINT '';
PRINT '최종 테이블별 레코드 수:';
PRINT '- national_assembly_members: ' + CAST(@MemberCount AS NVARCHAR(10)) + '명';
PRINT '- current_national_assembly_members: ' + CAST(@TermCount AS NVARCHAR(10)) + '건';
PRINT '- legislative_bills: ' + CAST(@BillCount AS NVARCHAR(10)) + '건';
PRINT '- bill_proposer_relationships: ' + CAST(@ProposerCount AS NVARCHAR(10)) + '건';
PRINT '- plenary_voting_records: ' + CAST(@VoteCount AS NVARCHAR(10)) + '건';

-- 외래키 제약조건 검증
PRINT '';
PRINT '외래키 제약조건 검증:';

DECLARE @InvalidTerms INT, @InvalidProposers INT, @InvalidVotesMembers INT, @InvalidVotesBills INT;

SELECT @InvalidTerms = COUNT(*) 
FROM current_national_assembly_members t
LEFT JOIN national_assembly_members m ON t.member_id = m.member_id
WHERE m.member_id IS NULL;

SELECT @InvalidProposers = COUNT(*) 
FROM bill_proposer_relationships p
LEFT JOIN legislative_bills b ON p.bill_id = b.bill_id
LEFT JOIN national_assembly_members m ON p.member_id = m.member_id
WHERE b.bill_id IS NULL OR m.member_id IS NULL;

SELECT @InvalidVotesMembers = COUNT(*) 
FROM plenary_voting_records v
LEFT JOIN national_assembly_members m ON v.member_id = m.member_id
WHERE m.member_id IS NULL;

SELECT @InvalidVotesBills = COUNT(*) 
FROM plenary_voting_records v
LEFT JOIN legislative_bills b ON v.bill_id = b.bill_id
WHERE v.bill_id IS NOT NULL AND b.bill_id IS NULL;

IF @InvalidTerms = 0 AND @InvalidProposers = 0 AND @InvalidVotesMembers = 0 AND @InvalidVotesBills = 0
BEGIN
    PRINT '✓ 모든 외래키 제약조건 검증 통과';
END
ELSE
BEGIN
    PRINT '외래키 제약조건 위반 발견:';
    IF @InvalidTerms > 0 PRINT '- 유효하지 않은 임기 정보: ' + CAST(@InvalidTerms AS NVARCHAR(10)) + '건';
    IF @InvalidProposers > 0 PRINT '- 유효하지 않은 발의자 관계: ' + CAST(@InvalidProposers AS NVARCHAR(10)) + '건';
    IF @InvalidVotesMembers > 0 PRINT '- 유효하지 않은 투표 의원: ' + CAST(@InvalidVotesMembers AS NVARCHAR(10)) + '건';
    IF @InvalidVotesBills > 0 PRINT '- 유효하지 않은 투표 법안: ' + CAST(@InvalidVotesBills AS NVARCHAR(10)) + '건';
END

-- ========================================
-- 5단계: 성능 테스트 쿼리 실행
-- ========================================
PRINT '';
PRINT '5단계: 성능 테스트 쿼리 실행 중...';

-- 테스트 쿼리 1: 현재 국회의원 조회
DECLARE @TestStart DATETIME2 = GETDATE();
SELECT COUNT(*) FROM current_assembly_members_view;
DECLARE @TestEnd DATETIME2 = GETDATE();
DECLARE @TestDuration INT = DATEDIFF(MILLISECOND, @TestStart, @TestEnd);
PRINT '- 현재 국회의원 조회 성능: ' + CAST(@TestDuration AS NVARCHAR(10)) + 'ms';

-- 테스트 쿼리 2: 법안 상세 정보 조회
SET @TestStart = GETDATE();
SELECT COUNT(*) FROM legislative_bills_detail_view;
SET @TestEnd = GETDATE();
SET @TestDuration = DATEDIFF(MILLISECOND, @TestStart, @TestEnd);
PRINT '- 법안 상세 정보 조회 성능: ' + CAST(@TestDuration AS NVARCHAR(10)) + 'ms';

-- 테스트 쿼리 3: 투표 통계 조회
SET @TestStart = GETDATE();
SELECT COUNT(*) FROM voting_statistics_view;
SET @TestEnd = GETDATE();
SET @TestDuration = DATEDIFF(MILLISECOND, @TestStart, @TestEnd);
PRINT '- 투표 통계 조회 성능: ' + CAST(@TestDuration AS NVARCHAR(10)) + 'ms';

-- ========================================
-- 최종 완료 메시지
-- ========================================
PRINT '';
PRINT '========================================';
PRINT '한국 국회 데이터베이스 정규화 마이그레이션 완료!';
PRINT '========================================';
PRINT '';
PRINT '✓ 생성된 테이블: 5개';
PRINT '✓ 생성된 뷰: 3개';
PRINT '✓ 생성된 인덱스: 8개';
PRINT '✓ 마이그레이션된 의원: ' + CAST(@MemberCount AS NVARCHAR(10)) + '명';
PRINT '✓ 마이그레이션된 법안: ' + CAST(@BillCount AS NVARCHAR(10)) + '건';
PRINT '✓ 마이그레이션된 투표기록: ' + CAST(@VoteCount AS NVARCHAR(10)) + '건';
PRINT '';
PRINT '다음 단계:';
PRINT '1. migration_test_procedures_triggers.sql 실행';
PRINT '2. 애플리케이션 코드 업데이트';
PRINT '3. 기존 테이블 백업 후 정리';
PRINT '========================================';