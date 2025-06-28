-- ========================================
-- 한국 국회 데이터베이스 정규화 - STEP 3: 현재 국회의원 정보 마이그레이션
-- 실행 시간: 약 2-5분 예상
-- ========================================

USE [database_name]; -- 실제 데이터베이스 이름으로 변경 필요
-- GO -- 필요시 주석 해제

PRINT '========================================';
PRINT 'STEP 3: 현재 국회의원 정보 마이그레이션 시작';
PRINT '========================================';

-- 기존 데이터 확인
DECLARE @SourceProfileCount INT, @MemberCount INT;
SELECT @SourceProfileCount = COUNT(*) FROM assembly_members_profile WHERE MONA_CD IS NOT NULL;
SELECT @MemberCount = COUNT(*) FROM national_assembly_members;

PRINT '원본 데이터 현황:';
PRINT '- assembly_members_profile: ' + CAST(@SourceProfileCount AS NVARCHAR(10)) + '건';
PRINT '- national_assembly_members: ' + CAST(@MemberCount AS NVARCHAR(10)) + '명';
PRINT '';

-- 현재 국회의원 정보 마이그레이션 (Profile 테이블 사용)
PRINT '현재 국회의원 임기 정보 마이그레이션 중...';

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

-- 마이그레이션 결과 확인
DECLARE @MigratedTermCount INT;
SELECT @MigratedTermCount = COUNT(*) FROM current_national_assembly_members;

PRINT '';
PRINT '마이그레이션 결과:';
PRINT '- 마이그레이션된 임기 정보: ' + CAST(@MigratedTermCount AS NVARCHAR(10)) + '건';

-- 데이터 품질 검증
DECLARE @ValidPartyCount INT, @ValidDistrictCount INT, @ValidCommitteeCount INT;
SELECT @ValidPartyCount = COUNT(*) FROM current_national_assembly_members WHERE political_party_name IS NOT NULL AND political_party_name != '';
SELECT @ValidDistrictCount = COUNT(*) FROM current_national_assembly_members WHERE electoral_district_name IS NOT NULL AND electoral_district_name != '';
SELECT @ValidCommitteeCount = COUNT(*) FROM current_national_assembly_members WHERE primary_committee_name IS NOT NULL AND primary_committee_name != '';

PRINT '';
PRINT '데이터 품질 검증:';
PRINT '- 유효한 정당명: ' + CAST(@ValidPartyCount AS NVARCHAR(10)) + '건';
PRINT '- 유효한 선거구명: ' + CAST(@ValidDistrictCount AS NVARCHAR(10)) + '건';
PRINT '- 유효한 위원회명: ' + CAST(@ValidCommitteeCount AS NVARCHAR(10)) + '건';

-- 정당별 분포 확인
PRINT '';
PRINT '정당별 의원 분포:';
SELECT 
    COALESCE(political_party_name, '정당명 없음') as party_name, 
    COUNT(*) as member_count
FROM current_national_assembly_members 
GROUP BY political_party_name
ORDER BY COUNT(*) DESC;

-- 선거구 유형별 분포 확인
PRINT '';
PRINT '선거구 유형별 분포:';
SELECT 
    COALESCE(election_district_type, '구분 없음') as district_type, 
    COUNT(*) as member_count
FROM current_national_assembly_members 
GROUP BY election_district_type
ORDER BY COUNT(*) DESC;

-- 당선 횟수별 분포 확인
PRINT '';
PRINT '당선 횟수별 분포:';
SELECT 
    COALESCE(reelection_count, '횟수 없음') as reelection_info, 
    COUNT(*) as member_count
FROM current_national_assembly_members 
GROUP BY reelection_count
ORDER BY COUNT(*) DESC;

-- 외래키 제약조건 검증
DECLARE @OrphanedRecords INT;
SELECT @OrphanedRecords = COUNT(*) 
FROM current_national_assembly_members c
LEFT JOIN national_assembly_members m ON c.member_id = m.member_id
WHERE m.member_id IS NULL;

IF @OrphanedRecords > 0
BEGIN
    PRINT '';
    PRINT '경고: 참조 무결성 오류 - ' + CAST(@OrphanedRecords AS NVARCHAR(10)) + '건의 orphaned 레코드가 발견되었습니다.';
END
ELSE
BEGIN
    PRINT '';
    PRINT '✓ 참조 무결성 검증 통과';
END

PRINT '';
PRINT '========================================';
PRINT 'STEP 3: 현재 국회의원 정보 마이그레이션 완료';
PRINT '========================================';