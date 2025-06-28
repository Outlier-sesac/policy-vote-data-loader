-- ========================================
-- 한국 국회 데이터베이스 - 트리거, 저장프로시저 테스트 스크립트
-- 실행 시간: 약 2-3분 예상
-- 주의: 이 스크립트는 생성 없이 테스트만 수행합니다
-- ========================================

-- USE [database_name]; -- 실제 데이터베이스 이름으로 변경 필요
-- GO -- 필요시 주석 해제

PRINT '========================================';
PRINT 'Trigger and Stored Procedure Test Start';
PRINT 'Existing objects must be created';
PRINT '========================================';

-- ========================================
-- 1단계: 트리거 테스트
-- ========================================
PRINT '';
PRINT 'Step 1: Trigger Test Start';
PRINT '========================================';

-- 트리거 테스트 1: 국회의원 정보 변경 감사
PRINT 'Test 1: Member Audit Trigger';
BEGIN TRY
    -- 테스트용 의원 정보 삽입
    INSERT INTO national_assembly_members (korean_name, mona_system_code) 
    VALUES ('테스트의원', 'TEST001');
    
    DECLARE @TestMemberId INT = SCOPE_IDENTITY();
    
    -- 정보 업데이트 (트리거 실행)
    UPDATE national_assembly_members 
    SET phone_number = '02-1234-5678' 
    WHERE member_id = @TestMemberId;
    
    -- 테스트 데이터 정리
    DELETE FROM national_assembly_members WHERE member_id = @TestMemberId;
    
    PRINT 'OK Member Audit Trigger Normal Operation';
END TRY
BEGIN CATCH
    PRINT 'ERROR Member Audit Trigger: ' + ERROR_MESSAGE();
END CATCH

-- 트리거 테스트 2: 투표 기록 검증
PRINT '';
PRINT 'Test 2: Voting Record Validation Trigger';
BEGIN TRY
    -- 유효한 투표 기록 테스트
    DECLARE @ValidMemberId INT;
    SELECT TOP 1 @ValidMemberId = member_id FROM national_assembly_members;
    
    IF @ValidMemberId IS NOT NULL
    BEGIN
        INSERT INTO plenary_voting_records (member_id, vote_decision, assembly_session_number)
        VALUES (@ValidMemberId, '찬성', 22);
        
        DECLARE @TestVoteId INT = SCOPE_IDENTITY();
        
        -- 테스트 데이터 정리
        DELETE FROM plenary_voting_records WHERE vote_record_id = @TestVoteId;
        
        PRINT 'OK Valid Vote Record Insert Success';
    END
    
    -- 무효한 투표 기록 테스트 (오류 발생 예상)
    BEGIN TRY
        INSERT INTO plenary_voting_records (member_id, vote_decision, assembly_session_number)
        VALUES (@ValidMemberId, '잘못된값', 22);
        
        PRINT 'ERROR Invalid Vote Record Inserted (Trigger Error)';
    END TRY
    BEGIN CATCH
        PRINT 'OK Invalid Vote Record Blocked: ' + ERROR_MESSAGE();
    END CATCH
    
END TRY
BEGIN CATCH
    PRINT 'ERROR Vote Validation Trigger Test: ' + ERROR_MESSAGE();
END CATCH

-- ========================================
-- 2단계: 저장 프로시저 테스트
-- ========================================
PRINT '';
PRINT 'Step 2: Stored Procedure Test Start';
PRINT '========================================';

-- 저장 프로시저 테스트 1: 현재 국회의원 조회
PRINT 'Test 1: Get Current Assembly Members Procedure';
BEGIN TRY
    DECLARE @MemberCount INT;
    
    -- 전체 의원 조회
    EXEC SP_GetCurrentAssemblyMembers;
    
    -- 특정 정당 의원 조회 (상위 정당 하나 선택)
    DECLARE @TestParty NVARCHAR(100);
    SELECT TOP 1 @TestParty = political_party_name 
    FROM current_national_assembly_members 
    WHERE political_party_name IS NOT NULL
    GROUP BY political_party_name
    ORDER BY COUNT(*) DESC;
    
    IF @TestParty IS NOT NULL
    BEGIN
        PRINT 'Party Query Test: ' + @TestParty;
        EXEC SP_GetCurrentAssemblyMembers @PoliticalPartyName = @TestParty;
    END
    
    PRINT 'OK Get Current Assembly Members Procedure Normal';
END TRY
BEGIN CATCH
    PRINT 'ERROR Get Current Assembly Members Procedure: ' + ERROR_MESSAGE();
END CATCH

-- 저장 프로시저 테스트 2: 법안별 투표 결과 조회
PRINT '';
PRINT 'Test 2: Get Bill Voting Results Procedure';
BEGIN TRY
    DECLARE @TestBillId INT;
    SELECT TOP 1 @TestBillId = bill_id 
    FROM legislative_bills 
    WHERE bill_id IN (SELECT DISTINCT bill_id FROM plenary_voting_records WHERE bill_id IS NOT NULL);
    
    IF @TestBillId IS NOT NULL
    BEGIN
        PRINT 'Test Bill ID: ' + CAST(@TestBillId AS NVARCHAR(10));
        EXEC SP_GetBillVotingResults @BillId = @TestBillId;
        PRINT 'OK Get Bill Voting Results Procedure Normal';
    END
    ELSE
    BEGIN
        PRINT 'No bills to test.';
    END
END TRY
BEGIN CATCH
    PRINT 'ERROR Get Bill Voting Results Procedure: ' + ERROR_MESSAGE();
END CATCH

-- 저장 프로시저 테스트 3: 의원별 투표 이력 조회
PRINT '';
PRINT 'Test 3: Get Member Voting History Procedure';
BEGIN TRY
    DECLARE @TestMemberIdForVoting INT;
    SELECT TOP 1 @TestMemberIdForVoting = member_id 
    FROM plenary_voting_records 
    GROUP BY member_id
    ORDER BY COUNT(*) DESC;
    
    IF @TestMemberIdForVoting IS NOT NULL
    BEGIN
        PRINT 'Test Member ID: ' + CAST(@TestMemberIdForVoting AS NVARCHAR(10));
        EXEC SP_GetMemberVotingHistory @MemberId = @TestMemberIdForVoting;
        PRINT 'OK Get Member Voting History Procedure Normal';
    END
    ELSE
    BEGIN
        PRINT 'No voting history to test.';
    END
END TRY
BEGIN CATCH
    PRINT 'ERROR Get Member Voting History Procedure: ' + ERROR_MESSAGE();
END CATCH

-- 저장 프로시저 테스트 4: 정당별 투표 통계
PRINT '';
PRINT 'Test 4: Get Party Voting Statistics Procedure';
BEGIN TRY
    DECLARE @TestSessionNumber INT;
    SELECT TOP 1 @TestSessionNumber = assembly_session_number 
    FROM plenary_voting_records 
    WHERE assembly_session_number IS NOT NULL
    GROUP BY assembly_session_number
    ORDER BY COUNT(*) DESC;
    
    IF @TestSessionNumber IS NOT NULL
    BEGIN
        PRINT 'Test Assembly Session: ' + CAST(@TestSessionNumber AS NVARCHAR(10));
        EXEC SP_GetPartyVotingStatistics @AssemblySessionNumber = @TestSessionNumber;
        PRINT 'OK Get Party Voting Statistics Procedure Normal';
    END
    ELSE
    BEGIN
        PRINT 'No voting statistics to test.';
    END
END TRY
BEGIN CATCH
    PRINT 'ERROR Get Party Voting Statistics Procedure: ' + ERROR_MESSAGE();
END CATCH

-- ========================================
-- 3단계: 사용자 정의 함수 테스트
-- ========================================
PRINT '';
PRINT 'Step 3: User-Defined Function Test Start';
PRINT '========================================';

-- 함수 테스트 1: 의원 나이 계산
PRINT 'Test 1: Member Age Calculation Function';
BEGIN TRY
    DECLARE @TestBirthDate NVARCHAR(20);
    SELECT TOP 1 @TestBirthDate = birth_date 
    FROM national_assembly_members 
    WHERE birth_date IS NOT NULL AND LEN(birth_date) >= 8;
    
    IF @TestBirthDate IS NOT NULL
    BEGIN
        DECLARE @CalculatedAge INT = dbo.FN_CalculateMemberAge(@TestBirthDate);
        PRINT 'Test Birth Date: ' + @TestBirthDate;
        PRINT 'Calculated Age: ' + CAST(@CalculatedAge AS NVARCHAR(10)) + ' years';
        PRINT 'OK Member Age Calculation Function Normal';
    END
    ELSE
    BEGIN
        PRINT 'No birth date data to test.';
    END
END TRY
BEGIN CATCH
    PRINT 'ERROR Member Age Calculation Function: ' + ERROR_MESSAGE();
END CATCH

-- 함수 테스트 2: 투표 참여율 계산 (current_session_code 고려)
PRINT '';
PRINT 'Test 2: Voting Participation Rate Function (with current_session_code)';
BEGIN TRY
    DECLARE @TestMemberForRate INT, @TestSessionForRate INT, @TestCurrentSessionCode INT;
    SELECT TOP 1 @TestMemberForRate = member_id, @TestSessionForRate = assembly_session_number, @TestCurrentSessionCode = current_session_code
    FROM plenary_voting_records 
    WHERE assembly_session_number IS NOT NULL AND current_session_code IS NOT NULL
    GROUP BY member_id, assembly_session_number, current_session_code
    ORDER BY COUNT(*) DESC;
    
    IF @TestMemberForRate IS NOT NULL AND @TestSessionForRate IS NOT NULL
    BEGIN
        -- 기존 함수 테스트 (current_session_code 없이)
        DECLARE @ParticipationRate DECIMAL(5,2) = dbo.FN_CalculateVotingParticipationRate(@TestMemberForRate, @TestSessionForRate, NULL);
        PRINT 'Test Member ID: ' + CAST(@TestMemberForRate AS NVARCHAR(10));
        PRINT 'Test Assembly Session: ' + CAST(@TestSessionForRate AS NVARCHAR(10));
        PRINT 'Participation Rate (all sessions): ' + CAST(@ParticipationRate AS NVARCHAR(10)) + '%';
        
        -- 새로운 함수 테스트 (current_session_code 포함)
        IF @TestCurrentSessionCode IS NOT NULL
        BEGIN
            DECLARE @SessionParticipationRate DECIMAL(5,2) = dbo.FN_CalculateVotingParticipationRate(@TestMemberForRate, @TestSessionForRate, @TestCurrentSessionCode);
            PRINT 'Test Current Session Code: ' + CAST(@TestCurrentSessionCode AS NVARCHAR(10));
            PRINT 'Participation Rate (specific session): ' + CAST(@SessionParticipationRate AS NVARCHAR(10)) + '%';
        END
        
        PRINT 'OK Voting Participation Rate Function Normal';
    END
    ELSE
    BEGIN
        PRINT 'No voting participation rate data to test.';
    END
END TRY
BEGIN CATCH
    PRINT 'ERROR Voting Participation Rate Function: ' + ERROR_MESSAGE();
END CATCH

-- ========================================
-- 최종 결과 요약
-- ========================================
PRINT '';
PRINT '========================================';
PRINT 'Trigger and Stored Procedure Test Complete';
PRINT '========================================';
PRINT '';
PRINT 'Tested Objects:';
PRINT 'OK Triggers: 2';
PRINT '  - TR_national_assembly_members_audit';
PRINT '  - TR_plenary_voting_records_validation';
PRINT '';
PRINT 'OK Stored Procedures: 4';
PRINT '  - SP_GetCurrentAssemblyMembers';
PRINT '  - SP_GetBillVotingResults';
PRINT '  - SP_GetMemberVotingHistory';
PRINT '  - SP_GetPartyVotingStatistics';
PRINT '';
PRINT 'OK User-Defined Functions: 2';
PRINT '  - FN_CalculateMemberAge';
PRINT '  - FN_CalculateVotingParticipationRate';
PRINT '';
PRINT 'All tests completed.';
PRINT '========================================';