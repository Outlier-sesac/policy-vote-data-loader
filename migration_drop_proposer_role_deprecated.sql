-- ========================================
-- bill_proposer_relationships 테이블에서 proposer_role 컬럼 제거
-- ========================================

PRINT 'proposer_role 컬럼 제거 시작...';

-- 1. 기존 제약조건 확인
PRINT '기존 제약조건 확인 중...';

-- unique 제약조건 이름 확인
DECLARE @ConstraintName NVARCHAR(128);
SELECT @ConstraintName = name 
FROM sys.key_constraints 
WHERE parent_object_id = OBJECT_ID('bill_proposer_relationships') 
  AND type = 'UQ';

-- 2. unique 제약조건 삭제 (proposer_role이 포함된 경우)
IF @ConstraintName IS NOT NULL
BEGIN
    PRINT '기존 UNIQUE 제약조건 삭제 중: ' + @ConstraintName;
    EXEC('ALTER TABLE bill_proposer_relationships DROP CONSTRAINT ' + @ConstraintName);
END

-- 3. 새로운 unique 제약조건 생성 (bill_id, member_id만 사용)
PRINT '새로운 UNIQUE 제약조건 생성 중...';
ALTER TABLE bill_proposer_relationships 
ADD CONSTRAINT UQ_bill_proposer_relationships_bill_member 
UNIQUE (bill_id, member_id);

-- 4. proposer_role 컬럼 삭제
PRINT 'proposer_role 컬럼 삭제 중...';
ALTER TABLE bill_proposer_relationships 
DROP COLUMN proposer_role;

-- 5. 결과 확인
PRINT '';
PRINT '=== 컬럼 삭제 완료 ===';

-- 테이블 구조 확인
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'bill_proposer_relationships'
ORDER BY ORDINAL_POSITION;

PRINT 'proposer_role 컬럼 제거 완료!';