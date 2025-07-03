# Python 스크립트 사용 가이드

## 프로젝트 개요

국회 공개API를 활용하여 법안 정보, 의원 정보, 회의록 등을 수집하고 Microsoft SQL Server 데이터베이스에 저장하는 데이터 적재 프로젝트의 Python 버전입니다. 이 프로젝트는 JavaScript 버전과 동일한 기능을 제공하며, 비동기 처리와 성능 최적화를 통해 더 효율적인 데이터 처리를 지원합니다.

## Python 스크립트별 역할

### 1. 데이터 수집 (Fetch Data)

#### `fetchAssemblyData.py` - 국회 기본 데이터 수집기

**역할**: 국회 공개API에서 의원 정보 및 법안 기본 데이터 수집

**주요 기능**:
- 4개 주요 API 엔드포인트에서 데이터 수집
- 비동기 처리로 효율적인 API 호출
- 페이지네이션 자동 처리
- 대수별/연령별 반복 처리

**API 엔드포인트**:
- ALLNAMEMBER (의원 통합 데이터)
- nprlapfmaufmqytet (의원 이력 - 대수별)
- nwvrqwxyaytdsfvhu (의원 프로필)
- nzmimeepazxkubdpn (법안 데이터 - 대수별)

**생성 파일**:
- `assembly_members_integrated.json` (의원 통합 데이터)
- `assembly_members_profile.json` (의원 프로필)
- `assembly_members_history_daesu_*.json` (대수별 의원 이력, 10-22대)
- `assembly_bills_age_*.json` (대수별 법안 데이터, 10-22대)

**실행 방법**:
```bash
python fetchAssemblyData.py
```

**특징**:
- `aiohttp`를 사용한 비동기 HTTP 요청
- 서버 부하 방지를 위한 0.1초 지연
- 자동 재시도 및 에러 처리
- 10-22대 국회 데이터 자동 순회

---

#### `filterBillsAndFetchVotes.py` - 가결 법안 필터링 및 표결 데이터 수집기

**역할**: 가결된 법안만 필터링하고 각 법안의 표결 상세 정보 수집

**주요 기능**:
- 가결 법안 필터링 ('원안가결', '수정가결')
- 17대 이후 법안만 대상
- 중복 제거 및 배치 처리
- 중간 결과 저장으로 재시작 지원

**입력**: `assembly_bills_age_*.json` 파일들

**처리 과정**:
1. 모든 법안 파일에서 가결된 법안 추출
2. BILL_ID 기준 중복 제거
3. API 호출로 표결 상세 데이터 수집
4. 배치 단위로 처리하여 서버 부하 최소화

**생성 파일**:
- `assembly_filtered_bills_passed.json` (가결 법안 목록)
- `assembly_bills_api_results.json` (표결 데이터)
- `assembly_bills_api_results_temp.json` (중간 결과)

**실행 방법**:
```bash
# 전체 실행 (필터링 + API 호출)
python filterBillsAndFetchVotes.py

# 필터링만 실행
python filterBillsAndFetchVotes.py --filter-only

# API 호출만 실행
python filterBillsAndFetchVotes.py --api-only
```

**특징**:
- 배치 크기 10으로 API 호출 제한
- 기존 결과와 중복 확인하여 불필요한 API 호출 방지
- 중간 결과 자동 저장
- 상세한 진행률 표시

---

#### `fetchConferenceData.py` - 법안별 회의 정보 수집기

**역할**: 각 가결 법안의 회의 및 위원회 정보 수집

**주요 기능**:
- VCONFBILLCONFLIST API 호출
- 법안별 회의록 메타데이터 수집
- 비동기 배치 처리

**입력**: `assembly_filtered_bills_passed.json`

**API 호출**: VCONFBILLCONFLIST API로 회의 정보 조회

**생성 파일**: `assembly_bills_conference_api_results.json`

**실행 방법**:
```bash
python fetchConferenceData.py
```

**특징**:
- 배치 크기 10으로 안정적 처리
- 2초 지연으로 서버 부하 방지
- 청크 단위 저장으로 대용량 데이터 처리

---

### 2. 파일 다운로드 (Download File)

#### `downloadConferencePdfs.py` - 회의록 PDF 문서 다운로더

**역할**: 각 회의의 PDF 회의록 파일 실제 다운로드

**주요 기능**:
- PDF 파일 실제 다운로드
- 중복 다운로드 방지
- 다운로드 진행률 추적
- 파일 존재 여부 확인

**입력**: `assembly_bills_conference_api_results.json`

**생성 구조**:
```
pdf_downloads/
├── {BILL_ID}/
│   ├── {CONF_KND}_{CONF_ID}_{ERACO}_{SESS}_{DGR}_{CONF_DT}.pdf
│   └── ...
└── pdf_tracking_list.json (추적 메타데이터)
```

**실행 방법**:
```bash
python downloadConferencePdfs.py
```

**특징**:
- User-Agent 헤더로 브라우저 모방
- 파일명 자동 정리 (특수문자 제거)
- 1초 지연으로 서버 부하 방지
- 실패한 다운로드도 추적 리스트에 기록

---

#### `createPdfTrackingList.py` - PDF 다운로드 계획 생성기

**역할**: 실제 다운로드 없이 PDF 추적 리스트만 생성하여 다운로드 계획 수립

**주요 기능**:
- 다운로드할 PDF 목록 생성
- 파일 존재 여부 확인
- 다운로드 통계 제공

**입력**: `assembly_bills_conference_api_results.json`

**생성 파일**: `pdf_tracking_list.json`

**실행 방법**:
```bash
python createPdfTrackingList.py
```

**특징**:
- 실제 다운로드 없이 계획만 수립
- 기존 파일 확인으로 중복 다운로드 방지
- 상세한 통계 정보 제공

---

#### `cleanupPdfFilenames.py` - PDF 파일명 정리기

**역할**: PDF 파일명 정규화 (6번째 언더스코어 이후 불필요한 텍스트 제거)

**주요 기능**:
- 파일명 패턴 분석 및 정리
- Dry-run 모드 지원
- 배치 처리 및 결과 추적

**대상**: `pdf_downloads/` 디렉토리의 PDF 파일들

**생성 파일**: `pdf_rename_results_*.json` (파일명 변경 작업 결과)

**실행 방법**:
```bash
# Dry-run (실제 변경 없이 테스트)
python cleanupPdfFilenames.py

# 실제 파일명 변경
python cleanupPdfFilenames.py --actual

# 파일명 변환 테스트
python cleanupPdfFilenames.py --test
```

**특징**:
- 안전한 Dry-run 모드
- 6번째 언더스코어 기준 자동 분할
- 중복 파일명 충돌 방지
- 상세한 변경 이력 저장

---

### 3. 데이터베이스 로딩 (Load Database)

#### `loadMainDataToDatabase.py` - 주요 데이터 DB 적재기

**역할**: JSON 데이터를 Microsoft SQL Server 데이터베이스에 적재

**주요 기능**:
- 테이블 자동 생성
- JSON 데이터 파싱 및 삽입
- 트랜잭션 관리

**입력**: 모든 `assembly_*.json` 파일 (의원, 법안 기본 데이터)

**생성 테이블**:
- `assembly_bills` (법안 정보)
- `assembly_members_history` (의원 이력)
- `assembly_members_history_daesu` (대수별 의원 이력)
- `assembly_members_integrated` (의원 통합 정보)
- `assembly_members_profile` (의원 프로필)

**실행 방법**:
```bash
python loadMainDataToDatabase.py
```

**특징**:
- 자동 테이블 스키마 생성
- 날짜 필드 자동 파싱
- 파일별 커밋으로 안정성 확보
- 상세한 에러 로깅

---

#### `loadVoteDataToDatabase.py` - 표결 데이터 전용 DB 적재기

**역할**: 표결 데이터를 데이터베이스에 적재 (개별 의원별 찬반 투표 결과)

**주요 기능**:
- 표결 상세 데이터 파싱
- 대용량 데이터 배치 처리
- 진행률 모니터링

**입력**: `assembly_bills_api_results.json` (표결 상세 데이터)

**생성 테이블**: `assembly_plenary_session_vote`

**실행 방법**:
```bash
python loadVoteDataToDatabase.py
```

**특징**:
- 중첩된 JSON 구조 자동 파싱
- 100건 단위 진행률 표시
- 실패한 레코드도 상세 로깅
- 전체 트랜잭션 롤백 지원

---

## JSON 파일 분류

### A. 의원 데이터 (Member Data)

| 파일명 | 설명 |
|--------|------|
| `assembly_members_integrated.json` | 의원 통합 데이터 |
| `assembly_members_profile.json` | 의원 프로필 데이터 |
| `assembly_members_history_daesu_*.json` | 대수별 의원 이력 (10-22대) |

### B. 법안 데이터 (Bill Data)

| 파일명 | 설명 |
|--------|------|
| `assembly_bills_age_*.json` | 대수별 법안 데이터 (10-22대) |
| `assembly_filtered_bills_passed.json` | 가결된 법안만 필터링 |

### C. API 결과 데이터 (API Results)

| 파일명 | 설명 |
|--------|------|
| `assembly_bills_api_results.json`, `assembly_bills_api_results_temp.json` | 표결 데이터 결과 |
| `assembly_bills_summary_fallback.json` | API 결과 요약 |
| `assembly_bills_conference_api_results.json` | 회의 데이터 결과 |

### D. PDF 관리 데이터 (PDF Management)

| 파일명 | 설명 |
|--------|------|
| `pdf_tracking_list.json` | PDF 다운로드 추적 메타데이터 |
| `pdf_rename_results_actual_*.json` | PDF 파일명 변경 작업 결과 |

### E. 설정 파일 (Configuration)

| 파일명 | 설명 |
|--------|------|
| `requirements.txt` | Python 패키지 의존성 |
| `assembly_apis.postman_collection.json` | Postman 컬렉션 |

## 주요 의존성 관계

1. **filterBillsAndFetchVotes.py** ← fetchAssemblyData.py에서 생성한 `assembly_bills_age_*.json`
2. **fetchConferenceData.py** ← filterBillsAndFetchVotes.py에서 생성한 `assembly_filtered_bills_passed.json`
3. **downloadConferencePdfs.py** ← fetchConferenceData.py에서 생성한 `assembly_bills_conference_api_results.json`
4. **loadVoteDataToDatabase.py** ← filterBillsAndFetchVotes.py에서 생성한 `assembly_bills_api_results.json`
5. **cleanupPdfFilenames.py** ← downloadConferencePdfs.py에서 생성한 PDF 파일들

## 실행 순서

```bash
# 1. 국회 기본 데이터 수집
python fetchAssemblyData.py

# 2. 가결 법안 필터링 및 표결 데이터 수집
python filterBillsAndFetchVotes.py

# 3. 법안별 회의 정보 수집
python fetchConferenceData.py

# 4. 회의록 PDF 다운로드 (선택사항)
python downloadConferencePdfs.py

# 5. PDF 파일명 정리 (선택사항)
python cleanupPdfFilenames.py --actual

# 6. 주요 데이터 DB 적재
python loadMainDataToDatabase.py

# 7. 표결 데이터 DB 적재
python loadVoteDataToDatabase.py
```

## 필수 패키지 설치

```bash
pip install aiohttp asyncio pyodbc python-dotenv
```

## 환경 설정

`.env` 파일을 프로젝트 루트에 생성하고 다음 내용을 입력하세요:

```env
DB_SERVER=your_server_name
DB_DATABASE=your_database_name
DB_USERNAME=your_username
DB_PASSWORD=your_password
API_KEY=your_assembly_api_key
```

## 주요 개선사항

### JavaScript 버전 대비 장점

1. **비동기 처리**: `aiohttp`와 `asyncio`를 활용한 효율적인 동시 처리
2. **에러 처리**: 강화된 예외 처리 및 복구 메커니즘
3. **메모리 관리**: 스트리밍 처리로 대용량 파일 효율적 처리
4. **코드 구조**: 클래스 기반 모듈화로 유지보수성 향상
5. **명령행 인터페이스**: argparse를 통한 유연한 실행 옵션
6. **타입 힌팅**: 코드 가독성 및 IDE 지원 향상

### 성능 최적화

1. **배치 처리**: API 호출을 배치 단위로 처리하여 서버 부하 최소화
2. **중복 방지**: 기존 데이터 확인으로 불필요한 작업 제거
3. **중간 저장**: 대용량 처리 시 중간 결과 저장으로 재시작 지원
4. **연결 풀링**: 데이터베이스 연결 최적화

### 안정성 강화

1. **트랜잭션 관리**: 데이터베이스 작업의 원자성 보장
2. **재시도 메커니즘**: 네트워크 오류 시 자동 재시도
3. **진행률 모니터링**: 실시간 작업 진행률 표시
4. **상세 로깅**: 디버깅을 위한 상세한 로그 기록

## 문제 해결

### 일반적인 오류

1. **모듈 설치 오류**:
   ```bash
   pip install --upgrade pip
   pip install aiohttp asyncio pyodbc python-dotenv
   ```

2. **데이터베이스 연결 오류**:
   - `.env` 파일의 데이터베이스 설정 확인
   - ODBC 드라이버 설치 확인

3. **API 호출 제한**:
   - 배치 크기 조정 (기본값: 10)
   - 지연 시간 증가

4. **메모리 부족**:
   - 청크 크기 감소
   - 중간 결과 저장 주기 단축

### 로그 확인

각 스크립트는 상세한 진행률과 오류 정보를 콘솔에 출력합니다. 오류 발생 시 해당 로그를 확인하여 문제를 진단할 수 있습니다.

## 라이선스

이 프로젝트는 원본 JavaScript 프로젝트와 동일한 라이선스를 따릅니다.