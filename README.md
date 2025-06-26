# 데이터 파일 관계도 및 처리 흐름

## 프로젝트 개요

국회 공개API를 활용하여 법안 정보, 의원 정보, 회의록 등을 수집하고 Microsoft SQL Server 데이터베이스에 저장하는 데이터 적재 프로젝트입니다.

## JavaScript 스크립트별 역할

### 1. 데이터 수집 (Fetch Data)

#### `fetchAssemblyData.js` - 국회 기본 데이터 수집기

- **역할**: 국회 공개API에서 의원 정보 및 법안 기본 데이터 수집
- **API 호출**: 4개 주요 API 엔드포인트 (의원 통합, 의원 이력, 의원 프로필, 법안 데이터)
- **생성 파일**:
  - `assembly_members_integrated.json` (의원 통합 데이터)
  - `assembly_members_profile.json` (의원 프로필)
  - `assembly_members_history_daesu_*.json` (대수별 의원 이력, 10-22대)
  - `assembly_bills_age_*.json` (대수별 법안 데이터, 10-22대)

#### `filterBillsAndFetchVotes.js` - 가결 법안 필터링 및 표결 데이터 수집기

- **입력**: `assembly_bills_age_*.json` (13개 파일)
- **처리**: 가결된 법안만 필터링 ('원안가결', '수정가결') 및 17대 이후만 대상
- **API 호출**: 각 법안의 표결 상세 정보 수집
- **생성 파일**:
  - `assembly_filtered_bills_passed.json` (가결 법안)
  - `assembly_bills_api_results.json` (표결 데이터)

#### `fetchConferenceData.js` - 법안별 회의 정보 수집기

- **입력**: `assembly_filtered_bills_passed.json`
- **역할**: 각 가결 법안의 회의 및 위원회 정보 수집
- **API 호출**: VCONFBILLCONFLIST API로 회의 정보 조회
- **생성 파일**: `assembly_bills_conference_api_results.json`

### 2. 파일 다운로드 (Download File)

#### `downloadConferencePdfs.js` - 회의록 PDF 문서 다운로더

- **입력**: `assembly_bills_conference_api_results.json`
- **역할**: 각 회의의 PDF 회의록 파일 실제 다운로드
- **기능**: 중복 다운로드 방지, 다운로드 진행률 추적
- **생성 파일**:
  - `pdf_tracking_list.json` (다운로드 추적 메타데이터)
  - `pdf_downloads/{BILL_ID}` 디렉토리 내 PDF 파일들

#### `createPdfTrackingList.js` - PDF 다운로드 계획 생성기

- **입력**: `assembly_bills_conference_api_results.json`
- **역할**: 실제 다운로드 없이 PDF 추적 리스트만 생성하여 다운로드 계획 수립
- **생성 파일**: `pdf_tracking_list.json`

#### `cleanupPdfFilenames.js` - PDF 파일명 정리기 (deprecated)

- **입력**: `pdf_downloads/` 디렉토리의 PDF 파일들
- **역할**: 파일명 정규화 (6번째 언더스코어 이후 불필요한 텍스트 제거)
- **생성 파일**: `pdf_rename_results_actual_*.json` (파일명 변경 작업 결과)

### 3. 데이터베이스 로딩 (Load Database)

#### `loadMainDataToDatabase.js` - 주요 데이터 DB 적재기

- **입력**: 모든 `assembly_*.json` 파일 (의원, 법안 기본 데이터)
- **역할**: JSON 데이터를 Microsoft SQL Server 데이터베이스에 적재
- **생성 테이블**: `assembly_bills`, `assembly_members_history`, `assembly_members_integrated`

#### `loadVoteDataToDatabase.js` - 표결 데이터 전용 DB 적재기

- **입력**: `assembly_bills_api_results.json` (표결 상세 데이터)
- **역할**: 표결 데이터를 데이터베이스에 적재 (개별 의원별 찬반 투표 결과)
- **생성 테이블**: `assembly_plenary_session_vote`

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
| `package.json` | Node.js 프로젝트 설정 |
| `assembly_apis.postman_collection.json` | Postman 컬렉션 |

## 주요 의존성 관계

1. **filterBillsAndFetchVotes.js** ← fetchAssemblyData.js에서 생성한 `assembly_bills_age_*.json`
2. **fetchConferenceData.js** ← filterBillsAndFetchVotes.js에서 생성한 `assembly_filtered_bills_passed.json`
3. **downloadConferencePdfs.js** ← fetchConferenceData.js에서 생성한 `assembly_bills_conference_api_results.json`
4. **loadVoteDataToDatabase.js** ← filterBillsAndFetchVotes.js에서 생성한 `assembly_bills_api_results.json`
5. **cleanupPdfFilenames.js** ← downloadConferencePdfs.js에서 생성한 PDF 파일들

## 실행 순서

1. `node fetchAssemblyData.js` - 국회 기본 데이터 수집 (의원 정보 및 법안 기본 데이터)
2. `node filterBillsAndFetchVotes.js` - 가결 법안 필터링 및 표결 데이터 수집
3. `node fetchConferenceData.js` - 법안별 회의 정보 수집
4. `node downloadConferencePdfs.js` - 회의록 PDF 다운로드
5. `node cleanupPdfFilenames.js` - PDF 파일명 정리 (deprecated)
6. `node loadMainDataToDatabase.js` - 주요 데이터 DB 적재
7. `node loadVoteDataToDatabase.js` - 표결 데이터 DB 적재
