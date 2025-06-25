# 대한민국 국회의원 관련 Open API 문서

이 문서는 대한민국 열린국회정보에서 제공하는 국회의원 관련 주요 5개 Open API 명세입니다.

HTTP Method는 모두 GET 을 사용합니다.

코드 실행 전 먼저 `.env` 파일을 추가해주세요.

```
DB_USERNAME=
DB_PASSWORD=
DB_SERVER=
DB_DATABASE=
API_KEY=
```

---

## 1. 국회의원 정보 통합 API

### 요청 URL

```
https://open.assembly.go.kr/portal/openapi/ALLNAMEMBER
```

### 필수 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| KEY | string (필수) | 인증키 |
| Type | string (필수) | 호출 문서 형식 (xml/json) |
| pIndex | integer (필수) | 페이지 위치 |
| pSize | integer (필수) | 페이지 당 요청 수 |

### 선택 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| NAAS_CD | string | 국회의원 코드 |
| NAAS_NM | string | 국회의원명 |
| PLPT_NM | string | 정당명 |
| BLNG_CMIT_NM | string | 소속 위원회명 |

### 출력값

| 출력명 | 출력 설명 |
|--------|------------|
| NAAS_CD | 국회의원 코드 |
| NAAS_NM | 국회의원명 |
| NAAS_CH_NM | 국회의원 한자명 |
| NAAS_EN_NM | 국회의원 영문명 |
| BIRDY_DIV_CD | 생일 구분 코드 |
| BIRDY_DT | 생일 일자 |
| DTY_NM | 직책명 |
| PLPT_NM | 정당명 |
| ELECD_NM | 선거구명 |
| ELECD_DIV_NM | 선거구 구분명 |
| CMIT_NM | 위원회명 |
| BLNG_CMIT_NM | 소속 위원회명 |
| RLCT_DIV_NM | 재선 구분명 |
| GTELT_ERACO | 당선 대수 |
| NTR_DIV | 성별 |
| NAAS_TEL_NO | 전화번호 |
| NAAS_EMAIL_ADDR | 이메일 주소 |
| NAAS_HP_URL | 홈페이지 주소 |
| AIDE_NM | 보좌관 |
| CHF_SCRT_NM | 비서관 |
| SCRT_NM | 비서 |
| BRF_HST | 약력 |
| OFFM_RNUM_NO | 사무실 호실 |
| NAAS_PIC | 의원 사진 |

---

## 2. 역대 국회의원 현황 API

### 요청 URL

```
https://open.assembly.go.kr/portal/openapi/nprlapfmaufmqytet
```

### 필수 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| KEY | string (필수) | 인증키 |
| Type | string (필수) | 호출 문서 형식 |
| pIndex | integer (필수) | 페이지 위치 |
| pSize | integer (필수) | 페이지 당 요청 수 |
| DAESU | string (필수) | 대수 |

### 선택 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| DAE | string | 대별 및 소속정당 |
| DAE_NM | string | 대별 |
| NAME | string | 이름 |
| BIRTH | string | 생년월일 |
| BON | string | 본관 |
| POSI | string | 출생지 |

### 출력값

| 출력명 | 출력 설명 |
|--------|------------|
| DAESU | 대수 |
| DAE | 대별 및 소속정당 |
| DAE_NM | 대별 |
| NAME | 이름 |
| NAME_HAN | 한자 이름 |
| JA | 자 |
| HO | 호 |
| BIRTH | 생년월일 |
| BON | 본관 |
| POSI | 출생지 |
| HAK | 학력 및 경력 |
| HOBBY | 종교 및 취미 |
| BOOK | 저서 |
| SANG | 상훈 |
| DEAD | 사망일 |

---

## 3. 국회의원 인적사항 API

### 요청 URL

```
https://open.assembly.go.kr/portal/openapi/nwvrqwxyaytdsfvhu
```

### 필수 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| KEY | string (필수) | 인증키 |
| Type | string (필수) | 호출 문서 형식 |
| pIndex | integer (필수) | 페이지 위치 |
| pSize | integer (필수) | 페이지 당 요청 수 |

### 선택 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| HG_NM | string | 이름 |
| POLY_NM | string | 정당명 |
| ORIG_NM | string | 선거구 |
| CMITS | string | 위원회 목록 |
| SEX_GBN_NM | string | 성별 |
| MONA_CD | string | 의원 코드 |

### 출력값

| 출력명 | 출력 설명 |
|--------|------------|
| HG_NM | 이름 |
| HJ_NM | 한자명 |
| ENG_NM | 영문명 |
| BTH_GBN_NM | 음/양력 구분 |
| BTH_DATE | 생년월일 |
| JOB_RES_NM | 직책명 |
| POLY_NM | 정당명 |
| ORIG_NM | 선거구 |
| ELECT_GBN_NM | 선거구 구분 |
| CMIT_NM | 대표 위원회 |
| CMITS | 위원회 목록 |
| REELE_GBN_NM | 재선 여부 |
| UNITS | 당선 횟수 |
| SEX_GBN_NM | 성별 |
| TEL_NO | 전화번호 |
| E_MAIL | 이메일 |
| HOMEPAGE | 홈페이지 |
| STAFF | 보좌관 |
| SECRETARY | 선임비서관 |
| SECRETARY2 | 비서관 |
| MONA_CD | 의원 코드 |
| MEM_TITLE | 약력 |
| ASSEM_ADDR | 사무실 호실 |

---

## 4. 국회의원 발의 법률안 API

### 요청 URL

```
https://open.assembly.go.kr/portal/openapi/nzmimeepazxkubdpn
```

### 필수 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| KEY | string (필수) | 인증키 |
| Type | string (필수) | 호출 문서 형식 |
| pIndex | integer (필수) | 페이지 위치 |
| pSize | integer (필수) | 페이지 당 요청 수 |
| AGE | string (필수) | 대수 |

### 선택 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| BILL_ID | string | 의안 ID |
| BILL_NO | string | 의안 번호 |
| BILL_NAME | string | 법률안명 |
| COMMITTEE | string | 소관 위원회 |
| PROC_RESULT | string | 본회의 심의 결과 |
| PROPOSER | string | 제안자 |
| COMMITTEE_ID | string | 소관위원회 ID |

### 출력값

| 출력명 | 출력 설명 |
|--------|------------|
| BILL_ID | 의안 ID |
| BILL_NO | 의안 번호 |
| BILL_NAME | 법률안명 |
| COMMITTEE | 소관 위원회 |
| PROPOSE_DT | 제안일 |
| PROC_RESULT | 본회의 심의 결과 |
| AGE | 대수 |
| DETAIL_LINK | 상세페이지 |
| PROPOSER | 제안자 |
| MEMBER_LIST | 제안자목록링크 |
| LAW_PROC_DT | 법사위처리일 |
| LAW_PRESENT_DT | 법사위상정일 |
| LAW_SUBMIT_DT | 법사위회부일 |
| CMT_PROC_RESULT_CD | 소관위처리결과 |
| CMT_PROC_DT | 소관위처리일 |
| CMT_PRESENT_DT | 소관위상정일 |
| COMMITTEE_DT | 소관위회부일 |
| PROC_DT | 의결일 |
| COMMITTEE_ID | 소관위원회ID |
| PUBL_PROPOSER | 공동발의자 |
| LAW_PROC_RESULT_CD | 법사위처리결과 |
| RST_PROPOSER | 대표발의자 |

---

## 5. 국회의원 본회의 표결 정보 API

> 국회의원 본회의 표결정보는 "PROC_RESULT" 상태가 "원안가결"/"수정가결"인 발의법률안의 BILL_ID 으로만 조회 가능하며 그중 일부 정보는 조회 불가능하다.

### 요청 URL

```
https://open.assembly.go.kr/portal/openapi/nojepdqqaweusdfbi
```

### 필수 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| KEY | string (필수) | 인증키 |
| Type | string (필수) | 호출 문서 형식 |
| pIndex | integer (필수) | 페이지 위치 |
| pSize | integer (필수) | 페이지 당 요청 수 |
| BILL_ID | string (필수) | 의안 ID |
| AGE | string (필수) | 대수 |

### 선택 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| HG_NM | string | 의원 이름 |
| POLY_NM | string | 정당명 |
| MEMBER_NO | string | 의원 번호 |
| VOTE_DATE | string | 의결일자 |
| BILL_NO | string | 의안 번호 |
| BILL_NAME | string | 의안명 |
| CURR_COMMITTEE | string | 소관 위원회 |
| RESULT_VOTE_MOD | string | 표결 결과 |
| CURR_COMMITTEE_ID | string | 위원회 ID |
| MONA_CD | string | 국회의원 코드 |

### 출력값

| 출력명 | 출력 설명 |
|--------|------------|
| HG_NM | 의원명 |
| HJ_NM | 한자명 |
| POLY_NM | 정당 |
| ORIG_NM | 선거구 |
| MEMBER_NO | 의원번호 |
| POLY_CD | 정당 코드 |
| ORIG_CD | 선거구 코드 |
| VOTE_DATE | 의결일자 |
| BILL_NO | 의안번호 |
| BILL_NAME | 의안명 |
| BILL_ID | 의안 ID |
| LAW_TITLE | 법률명 |
| CURR_COMMITTEE | 소관 위원회 |
| RESULT_VOTE_MOD | 표결 결과 |
| SESSION_CD | 회기 |
| CURRENTS_CD | 차수 |
| AGE | 대수 |
| MONA_CD | 국회의원 코드 |

---

## 6. 국회 API - 의안별 회의록 목록

### 요청 URL

```
https://open.assembly.go.kr/portal/openapi/VCONFBILLCONFLIST
```

### 필수 요청 인자

| 이름 | 타입 | 설명 |
|------|------|------|
| KEY | string (필수) | 인증키 |
| Type | string (필수) | 호출 문서 형식 |
| pIndex | integer (필수) | 페이지 위치 |
| pSize | integer (필수) | 페이지 당 요청 수 |
| BILL_ID | string (필수) | 의안 ID |

### 출력값

| 출력명 | 출력 설명 |
|--------|------------|
| BILL_ID | 의안 ID |
| BILL_NM | 의안명 |
| CONF_KND | 회의 종류 |
| CONF_ID | 회의 ID |
| ERACO | 대수 |
| SESS | 회기 |
| DGR | 차수 |
| CONF_DT | 회의일자 |
| DOWN_URL | 다운URL |
