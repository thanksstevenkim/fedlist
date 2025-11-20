# 연합우주를 여행하는 히치하이커를 위한 안내서

# The Hitchhiker's Guide to the Fediverse

정적 HTML, CSS, JS로 구성된 한국어 페디버스 인스턴스 목록입니다.  
`data/instances.json`의 기본 정보와 `data/stats.ok.json`의 통계를 병합해 한 화면에서 확인할 수 있습니다.  
GitHub Pages로 그대로 호스팅할 수 있으며, 검색·필터·정렬 기능을 기본 제공합니다.

모든 데이터는 공개 API를 통해 자동 수집되며, 개인정보는 포함되지 않습니다.

---

## 📁 데이터 구조

| 파일                          | 설명                                                    |
| ----------------------------- | ------------------------------------------------------- |
| `data/instances.json`         | 수동으로 등록한 인스턴스 목록                           |
| `data/stats.ok.json`          | ActivityPub 검증 성공 및 통계 이상치 없는 정상 인스턴스 |
| `data/stats.bad.json`         | 검증 실패·네트워크 오류·이상치 등 비정상 인스턴스       |
| `data/peer_suggestions.json`  | 연합 관계를 통해 발견된 새 인스턴스 후보                |
| `data/filtered_peers.json`    | 스팸/광고 도메인을 걸러낸 최종 후보                     |
| `data/spam_filtered.log.json` | 스팸 필터에서 제외된 도메인과 이유 로그                 |

---

## ⚙️ 실행 순서 (추천 워크플로)

아래 세 단계를 순서대로 실행하면 됩니다.

---

### ① 연합 피어 목록 생성

기존 인스턴스에서 연합된 피어 도메인을 탐색합니다.

```bash
python scripts/fetch_stats.py --discover-peers
```

- NodeInfo 및 소프트웨어별 API의 `peers` 정보를 수집합니다.
- 결과: `data/peer_suggestions.json`
- 이미 검사된(host가 `stats.ok.json` 또는 `stats.bad.json`에 존재하는) 도메인은 자동으로 제외됩니다.
- 표준 출력으로 내보내려면 `--peer-output -`을 사용할 수 있습니다.

---

### ② 스팸·비정상 도메인 필터링

`filter_spam.py`로 피어 목록에서 명백히 스팸·광고·비정상 도메인을 제거합니다.

```bash
python scripts/filter_spam.py
```

- 입력: `data/peer_suggestions.json`
- 출력:

  - `data/filtered_peers.json` — 통과된 후보 (다음 단계 입력으로 사용)
  - `data/spam_filtered.log.json` — 제외된 도메인과 사유 기록

- 필터링 기준:

  - 의심스러운 TLD (`.tk`, `.ml`, `.xyz`, 등)
  - 스팸/성인/도박/암호화폐 등 키워드
  - 숫자·반복 패턴 도메인
  - (존재 시) ActivityPub 검증 실패 또는 비정상 통계

- 옵션:

  - `--blocklist <파일>` : 추가 차단 목록 지정
  - `--dry-run` : 결과를 파일로 저장하지 않고 콘솔로만 출력

> 💡 이 단계에서 미리 걸러내면, `fetch_stats.py`가 불필요한 네트워크 요청을 하지 않아 훨씬 빠르게 동작합니다.

---

### ③ 통계 수집 (정상 도메인만)

스팸 필터를 통과한 인스턴스만 대상으로 통계를 가져옵니다.

```bash
python scripts/fetch_stats.py --input data/filtered_peers.json
```

- 결과:

  - `data/stats.ok.json` — ActivityPub 검증 성공 및 정상 통계
  - `data/stats.bad.json` — 검증 실패, 미응답, 이상치, 비정상 통계 등

- 인스턴스 1개 처리마다 즉시 저장되므로, 중간에 중단되어도 이미 수집된 데이터는 보존됩니다.
- `--input`을 사용하면 이미 기록된 호스트는 자동으로 스킵합니다.
- `requests` 패키지를 설치하면 HTTPS 요청 성능이 향상됩니다.
- 최초 실행 시 기존 `stats.json`이 존재하면 자동으로 OK/BAD 구조로 마이그레이션됩니다.

---

## 🧩 스크립트 요약

| 스크립트         | 역할                                                   |
| ---------------- | ------------------------------------------------------ |
| `fetch_stats.py` | ActivityPub 노드/플랫폼별 API를 통해 통계 수집 및 검증 |
| `filter_spam.py` | 도메인 이름 기반 스팸·광고·비정상 후보 자동 필터링     |
| `update.yml`     | (옵션) GitHub Actions를 이용해 자동 갱신 및 Pages 배포 |

---

## 🖥️ 웹 UI 기능

### ✔ 이름/URL 통합

- 인스턴스 이름이 바로 URL로 링크됨
- URL 열 제거 → 표가 더 간결해짐

### ✔ 가입 여부 토글 버튼

- 체크박스 제거
- "가입 닫힌 서버 표시" 버튼
- 클릭 시 초록색 활성 상태 (`badge--ok` 스타일)

### ✔ 검색 및 언어 필터 개선

- 검색창 폭 확장
- 언어 드롭다운은 자동 정규화된 언어명 표시 (예: `en` → "영어")

### ✔ 기타 기능

- 플랫폼 필터 자동 생성
- 설명·이름 실시간 검색
- 통계 열 정렬
- 가입 여부 배지 표시
- 비정상 인스턴스 자동 표시

---

## 🔄 자동 통계 및 Pages 배포

`.github/workflows/update.yml`은 매일 06:00 (Asia/Seoul) / 21:00 (UTC)에
`fetch_stats.py`를 실행해 `stats.ok.json`과 `stats.bad.json`을 갱신하고
GitHub Pages로 자동 배포합니다.

- `GITHUB_TOKEN`에 `contents: write`, `pages: write` 권한이 필요합니다.
- 제한된 조직 계정에서는 `repo` 권한을 가진 PAT을 `REPO_TOKEN`으로 등록하세요.
- 로컬에서도 [`act`](https://github.com/nektos/act)로 `act -j update` 명령으로 테스트 가능합니다.

---

## 💻 로컬 미리보기

```bash
python -m http.server 8000
```

`http://localhost:8000`에서 사이트를 확인합니다.
브라우저에서 `file://`로 직접 열면 JSON이 불러와지지 않을 수 있습니다.

---

## ⚖️ 라이선스

이 프로젝트는 [MIT License](./LICENSE)를 따릅니다.
