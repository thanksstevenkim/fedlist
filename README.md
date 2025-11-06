# 한국어 Fediverse 인스턴스 디렉토리

정적 HTML, CSS, JS로 구성된 한국어 페디버스 인스턴스 목록입니다. `data/instances.json`의 기본 정보와 `data/stats.json`의 통계를
병합해 한 화면에서 확인할 수 있으며, 검색/플랫폼 필터와 간단한 정렬 기능을 제공합니다. GitHub Pages에서 그대로 호스팅할 수 있습니다.

## 데이터 편집

1. `data/instances.json`을 열어 항목을 추가하거나 수정합니다.
2. 각 항목은 `name`, `host`, `url`, `platform`, `description`, `languages`(수동 언어 코드 배열) 필드를 포함합니다.
3. 통계 수집이 언어 목록을 감지하지 못하는 경우를 대비해 `languages`에 우선 표시할 언어 코드를 지정할 수 있습니다.
4. 저장 후 페이지를 새로고침하면 변경 내용이 반영됩니다.

## 통계 갱신

통계는 하루 1회 정도 갱신하도록 가정하며, 아래 명령으로 수동 실행할 수 있습니다.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install requests  # 선택: 설치하지 않으면 표준 라이브러리로 동작합니다.
python scripts/fetch_stats.py
```

명령을 실행하면 `data/stats.json`이 생성·갱신됩니다. 결과를 커밋하고 GitHub에 푸시하면 정적 페이지에도 통계가 반영됩니다.
- `requests`를 설치하면 HTTPS 처리와 타임아웃 제어가 보다 견고해집니다. 설치하지 않아도 스크립트는 표준 라이브러리 `urllib`로
  동작합니다.

### 연합 인스턴스 탐색(선택)

기존 인스턴스와 연합 중인 도메인을 참고하려면 다음 명령으로 후보 목록을 생성할 수 있습니다.

```bash
python scripts/fetch_stats.py --discover-peers
```

- NodeInfo와 소프트웨어별 API에서 제공하는 `peers` 정보, Mastodon의 `GET /api/v1/instance/peers` 응답을 바탕으로
  `data/peer_suggestions.json`이 생성됩니다.
- 표준 출력으로 직접 확인하고 싶다면 `python scripts/fetch_stats.py --discover-peers --peer-output -` 처럼 `-`를 지정하세요.
- 생성된 목록은 `data/instances.json`에 새 항목을 추가할 때 참고용으로 사용하고, 실제 이름(name)은 원본 인스턴스에서 확인한 뒤 입력합니다.

## 검색과 필터

- 상단 검색 입력(`id="q"`)은 이름과 설명을 대상으로 부분 일치 검색을 수행합니다.
- 플랫폼 드롭다운(`id="platformFilter"`)은 `data/instances.json`에 존재하는 플랫폼 목록을 자동으로 반영합니다.
- 통계 열 헤더(총 사용자, 활성 사용자)는 클릭 시 오름차순/내림차순을 토글합니다.
- `languages` 열은 `data/instances.json`의 수동 지정 언어와 `data/stats.json`에서 감지한 언어를 병합해 표시합니다.
- ActivityPub 검증에 실패한 항목은 이름 옆에 “검증 실패” 뱃지가 붙습니다.
- 가입 열은 NodeInfo/보조 API에서 확인한 가입 가능 여부를 “열림/닫힘/불명” 중 하나로 요약합니다.
- 긴 설명은 이름 셀 하단에 작은 글씨로 표시되며, 테이블 간격을 넉넉하게 조정했습니다.

## ActivityPub 검증

`scripts/fetch_stats.py`는 아래 순서로 인스턴스를 점검합니다.

1. `https://{host}/.well-known/nodeinfo`를 호출해 최신 NodeInfo 문서를 찾습니다.
   - NodeInfo 응답에서 소프트웨어 이름, 가입 가능 여부, 사용자/활성 사용자/게시물 수를 우선 추출합니다.
2. NodeInfo만으로 부족한 경우 소프트웨어별 보조 엔드포인트를 순차적으로 시도합니다.
   - Mastodon: `GET /api/v2/instance` → 실패 시 `GET /api/v1/instance`
   - Misskey: `POST /api/meta` (본문 `{}`)
3. 어떤 단계든 성공하면 `verified_activitypub`이 `true`로 기록되고, 실패가 이어지면 `false`와 함께 로그에 이유가 남습니다.

언어 정보는 NodeInfo `usage.languages`나 Mastodon `configuration.languages`로 감지되며, 누락된 언어는 `data/instances.json`의
`languages` 필드로 수동 지정할 수 있습니다. 감지가 실패하더라도 UI는 `-` 또는 “데이터 없음”으로 표시됩니다.
 
NodeInfo 메타데이터와 소프트웨어별 peers API 응답은 연합 도메인 후보를 수집하는 데에도 활용되며, 자세한 사용법은 위 “연합 인스턴스
탐색” 절을 참고하세요.

## 문자열(i18n)

모든 UI 문자열은 `i18n/strings.json`에 정의되어 있으며, 기본 언어는 `ko`입니다. 새 언어를 추가하려면 동일한 키 구조로
언어 코드를 추가하고 `index.html`의 `lang` 속성을 변경하세요. JSON을 불러오지 못하는 경우에는 내장 한국어 문자열로
폴백합니다.

## 자동 통계 및 Pages 배포

저장소에는 `scripts/fetch_stats.py`를 매일 06:00 (Asia/Seoul) / 21:00 (UTC) 에 실행해 통계를 커밋하고 GitHub Pages에 배포하는
워크플로 `.github/workflows/update.yml`이 포함되어 있습니다.

- GitHub Actions의 기본 `GITHUB_TOKEN`에 `contents: write`, `pages: write` 권한이 필요합니다.
- 조직 정책으로 쓰기 권한이 제한된 경우 `repo` 권한이 있는 PAT를 `REPO_TOKEN` 비밀키로 등록하고, 워크플로의 주석 처리된
  `token` 항목을 활성화하세요.
- 워크플로를 로컬에서 점검하려면 [`act`](https://github.com/nektos/act)를 설치한 뒤 `act -j update`를 실행하세요.

## 로컬 미리보기

브라우저의 `file://` 경로로 열면 보안 정책 때문에 JSON을 불러오지 못할 수 있습니다. 아래 명령으로 간단한 서버를 실행한 뒤 `http://localhost:8000`을 방문하세요.

```bash
python -m http.server 8000
```

## GitHub Pages 배포

1. 이 저장소를 GitHub에 푸시합니다.
2. GitHub에서 저장소의 **Settings → Pages**로 이동합니다.
3. **Source**를 `Deploy from a branch`, **Branch**를 `main`(또는 원하는 브랜치)과 `/ (root)`로 설정합니다.
4. 저장하면 몇 분 후 `https://<사용자명>.github.io/<저장소명>/`에서 사이트를 확인할 수 있습니다.

## 라이선스

이 프로젝트는 [MIT License](./LICENSE)를 따릅니다.
