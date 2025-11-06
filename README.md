# 한국어 Fediverse 인스턴스 디렉토리

정적 HTML, CSS, JS로 구성된 한국어 페디버스 인스턴스 목록입니다. 데이터는 `data/instances.json` 파일 하나로 관리하며, GitHub Pages에서 그대로 호스팅할 수 있습니다.

## 데이터 편집

1. `data/instances.json`을 열어 항목을 추가하거나 수정합니다.
2. `name`, `url`, `platform`, `description` 필드를 유지한 채 필요한 값으로 변경합니다.
3. 저장 후 페이지를 새로고침하면 변경 내용이 반영됩니다.

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
