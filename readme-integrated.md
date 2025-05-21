# 건축물 및 토지 정보 관리 시스템

이 서비스는 에어테이블에 등록된 지번주소를 바탕으로 공공데이터포털 API를 통해 건축물 정보와 토지 정보를 조회하고 에어테이블에 업데이트하는 자동화 시스템입니다.

## 서비스 관리

### 로그 관리
- 서비스 로그는 `/home/building-service/logs/` 디렉토리에 일별로 저장됩니다.
- 7일 이상 된 로그는 자동으로 삭제됩니다.
- PM2 로그는 다음 명령어로 확인할 수 있습니다:
  ```bash
  pm2 logs building-service
  ```

### 서비스 상태 확인
```bash
# PM2 상태 확인
pm2 status

# 서비스 상세 정보 확인
pm2 show building-service
```

### 서비스 중지 및 재시작
```bash
# 서비스 재시작
pm2 restart building-service

# 서비스 중지
pm2 stop building-service

# 서비스 삭제
pm2 delete building-service
```

### 문제 해결

1. 서비스가 시작되지 않는 경우
   - 로그 확인: `pm2 logs building-service`
   - 환경 설정 확인: `.env` 파일에 모든 값이 제대로 설정되어 있는지 확인
   - 권한 문제: 디렉토리 및 파일 권한 확인

2. API 호출 오류
   - API 키 확인: 모든 API 키가 유효한지 확인
   - 네트워크 연결 상태 확인
   - 요청 제한 확인: API 공급자의 요청 제한 확인

3. 에어테이블 연동 오류
   - API 키 확인: 에어테이블 API 키가 올바른지 확인
   - 테이블/뷰 ID 확인: 에어테이블의 Base ID, Table ID, View ID가 올바른지 확인

## 시스템 구조

서비스는 다음과 같은 디렉토리 구조로 구성됩니다:

```
/home/building-service/
├── app.js              # 메인 애플리케이션 파일
├── package.json        # Node.js 패키지 정보
├── .env                # 환경 설정 파일
├── logs/               # 로그 파일 디렉토리
│   └── YYYY-MM-DD.log  # 일별 로그 파일
└── public/             # 웹 인터페이스 파일
    └── index.html      # 메인 웹 인터페이스
```

Nginx 설정은 다음 위치에 있습니다:
```
/etc/nginx/sites-available/building
/etc/nginx/sites-enabled/building -> /etc/nginx/sites-available/building
```

## 기술 스택

- **Backend**: Node.js, Express
- **API 통신**: Axios
- **XML 처리**: xml-js
- **작업 스케줄링**: node-cron
- **프로세스 관리**: PM2
- **웹 서버**: Nginx
- **데이터베이스**: Airtable

## 보안 고려사항

- `.env` 파일에 저장된 API 키와 비밀 정보는 적절한 권한 설정(600)으로 보호해야 합니다.
- 공개 액세스가 필요한 경우, 웹 인터페이스에 Basic 인증을 추가하는 것을 고려하세요.
- 주기적으로 API 키를 교체하는 것이 좋습니다.

## 라이센스

이 프로젝트는 개인 사용 목적으로 개발되었습니다.주요 기능

### 건축물 정보 처리
- 에어테이블에서 지번주소 데이터 조회
- 구글 스크립트 API를 통해 시군구 및 법정동 코드 획득
- 공공데이터포털 API를 통해 건축물 정보 조회
- 건축물 정보 가공 및 에어테이블 업데이트

### 토지 정보 처리
- 에어테이블에서 지번주소 데이터 조회
- 구글 스크립트 API를 통해 시군구 및 법정동 코드 획득
- PNU 코드 생성
- 토지 정보 API를 통해 토지 정보 조회
- 토지 정보 가공 및 에어테이블 업데이트

### 시스템 기능
- 작업 스케줄링 (매시간 자동 실행, 10AM-7PM)
- 작업 상태 및 결과 모니터링
- 웹 인터페이스를 통한 작업 관리
- 상세 로깅 및 오류 처리

## 설치 및 배포 방법

### 시스템 요구사항
- Node.js 18.0.0 이상
- npm 또는 yarn
- Nginx 웹 서버
- PM2 (프로세스 관리자)

### 배포 단계

1. 프로젝트 파일 다운로드 및 준비
```bash
git clone https://github.com/yourusername/building-land-service.git
cd building-land-service
```

2. 환경 설정 파일 수정
`.env` 파일을 열어 필요한 API 키와 설정 정보를 입력합니다:
```
AIRTABLE_API_KEY=your_airtable_api_key
GOOGLE_SCRIPT_URL=your_google_script_url
PUBLIC_API_KEY=your_public_api_key
```

3. 자동 배포 스크립트 실행
```bash
chmod +x deploy.sh
sudo ./deploy.sh
```

4. 수동 설치 (배포 스크립트를 사용하지 않는 경우)

   a. 디렉토리 생성 및 파일 준비
   ```bash
   sudo mkdir -p /home/building-service
   sudo mkdir -p /home/building-service/logs
   sudo mkdir -p /home/building-service/public
   
   # 파일 복사
   sudo cp app.js package.json .env /home/building-service/
   sudo cp -r public/* /home/building-service/public/
   ```

   b. 의존성 설치
   ```bash
   cd /home/building-service
   npm install --production
   ```

   c. PM2를 통한 서비스 시작
   ```bash
   # PM2 설치 (아직 설치되지 않은 경우)
   sudo npm install -g pm2
   
   # 서비스 시작
   pm2 start app.js --name "building-service"
   
   # 시스템 재시작 시 자동으로 시작하도록 설정
   pm2 startup
   pm2 save
   ```

   d. Nginx 설정
   ```bash
   # Nginx 설정 파일 생성
   sudo cp nginx/building /etc/nginx/sites-available/
   
   # 심볼릭 링크 생성
   sudo ln -s /etc/nginx/sites-available/building /etc/nginx/sites-enabled/
   
   # Nginx 설정 테스트
   sudo nginx -t
   
   # Nginx 재시작
   sudo systemctl restart nginx
   ```

## 사용 방법

### 웹 인터페이스
웹 브라우저에서 도메인(building.goldenrabbit.biz)에 접속하여 직관적인 웹 인터페이스를 통해 서비스를 관리할 수 있습니다.

- 건축물 정보 작업 실행 버튼: 건축물 정보만 업데이트
- 토지 정보 작업 실행 버튼: 토지 정보만 업데이트
- 모든 작업 실행 버튼: 건축물 정보와 토지 정보 모두 업데이트

### API 엔드포인트
- `GET /health`: 서비스 상태 확인
- `GET /run-building-job`: 건축물 정보 작업 수동 실행
- `GET /run-land-job`: 토지 정보 작업 수동 실행
- `GET /run-all-jobs`: 모든 작업 수동 실행

## 