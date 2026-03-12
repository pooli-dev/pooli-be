# 🐙 POOLI Backend

<p align="center">
  <img src="https://github.com/user-attachments/assets/1672b1f7-73d5-45d0-a228-03eed92a6a01" width="350"/>
</p

Pooli Backend는 **데이터 차감 트래픽 생성 및 처리 시스템과 이를 모니터링하기 위한 백엔드 서버**입니다.

Spring Boot 기반 API 서버로, Redis Stream 기반 메시지 처리와 Prometheus/Grafana 기반 모니터링을 제공합니다.

---

## Convention

### Code Convention
- https://www.notion.so/yerin1412/Code-Convention-5c8389b3e03983eab2f601a867af08dd

### Git Convention
- https://www.notion.so/yerin1412/Git-Convention-459389b3e03983d88a5e819b513c11bb


### Notion - Mentoring
#### 멘토링 
- https://www.notion.so/yerin1412/31a389b3e039803bb38ef8a973a4d54b

## 🛠️ Tech Stack

##### Language / Framework
![Java](https://img.shields.io/badge/Java-000000?style=for-the-badge&logo=openjdk&logoColor=white)
![Spring](https://img.shields.io/badge/Springboot-6DB33F?style=for-the-badge&logo=spring&logoColor=white)


##### Database
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)

##### Infra
![Redis](https://img.shields.io/badge/Redis-DD0031?style=for-the-badge&logo=redis&logoColor=white)
![Amazon EC2](https://img.shields.io/badge/Amazon_EC2-FF9900?style=for-the-badge&logo=amazon-ec2&logoColor=white)
![Amazon S3](https://img.shields.io/badge/Amazon_S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)

##### CI / CD
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)
![AWS CodeDeploy](https://img.shields.io/badge/AWS_CodeDeploy-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)

##### Collaboration Tools
![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)
![Notion](https://img.shields.io/badge/Notion-000000?style=for-the-badge&logo=notion&logoColor=white)
![Jira](https://img.shields.io/badge/Jira-0052CC?style=for-the-badge&logo=jira&logoColor=white)
![Discord](https://img.shields.io/badge/Discord-7289DA?style=for-the-badge&logo=discord&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)

##### Monitoring
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Loki](https://img.shields.io/badge/Loki-F46800?style=for-the-badge&logo=grafana&logoColor=white)



## Environment Variables

`.env`

```
SERVER_PORT=

DB_URL=
DB_USERNAME=
DB_PASSWORD=
# 0 means app startup does not fail immediately when DB is down
DB_INIT_FAIL_TIMEOUT=0

# Set false until migration scripts are ready
FLYWAY_ENABLED=TRUE
FLYWAY_BASELINE_ON_MIGRATE=true

SPRING_SECURITY_USER=
SPRING_SECURITY_PASSWORD=

MYBATIS_TYPE_ALIASES_PACKAGE=

LOG_LEVEL_MYBATIS=DEBUG
LOG_LEVEL_JDBC=DEBUG

OPENAPI_ENABLED=true
OPENAPI_DOCS_PATH=/v3/api-docs
SWAGGER_UI_ENABLED=true
SWAGGER_UI_PATH=/swagger-ui.html

REDIS_HOST=
REDIS_PORT=
REDIS_PASSWORD=

SESSION_TIMEOUT=30m
SESSION_REDIS_NAMESPACE=pooli:session
SESSION_COOKIE_NAME=JSESSIONID
SESSION_COOKIE_SECURE=true
SESSION_COOKIE_SAME_SITE=none

APP_CORS_ALLOWED_ORIGINS=

MONGO_URI=
LOCAL_MONGO_URI =

AWS_SECRET_KEY=
AWS_ACCESS_KEY=

CACHE_REDIS_HOST=
CACHE_REDIS_PORT=
CACHE_REDIS_PASSWORD=

STREAMS_REDIS_HOST=
STREAMS_REDIS_PORT=
STREAMS_REDIS_PASSWORD=
```





