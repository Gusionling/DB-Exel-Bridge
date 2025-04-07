"""
ExcelSync 설정 파일
DB 연결 정보와 동기화 설정을 관리합니다.
"""
import os
from dotenv import load_dotenv

# .env 파일 로드 (있는 경우)
load_dotenv()

# 엑셀 파일 설정
EXCEL_FILE = os.getenv("EXCEL_FILE", "data.xlsx")   # 모니터링할 엑셀 파일 경로

# 데이터베이스 연결 설정
# DB 타입 설정 (sqlite, mysql, postgresql 중 선택)
DB_TYPE = os.getenv("DB_TYPE", "sqlite")

# 기본 DB 설정값
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")  # 환경 변수로 관리하는 것이 좋음
DB_NAME = os.getenv("DB_NAME", "excel")
DB_CHARSET = os.getenv("DB_CHARSET", "utf8")

# 데이터베이스 타입에 따른 연결 문자열 생성
if DB_TYPE == "sqlite":
    DB_CONNECTION = os.getenv("DB_CONNECTION", "sqlite:///database.db")
elif DB_TYPE == "mysql":
    DB_CONNECTION = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}?charset={DB_CHARSET}"
elif DB_TYPE == "postgresql":
    DB_CONNECTION = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# 테이블 설정
TABLE_NAME = os.getenv("TABLE_NAME", "employees")   # 동기화할 데이터베이스 테이블 이름
ID_COLUMN = os.getenv("ID_COLUMN", "employee_id")   # 기본 키로 사용할 열 이름

# 동기화 설정
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30"))  # 파일 변경 확인 간격 (초)

# 로깅 설정
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")  # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FILE = os.getenv("LOG_FILE", "logs/excel_sync.log")

# 디버그 모드 설정
DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")