import pandas as pd
from sqlalchemy import create_engine, inspect, text
import logging

# 로깅 설정
logger = logging.getLogger("db_handler")

class DatabaseHandler:
    """데이터베이스 처리 클래스 """

    def __init__(self, connection_string, table_name, id_column):
        """
        데이터베이스 핸들러 초기화
        
        Args:
            connection_string (str): DB 연결 문자열
            table_name (str): 테이블 이름
            id_column (str): 기본 키 열 이름
        
        """

        self.connection_string = connection_string
        self.table_name = table_name
        self.id_column = id_column
        self.engine = None
        logger.info(f"DB 핸들러 초기화 : {table_name}")

        self._create_engine()

    def _create_engine(self):
        """SQLAlchemy 엔진 생성"""

        try:
            self.engine = create_engine(self.connection_string)
            logger.debug("DB 엔진 생성 성공")
        except Exception as e:
            logger.error(f"DB 엔진 생성 오류: {str(e)}")
            self.engine = None

    def table_exist(self):
        """테이블 존재 확인"""
        if not self.engine:
            return False
        
        inspector = inspect(self.engine)
        return inspector.has_table(self.table_name)

    
    def create_table(self, dataframe):
        """
        데이터프레임 구조에 맞게 테이블 생성
        
        Args:
            dataframe (pandas.DataFrame): 테이블 구조의 기준이 될 데이터프레임
            
        Returns:
            bool: 생성 성공 여부
        
        """
        try:
            if not self.engine:
                return False
            
            
            logger.info(f"테이블 생성 : {self.table_name}")
            dataframe.to_sql(self.table_name, self.engine, index=False, if_exists="replace")
            return True
        
        except Exception as e:
            logger.error(f"테이블 생성 오류:{self.table_name}, {str(e)}")
            return False
        

    def read_data(self):
        """
        데이터 베이스에서 데이터 읽기

        Returns:
            pandas.DataFrame: DB 데이터
        
        """

        try:
            if not self.engine:
                return None
            
            if not self.table_exist():
                logger.warning(f"테이블이 존재하지 않음 : {self.table_name}")
                return pd.DataFrame

            logger.debug(f"DB 데이터 읽기 : {self.table_name}")
            query = f"SELECT * FROM {self.table_name}"
            return pd.read_sql(query, self.engine)
        
        except Exception as e:
            logger.error(f"DB 일기 오류 : {str(e)}")
            return None
        

    def insert_records(self, dataFrame):
        """
        새 레코드 삽입

        Args:
            dataframe (pandas.DataFrame): 삽입할 데이터
            
        Returns:
            bool: 삽입 성공 여부

        """
        try:
            if not self.engine or dataFrame.empty:
                return False
            
            logger.info(f"레코드 삽입: {len(dataFrame)}개")
            dataFrame.to_sql(self.table_name, self.engine, if_exists="append", index=False)
            return True
        
        except Exception as e:
            logger.error(f"레코드 삽입 오류: {str(e)}")
            return False
        
    
    def update_record(self, id_value, update_data):
        """
        레코드 업데이트
        
        Args:
            id_value: 업데이트할 레코드의 ID 값
            update_data (dict): 업데이트할 열과 값
            
        Returns:
            bool: 업데이트 성공 여부
        
        """
        try:
            if not self.engine:
                return False
            
            with self.engine.connect() as conn:

                set_clause = ", ".join([f"{column} = :{column}" for column in update_data.keys()])
                query = text(f"UPDATE {self.table_name} SET {set_clause} WHERE {self.id_column} = :id")

                # 매개 변수 준비
                params = update_data.copy()
                params['id'] = id_value

                # 쿼리 실행 
                conn.execute(query, params)
                conn.commit()

                logger.debug(f"레코드 업데이트: ID = {id_value}")
                return True
        
        except Exception as e:
            logger.error(f"레코드 업데이트 오류: {str(e)}")
            return False
        
    def delete_record(self, id_value):
        """
        레코드 삭제

         Args:
            id_value: 삭제할 레코드의 ID 값
            
        Returns:
            bool: 삭제 성공 여부
        
        """

        try:
            if not self.engine:
                return False
            
            with self.engin.connect() as conn:
                query = text(f"DELETE FROM {self.table_name} WHERE {self.id_column} = :id")
                conn.execute(query, {"id" : id_value})
                conn.commit()


                logger.debug(f"레코드 삭제: ID={id_value}")
                return True
            
        except Exception as e:
            logger.error(f"레코드 삭제 오루 : {str(e)}")
            return False

    
        





            



