import pandas as pd
import logging
import time


# 로깅 설정
logger = logging.getLogger("sync_engine")

class SyncEngine:
    """Excel과 DB 간 동기화 엔진"""

    def __init__(self, excel_handler, db_handler, id_column):
        """
        동기화 엔진 초기화
        
        Args:
            excel_handler (ExcelHandler): Excel 파일 핸들러
            db_handler (DatabaseHandler): 데이터베이스 핸들러
            id_column (str): 기본 키 열 이름
        
        """
        self.excel_handler = excel_handler
        self.db_handler = db_handler
        self.id_column = id_column
        logger.info("동기화 엔진 초기화")

        """ 
        추측
        
        Excel이 변경이 되었을 때 -> pd 로 df update 후 반영
        Database가 변경이 되었을 때 <- 어떻게 감지? 
        
        """

    
    def excel_to_db(self):
        """
        Excel -> DB 동기화

        Returns:
            bool: 동기화 성공 여부
        
        """
        logger.info("Excel -> DB 동기화 시작")
        start_time = time.time()

        try:
            #엑셀 데이터 읽기
            excel_df = self.excel_handler.read_data()
            if excel_df is None:
                return False
            
            # DB 테이블 존재 확인 및 필요시 생성
            if not self.db_handler.table_exists():
                logger.info("DB 테이블이 없어 새로 생성합니다.")
                return self.db_handler.create_table(excel_df)
            
            # DB 데이터 읽기
            db_df = self.db_handler.read_data()
            if db_df is None:
                return False
            
            # ID 열로 인덱싱
            excel_df.set_index(self.id_column, inplace = True)
            db_df.set_index(self.id_column, inplace=True)

            # 1, 추가된 레코드 처리
            new_records = excel_df.loc[~excel_df.index.isin(db_df.index)]
            if not new_records.empty:
                new_records_df = new_records.reset_index()  # ID 열을 다시 일반 열로 변환
                self.db_handler.insert_records(new_records_df)
                logger.info(f"{len(new_records)}개의 새 레코드가 추가되었습니다")

                
