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
            # 없는 데이터를 loc을 활용해서 인덱싱하여 찾는다.
            new_records = excel_df.loc[~excel_df.index.isin(db_df.index)]
            if not new_records.empty:
                new_records_df = new_records.reset_index()  # ID 열을 다시 일반 열로 변환
                self.db_handler.insert_records(new_records_df)
                logger.info(f"{len(new_records)}개의 새 레코드가 추가되었습니다")

            
            # 2. 변경된 레코드 처리
            common_records = excel_df.loc[excel_df.index.isin(db_df.indexl)]
            updates_count = 0

            for idx in common_records.index:
                excel_row = excel_df.loc[idx]
                db_row = db_df.loc[idx]

                # 값이 다른지 확인
                if not excel_row.equals(db_row):
                    update_data = {col: excel_row[col] for col in excel_row.index}
                    self.db_handler.update_record(idx, update_data)
                    updates_count += 1

                if updates_count > 0:
                    logger.info(f"{updates_count}개의 레코드가 업데이트되었습니다")

            
            # 삭제된 레코드 처리 (선택적)
            deleted_records = db_df.loc[~db_df.index.isin(excel_df.index)]
            if not deleted_records.empty:
                for idx in deleted_records.index:
                    self.db_handler.delete_record(idx)
                logger.info(f"{len(deleted_records)}개의 레코드가 삭제되었습니다.")

            elapsed_time = time.time() - start_time
            logger.info(f"Excel ->  DB 동기화 완료 (소요 시간: {elapsed_time:.2f}초)")
            return True
        
        except Exception as e:
            logger.error(f"Excel -> DB 동기화 오류: {str(e)}")
            return False
    
    def db_to_excel(self):
        """
        DB -> Excel 동기화

        Returns:
            bool: 동기화 성공 여부

        """
        logger.info("DB-> Excel 동기화 시작")
        start_time = time.time()

        try:
            #DB 데이터 읽기
            db_df = self.db_handler.read_data()
            if db_df is None or db_df.empty:
                logger.warning("DB에 데이터가 없습니다.")
                return False
            
            #파일에 저장
            result = self.excel_handler.write_data(db_df)
            
            elapsed_time = time.time() - start_time
            logger.info(f"DB-> Excel 동기화 완료 (소요시간: {elapsed_time:.2f}초)")
            return result
        
        except Exception as e:
            logger.error(f"DB-> Excel동기화 오류: {str(e)}")
            return False
        
                
                



