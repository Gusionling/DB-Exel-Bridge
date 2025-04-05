import pandas as pd
import os
import hashlib
import logging
from datetime import datetime

# 로깅 설정
logger = logging.getLogger("excel_handler")

class ExcelHandler:

    def __init__(self, excel_path):
        """
        Excel 핸들러 초기화
    
        Args:
            excel_path (str): Excel 파일 경로
        
        """

        self.excel_path = excel_path
        self.last_modified_time = None   # 마지막으로 확인한 파일 수정 시간 (변경 감지용)
        self.file_hash = None       # 변경 감지를 위해서 마지막으로 계산한 파일 해시값을 가지고 있어야 한다.
        logger.info(f"Excel 핸들러 초기화: {excel_path} ")


    def file_exist(self):
        """엑셀 파일 존재 여부 확인"""
        return os.path.exists(self.excel_path)
    
    def read_data(self):
        """
        엑셀 파일에서 데이터 읽기 

        Returns:
            pandas.Dataframe: 엑셀 데이터

        """

        try:
            if not self.file_exist():
                logger.error(f"파일이 존재하지 않음 : {self.excel_path}")
                return None
            
            logger.debug(f"엑셀 파일 읽기: {self.excel_path}")
            df = pd.read_excel(self.excel_path)
            return df
        
        except Exception as e:
            logger.error(f"엑셀 파일 읽기 오류:{str(e)}")
            return None
    
    def write_data(self, df):
        """
        데이터 프레임을 엑셀 파일로 저장

        Args:
            df (pandas.DataFrame): 저장할 데이터
        
        Returns:
            bool: 저장 성공 여부
        """

        try:
            logger.debug(f"엑셀 파일 저장: {self.excel_path} ")
            df.to_excel(self.excel_path, index=False)
            #파일 메타 데이터 업데이트
            self._update_file_metadata()
            return True
        
        except Exception as e:
            logger.error(f"엑셀 파일 저장 오류: {str(e)}")
            return False

    def detect_changes(self):
        """
        파일 변경 여부 감지

        Returns:
            bool: 변경 여부
        """

        try:
            if not self.file_exist():
                return False
            
            # 파일 수정 시간 확인
            current_modified_time = os.path.getmtime(self.excel_path)

            # 파일 해시 계산
            with open(self.excel_path, 'rb') as f:
                current_hash = hashlib.md5(f.read()).hexdigest
            
            #변경 여부 확인
            if (self.last_modified_time is None or current_modified_time > self.last_modified_time_time or 
                self.file_hash != current_hash):

                # 메타데이터 업데이트
                self.last_modified_time = current_modified_time
                self.file_hash = current_hash
            
                logger.info(f"파일 변경 감지: {self.excel_path}")
                return True
            
            return False
        
        except Exception as e:
            logger.error(f"변경 감지 오류: {str(e)}")
            return False
    

    def _update_file_metadata(self):
        """파일 메타데이터 (수정 시간, 헤시) 업데이트"""
        if not self.file_exist():
            return
        
        self.last_modified_time = os.path.getmtime(self.excel_path)

        with open(self.excel_path, 'rb') as f:
            self.file_hash = hashlib.md5(f.read()).hexdigest

    

        


