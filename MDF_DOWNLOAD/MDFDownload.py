
import asammdf
from requests.auth import HTTPBasicAuth
import requests
import pymysql
import datetime
import json
import zipfile
import io
import flask
import numpy as np
from asammdf import MDF, Signal
from datetime import datetime
import pandas as pd
import zipfile
import os
import logging
from logging.handlers import RotatingFileHandler

class DCCConnect:


    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    logging.basicConfig(
        filename="app.log",  # 로그 파일 경로
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"  # UTF-8 인코딩 설정 (한글 깨짐 방지)
    )
    logging.info("이것은 파일에 저장되는 로그입니다.")

    logger = logging.getLogger("RotatingLogger")
    logger.setLevel(logging.INFO)

    # 최대 1MB 크기로 3개 파일까지 유지
    file_handler = RotatingFileHandler("app.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(file_handler)

    logger.info("이 로그는 파일 크기 제한이 적용된 로그입니다.")

    global conn
    global username
    global password
    global data_links
    global API_URL
    global download_path
    global cursor
    global zip_path

    def __init__(self,db_conn):
        self.conn = db_conn
        self.username = "ymkim2924@autonomouskr.com"
        self.password = "PkWtFVfwAx2Z"
        self.download_path = "D:/path/download/mdf"
        self.zip_path = "D:/path/to/mdf_files"
        self.API_URL = "https://pilot.carmedialab.com/DccResultDataAccess/rest/results"
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        self.data_links = []
        self.get_MdfFileUUIDList()
        self.get_allMDF()
#       self.get_MDFfile()


    """
       DCC server 에 있는 모든 mdf4 파일 리스트를 가져온다. 
    """
    def get_allMDF(self):

        print("[INFO] GET MDF file all list from DCC SERVER.")
        response = requests.get(self.API_URL,auth=HTTPBasicAuth(self.username,self.password))

        try:
            if response.status_code == 200:
                result = response.text()

                if result.uuid not in self.data_links:
                    self.insert_files(result)

                print("[INFO] GET MDF file all list from DCC SERVER.")
            else:
                print("[INFO] Failed MDF file list get from DCC SERVER.")

        except Exception as e:
           print(f"[ERROR] GET MDF file all list from DCC SERVER.{e}")

    def insert_files(self, result):
        insert_sql = "SELECT * FROM mdfparser.tbl_dcc_files WHERE download_yn <> 'Y'"
        insert_val = (
            
            )
        self.cursor.execute(insert_sql)

    def get_MdfFileUUIDList(self):

        select_file_sql = "SELECT * FROM mdfparser.tbl_dcc_files WHERE download_yn <> 'Y'"
            
        self.cursor.execute(select_file_sql)
        results = self.cursor.fetchall()
        
        for file in results:
            self.data_links.append(file)

    def get_MDFfile(self):
        print("[INFO] getMDFfile from DCC_Client")

        if(len(self.data_links)):
            for data_link in self.data_links:          
                response = requests.get(data_link['data_link'],auth=HTTPBasicAuth(self.username,self.password),stream=True)
                download_yn = ''
                if(response.status_code == 200):
                    file_path = self.download_path+"/"+data_link['obu']+"_"+data_link['vin']+"_"+data_link['measurement_uuid']+".zip"
                    with open(file_path, "wb") as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)

                    download_yn = 'Y'
                    updata_file_sql = "UPDATE mdfparser.tbl_dcc_files SET download_yn = '"+download_yn+"' WHERE uuid = '"+data_link['uuid']+"'";
                    result = self.cursor.execute(updata_file_sql)
                    print(updata_file_sql)
                    print("[SUCCESS] MDF4 FILE DOWNLOAD AND QUERY UPDATE SUCCESS.")
                else:
                    download_yn = 'N'
                    updata_file_sql = "UPDATE mdfparser.tbl_dcc_files SET download_yn = '"+download_yn+"' WHERE uuid = '"+data_link['uuid']+"'";
                    result = self.cursor.execute(updata_file_sql)
                    print("[INFO] MDF4 FILE DOWNLOAD FAILED")
                self.conn.commit()
            self.openZip()
        else:
            self.openZip()

    
    def openZip(self):
        for zip_file in os.listdir(self.download_path):
            file_path = self.download_path+"/"+zip_file
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(self.zip_path)

def get_mariadb():
    conn = pymysql.connect(host ='localhost', user='root', password = '1q2w3e4r', db = 'mdfparser')
    return conn

if __name__ == '__main__':

    conn = get_mariadb();
    dccConnect = DCCConnect(conn)


    
    



