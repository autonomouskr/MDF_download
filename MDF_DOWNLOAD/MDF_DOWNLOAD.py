
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

class DCCConnect:

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
        self.getMdfFileUUIDList()
        self.getMDFfile()

    def getMdfFileUUIDList(self):

        select_file_sql = "SELECT * FROM mdfparser.tbl_dcc_files WHERE download_yn <> 'Y'"
            
        self.cursor.execute(select_file_sql)
        results = self.cursor.fetchall()
        
        for file in results:
            self.data_links.append(file)

    def getMDFfile(self):
        print("[INFO] getMDFfile from DCC_Client")

        if(len(self.data_links)):
            for data_link in self.data_links:          
                response = requests.get(data_link['data_link'],auth=HTTPBasicAuth(self.username,self.password),stream=True)
                download_yn = ''
                if(response.status_code == 200):
                    file_path = self.download_path+"/"+data_link['obu']+"_"+data_link['vin']+"_"+data_link['measurement_uuid']+".zip"
                    with open(file_path, "wb") as file:
                        for chunk in response.iter_content(chunk_size=8192):  # 청크 단위로 읽기
                            file.write(chunk)
                        #print(f"파일 다운로드 완료: {self.download_path+"/"+data_link['obu']}")

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

    #DB 연결
    conn = get_mariadb();

    #DCC 접속
    dccConnect = DCCConnect(conn)


    
    



