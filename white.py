import os
import pandas as pd
from zipfile import ZipFile
import csv, sqlite3

## .zip 파일을 .csv 파일로 수월하게 변환하기 위해 파일명을 통일합니다.
# namechange = os.listdir('./white_zip')
# year = 1907
# for name in namechange:
#     src = os.path.join('./white_zip', name)
#     dst = str(year) + '.zip'
#     dst = os.path.join('./white_zip', dst)
#     os.rename(src, dst)
#     year += 1

## .zip 파일을 .csv 파일로 변환합니다.
# for num in range(1907, 2021):
#     path = './white_zip/%d.zip' %num
#     with ZipFile(path) as unzip:
#         unzip.extractall('white')

## .csv 파일을 모두 병합합니다.
# forders = os.listdir('./white')
# data = pd.DataFrame()
# for i in range(0, len(forders)):
#     if forders[i].split('.')[1] == 'csv':
#         file = './white/'+forders[i]
#         df = pd.read_csv(file, encoding = 'cp949') 
#         data = pd.concat([data, df])

## 원시 데이터에서 원하는 날짜 데이터만 저장합니다.
# data = pd.read_csv('./data.csv', index_col=0)
# snow = data[data['일시'].str.contains('12-19|12-20|12-21|12-22|12-23|12-24|12-25')]
# snow.to_csv('./snow.csv')

## 데이터 전처리(1) 후 csv 파일로 저장합니다.
# snow = pd.read_csv('./snow.csv', index_col=0)
# snow.sort_values(by=['일시'], axis=0, inplace=True)
# snow.drop(['지점', '현상번호(국내식)'], axis=1, inplace=True)
# snow.reset_index(drop=True, inplace=True)

## 헤더를 한글로 바꿔주고 csv 파일로 저장합니다.
## 변경 전 헤더
## [일시, 기온(°C), 강수량(mm), 풍속(m/s), 풍향(16방위), 습도(%), 증기압(hPa), 이슬점온도(°C), 현지기압(hPa), 해면기압(hPa),
## 일조(hr), 일사(MJ/m2), 적설(cm), 3시간신적설(cm), 전운량(10분위), 중하층운량(10분위), 운형(운형약어), 최저운고(100m ), 시정(10m),
## 지면상태(지면상태코드), 지면온도(°C), 5cm 지중온도(°C), 10cm 지중온도(°C), 20cm 지중온도(°C), 30cm 지중온도(°C)]
# headers = ['Date', 'Temperature', 'Precipitation', 'WindSpeed', 'WindDirection', 'Humidity', 'VaporPressure',
#         'DewPoint', 'LocalPressure', 'SeaLevelPressure', 'Sunshine', 'Radiation', 'Snow', '3hourSnow',
#         'AmountofCloud', 'LowMiddleLevelClouds', 'CloudsShape', 'MinimumCloudHeight', 'Visibility',
#         'GroundCondition', 'GroundTemperature', 'Soil5cm', 'Soil10cm', 'Soil20cm', 'Soil30cm']
# snow = pd.read_csv('./snow.csv', index_col=0, header=0, names=headers)
# snow.to_csv('./snow.csv')

## SQLlite 연결 및 테이블 생성
conn = sqlite3.connect('SNOW.db')
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS weather;")
cur.execute("""CREATE TABLE weather (Date DATETIME,
                                    Temperature FLOAT,
                                    Precipitation FLOAT,
                                    WindSpeed FLOAT,
                                    WindDirection INTEGER,
                                    Humidity INTEGER,
                                    VaporPressure FLOAT,
                                    DewPoint FLOAT,
                                    LocalPressure FLOAT,
                                    SeaLevelPressure FLOAT,
                                    Sunshine FLOAT,
                                    Radiation FLOAT,
                                    Snow FLOAT,
                                    '3hourSnow' FLOAT,
                                    AmountofCloud INTEGER,
                                    LowMiddleLevelClouds INTEGER,
                                    CloudsShape NVARCHAR(20),
                                    MinimumCloudHeight INTEGER,
                                    Visibility INTEGER,
                                    GroundCondition INTEGER,
                                    GroundTemperature FLOAT,
                                    Soil5cm FLOAT,
                                    Soil10cm FLOAT,
                                    Soil20cm FLOAT,
                                    Soil30cm FLOAT);
            """)

with open('./snow.csv', 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    white = [(i['Date'], i['Temperature'], i['Precipitation'], i['WindSpeed'], i['WindDirection'], i['Humidity'],
            i['VaporPressure'], i['DewPoint'], i['LocalPressure'], i['SeaLevelPressure'], i['Sunshine'],
            i['Radiation'], i['Snow'], i['3hourSnow'], i['AmountofCloud'], i['LowMiddleLevelClouds'],
            i['CloudsShape'], i['MinimumCloudHeight'], i['Visibility'], i['GroundCondition'],
            i['GroundTemperature'], i['Soil5cm'], i['Soil10cm'], i['Soil20cm'], i['Soil30cm']) for i in reader]

cur.executemany("""INSERT INTO weather (Date,
                                Temperature,
                                Precipitation,
                                WindSpeed,
                                WindDirection,
                                Humidity,
                                VaporPressure,
                                DewPoint,
                                LocalPressure,
                                SeaLevelPressure,
                                Sunshine,
                                Radiation,
                                Snow,
                                '3hourSnow',
                                AmountofCloud,
                                LowMiddleLevelClouds,
                                CloudsShape,
                                MinimumCloudHeight,
                                Visibility,
                                GroundCondition,
                                GroundTemperature,
                                Soil5cm,
                                Soil10cm,
                                Soil20cm,
                                Soil30cm) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, white)

conn.commit()
conn.close()
