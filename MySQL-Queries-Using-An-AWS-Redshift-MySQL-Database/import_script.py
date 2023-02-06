import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import *
import os
import re

print('please input the value for scheme (synonymous for database')

u = 'root' # for the case of local mysql instance running on UNIX socket.
pw= '' # no password by default for local mysql instance for user 'root'
#input_scheme = input()
host='database.nggdttw.eu-central.rds.amazonaws.com' # localhost for UNIX socket

def ce(host,u=u,pw=pw,scheme='MAIN1'):
	engine = create_engine(
			f"mysql+mysqlconnector://{u}:{pw}@{host}/{scheme}"
					)
	try:
		print(f'Done creating the engine for: mysql+mysqlconnector://{u}:{pw}@{host}/{scheme}')
		return engine
	except Exception:
		print(f'Error creating the engine for: mysql+mysqlconnector://{u}:{pw}@{host}/{scheme}')

engine = ce(host=host,u='admin',pw='-------',scheme='PRAC1')

print('please input the value for data_dir as an absolute path\nwithout trailing slash')
data_dir='/Volumes/data/dataset-practice/video-game-sales'

maptype = {"dtype('O')":"Text","dtype('int64')":"Integer","dtype('float64')":"Float"}

def get_convert(data_dir=data_dir,engine=engine,maptype=maptype):
	files=os.listdir(data_dir)
	pat = re.compile('\.csv$',re.IGNORECASE)
	csv_files=[file for file in files if pat.search(file)]
	maptype = {"dtype('O')":"Text","dtype('int64')":"Integer","dtype('float64')":"Float"}
	for f in csv_files:
		l = data_dir + "/"+ f
		df=pd.read_csv(l,low_memory=False)
		print(f'done creating df for file {f}')
		dtype = dict(zip(df.columns.tolist(),list()))
		dtypesf = [df[col].dtype for col in df.columns]
		for i in df.columns.tolist():
			print(i)
		for j in dtypesf:
			print(j)
		try:
			for (col,dtypecol) in zip(df.columns.tolist(),dtypesf):
				for i in maptype.keys():
					if dtypecol == str(i[1]):
						dtype[i[0]].append(maptype[dtypecol])
		except ValueError:
			print('problem with "maptype" line ~60')




				
					
		df.to_sql(
                name=f[:-4],
				con=engine,
				if_exists='replace',
				index=False,
				chunksize=1000,
				dtype = dtype,
				)
	print('done')

get_convert()
