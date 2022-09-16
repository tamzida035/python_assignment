import pandas as pd
from sqlalchemy import create_engine
import data_visualise
import csv
import my_tables,db_CRUD_funcs
from sqlalchemy.sql import text


def convert_csv_to_dataframe(path,file_name):
	'''
	convert a csv file to dataframe.

	file_name=name of the csv file
	path=path to the csv file
	return: converted dataframe object
	'''

	file_loc=path+file_name
	try:
		data = pd.read_csv(file_loc)
	except:
		raise FileNotFoundError(file_name,"FILE EXISTS NOT OR LOCATION IS INCORRECT")
	else:
		return data




def data_read_and_load(path,file_name,table_name,eng):
	'''
	Read data from csv and convert it into dataframe.
	Then convert the dataframe object into sql database table

	file_name=name of the csv file
	path=path to the csv file
	table_name=name of the sql database table
	eng= database connection object
	'''

	data =convert_csv_to_dataframe(path,file_name)
	data.to_sql(table_name,eng,if_exists='replace')
		
			


def convert_table_to_dataframe(table_name,ideal_index,eng):
	'''
	convert database table to dataframe

	table_name= name of the database table
	ideal_index= number of the mappable ideal function

	return: resulting dataframe object
	'''

	#data=pd.read_sql_table(table_name,eng)
	data=pd.read_sql(text(("SELECT x,y FROM %s WHERE no_ideal=%s")%(table_name,ideal_index)),eng)
	return data
	
		


def read_csv_line_by_line(path,file_name,li_ideal,test_map,mapp,mapp_col_1,mapp_col_2,ideal,ideal_col_1,ideal_col_2,eng,NO_OF_FITTING_IDEAL_FUNCTIONS):
	'''
	read 'file_name' csv file line by line and check if each test point can be mapped to any of the 4 chosen ideal functions.  Return the resulting table
	via 'test_map' Mytable object

	file_name=name of the csv file
	path=path to the csv file
	li_ideal=list containing mapped ideal functions numbers
	mapp=MyTable object containing table storing the calculated max deviations
	mapp_col_1=column of 'mapp' Mytable table object storing ideal function numbers
	mapp_col_2=column of 'mapp' Mytable table object storing maximum deviations between train & 'ideal' functions
	ideal=MyTable object containing table storing the 'ideal.csv' data
	ideal_col_1=column of 'ideal' Mytable table object containing the x-values of ideal functions
	ideal_col_2=column prefix of 'ideal' Mytable table object containing the y-values of ideal functions
	eng= database connection object
	NO_OF_FITTING_IDEAL_FUNCTIONS= total no of chosen ideal functions

	return: MyTable object containing table storing mappable test points,mapping error & corresponding ideal function number
	'''

	file_loc=path+file_name
	try:
		with open(file_loc,newline='') as f: 
			reader=csv.DictReader(f)
			for row in reader:
				i=0
				while i<NO_OF_FITTING_IDEAL_FUNCTIONS:
					# fill up the 'test_map' object with mappable test points,mapping error & corresponding ideal function number data
					test_map=db_CRUD_funcs.populate_test_table(i,li_ideal,test_map,mapp,mapp_col_1,mapp_col_2,ideal,ideal_col_1,ideal_col_2,row['x'],row['y'],eng)
					i+=1
		
			return test_map
	except:
		raise FileNotFoundError(file_name,"FILE EXISTS NOT OR LOCATION IS INCORRECT")
    	





	






