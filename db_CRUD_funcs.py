from sqlalchemy import MetaData,create_engine
#import sqlalchemy
import my_tables,created_metadata,dataframe_to_db,created_Exceptions,load_tables,my_tables
from sqlalchemy.sql import text
import csv


def calculate_squared_dev(sq_dev,sq_dev_col,train,ideal,train_no,ideal_no,col_x,col_y,eng,is_squared=True):
	'''
		#calculate squared deviation or absolute deviation between 'train_no'th train function & 'ideal_no'th 
		ideal function, depending on value of 'is_squared'.  Insert the values into 'sq_dev' Mytable table object at 
		'sq_dev_col' column.  col_x & col_y are same column prefixes ('x',y') for 'train' & 'ideal' Mytable tables

		eng= database connection object
		ideal=MyTable object containing table storing the 'ideal.csv' data
		train=MyTable object containing table storing the 'train.csv' data
		col_x=column of 'ideal'/'train' Mytable table objects containing the x-values of ideal/train functions
		col_y=column prefixes of 'ideal'/'train' Mytable table objects containing the y-values of ideal/train functions
		is_squared=True, if squared deviation is calculated.  Else absolute deviation is calculated

		return: MyTable object containing the squared deviation or absolute deviation
	'''

	t_name=sq_dev.table_name
	t_col=sq_dev_col
	train_name=train.table_name
	name=ideal.table_name
	with eng.connect() as conn:
		if(is_squared==True):
			conn.execute(text(("INSERT INTO %s (%s) SELECT (%s.%s%s-%s.%s%s)*(%s.%s%s-%s.%s%s) FROM %s INNER JOIN %s ON %s.%s=%s.%s")%(t_name,t_col,train_name,col_y,train_no,name,col_y,ideal_no,train_name,col_y,train_no,name,col_y,ideal_no,train_name,name,train_name,col_x,name,col_x)))
		else:
			conn.execute(text(("INSERT INTO %s (%s) SELECT Abs(%s.%s%s-%s.%s%s) FROM %s INNER JOIN %s ON %s.%s=%s.%s")%(t_name,t_col,train_name,col_y,train_no,name,col_y,ideal_no,train_name,name,train_name,col_x,name,col_x)))
		return sq_dev

def calculate_sum_squared_dev(sq_dev,sq_dev_col,sum_sq_dev,sum_sq_dev_col,eng):
	'''
	#Calculate sum of squared deviations between 'train_no'th train function & 'ideal_no'th ideal function.
	MyTable table object 'sq_dev' at column 'sq_dev_col' stores the squared deviation values.
	Insert the calculated values into 'sum_squared_dev' MyTable table at 'sum_sq_dev_col' column 

	eng= database connection object

	return: MyTable object containing table storing the sum of squared deviations
	'''

	table_1=sq_dev.table_name  #'sq_dev'
	table_1_col=sq_dev_col      #'dev'
	table_2=sum_sq_dev.table_name #'sum_squared_dev'
	table_2_col=sum_sq_dev_col  #'sum_dev'
	with eng.connect() as conn:
		conn.execute(text(("INSERT INTO %s (%s) SELECT SUM(%s) FROM %s")%(table_2,table_2_col,table_1_col,table_1)))
		return sum_sq_dev


def train_to_ideal_mapping(mapp,mapp_col,sum_sq_dev,sum_sq_dev_col1,sum_sq_dev_col2,eng):
	'''
		find ideal function that best fits the train function.The number of the fitting function is 
		to be inserted into 'mapp' table at 'map_table_col' column

		sum_sq_dev= name of Mytable object storing sum of squared deviations between ideal & train functions
		sum_sq_dev_col1= column of 'sum_sq_dev' Mytable table object storing train function numbers
		sum_sq_dev_col2= column of 'sum_sq_dev' Mytable table object storing sum of squared deviations between ideal & train functions
		eng= database connection object

		return: MyTable object containing table storing train function numbers and corresponding best fitting ideal function numbers
	'''

	table_1=mapp.table_name #'train_to_ideal'
	table_1_col=mapp_col #'ideal_no'
	table_2=sum_sq_dev.table_name # 'sum_squared_dev'
	table_2_col_1= sum_sq_dev_col1 #'no'
	table_2_col_2=sum_sq_dev_col2 #'sum_dev'
	with eng.connect() as conn:
		conn.execute(text(("INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s= (SELECT MIN(%s) FROM %s)"%(table_1,table_1_col,table_2_col_1,table_2,table_2_col_2,table_2_col_2,table_2))))
		return mapp


def get_list_from_column(mapp,eng,col_name):
	'''
		put the function numbers stored at 'col_name' column in 'mapp' table to list 

		eng= database connection object

		return: list containing the function numbers
	'''

	table=mapp.table_name
	with eng.connect() as conn:
		query=conn.execute(text(("SELECT * FROM %s")%(table)))
		li=[]
		if(col_name=='train_no'):
			for q in query:
				li.append(q.train_no)
		elif(col_name=='ideal_no'):
			for q in query:
				li.append(q.ideal_no)
		return li


def calculate_max_deviation(mapp,mapp_col_1,mapp_col_2,train,ideal,col_x,col_y,li_train,li_ideal,eng,NO_OF_TRAINING_FUNCTIONS): 
	'''
		calculate max deviation between  chosen 'ideal' function and corresponding 'train' function. The result is stored at column
		'mapp_col_2' of 'mapp' Mytable table object. col_x & col_y are same column prefixes ('x',y') for 'train' & 'ideal' Mytable tables

		eng= database connection object
		mapp_col_1=column of 'mapp' Mytable table object storing train function numbers
		col_x=column of 'ideal'/'train' Mytable table objects containing the x-values of ideal/train functions
		col_y=column prefixes of 'ideal'/'train' Mytable table objects containing the y-values of ideal/train functions
		li_train=list containing train functions numbers
		li_ideal=list containing mapped ideal functions numbers
		NO_OF_TRAINING_FUNCTIONS= total number of training functions

		return: MyTable object containing table storing the calculated max deviations 
	'''
	# create temp table to store intermediate data
	try:
		temp=load_tables.load_table('temp',eng,False,'a_dev')
	except Exception:
		raise TableAlreadyExists(temp.table_name)
	
	i=0
	length=len(li_train)
	while(i<NO_OF_TRAINING_FUNCTIONS):
		train_col=str(li_train[i])
		ideal_col=str(li_ideal[i])
		temp=calculate_squared_dev(temp,'a_dev',train,ideal,train_col,ideal_col,col_x,col_y,eng,False)
		with eng.connect() as conn:
			conn.execute(text(("UPDATE %s SET %s=(SELECT MAX(a_dev) FROM temp) WHERE %s=%s")%(mapp.table_name,mapp_col_2,mapp_col_1,li_train[i])))
		temp.clear_mytable(eng)
		i+=1
	return mapp

def populate_test_table(index,li_ideal,test_map,mapp,mapp_col_1,mapp_col_2,ideal,ideal_col_1,ideal_col_2,row_x,row_y,eng):
	'''
	fill the table in Mytable object 'test_map' with  mappable test points, number of corresponding ideal functions and mapping error
	index=number of the ideal function

	li_ideal=list containing mapped ideal functions numbers
	mapp=MyTable object containing table storing the calculated max deviations
	mapp_col_1=column of 'mapp' Mytable table object storing ideal function numbers
	mapp_col_2=column of 'mapp' Mytable table object storing maximum deviations between train & 'ideal' functions
	ideal=MyTable object containing table storing the 'ideal.csv' data
	ideal_col_1=column of 'ideal' Mytable table object containing the x-values of ideal functions
	ideal_col_2=column prefix of 'ideal' Mytable table object containing the y-values of ideal functions
	eng= database connection object
	row_x=x-value of the test point that might be mapped
	row_y=y-value of the test point that might be mapped

	return: MyTable object containing table storing mappable test points,mapping error & corresponding ideal function number
	'''
	
	ideal_no=str(li_ideal[index])
	one=1
	SQ_ROOT_TWO=1.41421356237

	# create temp2 table to store intermediate data
	try:
		temp2=load_tables.load_table('temp2',eng)
	except Exception:
		raise TableAlreadyExists(temp2.table_name)
		
	temp2_col_1='n'
	temp2_col_2='test_ideal_diff'
	temp2_col_3='ideal_train_diff'

	
	with eng.connect() as conn:
		query=conn.execute(text(("INSERT INTO %s (%s) SELECT abs(%s%s- %s) FROM %s WHERE %s.%s=%s")%(temp2.table_name,temp2_col_2,ideal_col_2,ideal_no,row_y,ideal.table_name,ideal.table_name,ideal_col_1,row_x)))
		query=conn.execute(text(("UPDATE %s SET %s=(SELECT (%s*(%s)) FROM %s WHERE %s=%s) WHERE %s=%s")%(temp2.table_name,temp2_col_3,SQ_ROOT_TWO,mapp_col_2,mapp.table_name,mapp_col_1,li_ideal[index],temp2_col_1,one)))
		query=conn.execute(text(("SELECT * FROM %s")%(temp2.table_name)))
		for q in query:
			if(q.test_ideal_diff<q.ideal_train_diff):
				diff=q.test_ideal_diff
				conn.execute(text(("INSERT INTO %s VALUES (%s,%s,%s,%s)")%(test_map.table_name,row_x,row_y,diff,li_ideal[index])))
	temp2.clear_mytable(eng)
	return test_map


	
			
	


