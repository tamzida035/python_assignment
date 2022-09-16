from sqlalchemy import Table,Column,Integer,String,MetaData,create_engine,Float,Numeric
from sqlalchemy.sql import text




def make_train_table(table_name,m):
	'''
	  build training data table 
	  table_name= name of table
	  m=metaData
	'''
	train= Table(table_name,m,
	Column('x',Numeric),
	Column('y1',Numeric),
	Column('y2',Numeric),
	Column('y3',Numeric),
	Column('y4',Numeric)
	)
	return train

def make_single_column_table(table_name,m,col_name):
	'''
	  build single column table 
	  table_name= name of table
	  m=metaData
	  con_name=column name
	'''

	table= Table(table_name,m,
	Column(col_name,Numeric),

	)
	return table


def make_sum_squared_dev_table(table_name,m):
	'''
	  build table to store sum of squared dev of
	  each of the 4 training funcss
	  table_name= name of table
	  m=metaData
	'''
	table= Table(table_name,m,
	Column('n',Integer,primary_key=True,autoincrement=True),
	Column('sum_dev',Numeric)
	
	)
	return table

def make_mapping_table(table_name,m):
	'''
		build table to store train func no & fitting corresponding 
		ideal func no
	'''
	table= Table(table_name,m,
	Column('train_no',Integer,primary_key=True,autoincrement=True),
	Column('ideal_no',Integer)
	
	)
	return table


def build_table(m,eng):
	'''
	build the table using metadata m and engine eng
	'''
	return m.create_all(eng)


def create_fifty_columns(eng,table_name,is_fifty,col_prefix):
	'''
		create 50 columns for table with name 'table_name' with engine 'eng'
		isfifty= if true,make 50 columns .Else make 49 columns
	'''
	with eng.connect() as con:
		if(is_fifty):
			no=1
		else:
			no=2
		while no<51:
			col_name=col_prefix+str(no)
			try:
				query=con.execute(text(("ALTER TABLE %s ADD %s Float")% (table_name,col_name)))
			except:
				#msg=table_name +": 50 Columns already exist"
				msg="This "+table_name+": "+col_name+" already exists"
				#return msg
				print(msg)
				#break
			no+=1
		msg=table_name +": 50 Columns successfully created"
		print(msg) 



	



#******************************DEBUG FUNCS********************************************************

def get_table_name(m):
	'''
			debug func
	'''
	return m.sorted_tables.name


def verify_train_table(table_name,eng):
	'''
		query the train table and print the acquired data
		debug func
	'''
	with eng.connect() as conn:
		query = conn.execute(text(("SELECT x,y1,y2 FROM %s")%(table_name)))
		for q in query:
			print("{0} {1} {2}".format(q.x,q.y1,q.y2))


def verify_ideal_table(table_name,eng):
	'''
		query the ideal table and print the acquired data
		debug func
	'''
	with eng.connect() as conn:
		query = conn.execute(text(("SELECT x,y1,y50 FROM %s")%(table_name)))
		for q in query:
			print("{0} {1} {2}".format(q.x,q.y1,q.y50))

def verify_column_is_filled(table_name,col_name,eng):
	'''
		query whether column 'col_name' of 'table_name' table and print the containing data (if any)
		debug func
	'''
	with eng.connect() as conn:
		query = conn.execute(text(("SELECT %s FROM %s")%(col_name,table_name)))
		no=0;
		for q in query:
			no+=1
			print(q.sum_dev1)
		print(no)

def verify_column_is_filled2(table_name,col_name,eng):
	'''
		query whether column 'col_name' of 'table_name' table and print the containing data (if any)
		debug func
	'''
	with eng.connect() as conn:
		query = conn.execute(text(("SELECT %s FROM %s")%(col_name,table_name)))
		no=0;
		for q in query:
			no+=1
			print(q.dev)
		print(no)

	