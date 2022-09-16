from sqlalchemy import Table,Column,Integer,String,MetaData,create_engine,Float,Numeric
from sqlalchemy.sql import text
from sqlalchemy.exc import OperationalError
from created_Exceptions import TableDoesNotExist

class MyTable():
	'''
		database table structure
	'''
	def __init__(self,table_name,col_name=''):
		'''
		defines the table stuctures

		'train' & 'ideal' has columns as per their respective csv files.
		 'sq_dev' has single column named 'dev'
		 'temp' has single column named 'a_dev' 
		'''
		
		self.table_name=table_name
		self.meta_data=MetaData()
		if(table_name=='train'):
			self.table= Table(table_name,self.meta_data,
			Column('x',Numeric),
			Column('y1',Numeric),
			Column('y2',Numeric),
			Column('y3',Numeric),
			Column('y4',Numeric)
			)
		elif(table_name=='ideal' or table_name=='sq_dev' or table_name=='temp'):
			self.table=Table(table_name,self.meta_data,
			Column(col_name,Numeric))
		elif(table_name=='sum_squared_dev'):
			self.table=Table(table_name,self.meta_data,
			Column('no',Integer,primary_key=True,autoincrement=True),
			Column('sum_dev',Numeric))
		elif(table_name=='temp2'):
			self.table=Table(table_name,self.meta_data,
			Column('n',Integer,primary_key=True,autoincrement=True),
			Column('test_ideal_diff',Numeric),
			Column('ideal_train_diff',Numeric))
		elif(table_name=='train_to_ideal'):
			self.table=Table(table_name,self.meta_data,
			Column('train_no',Integer,primary_key=True,autoincrement=True),
			Column('ideal_no',Integer),
			Column('abs_dev',Numeric))
		elif(table_name=='test_map'):
			self.table=Table(table_name,self.meta_data,
			Column('x',Numeric),
			Column('y',Numeric),
			Column('dev',Numeric),
			Column('no_ideal',Integer))
					
        
	def build_fifty_columns(self,eng):
		'''
		Add 50 columns to 'ideal' table
		eng=database connection object
		'''

		is_exists=False
		with eng.connect() as con:
			no=1
			while no<=50:
				col_no=str(no)
				try:
					query=con.execute(text(("ALTER TABLE %s ADD y%s Numeric")% (self.table_name,col_no)))
				except OperationalError:
					msg="Table "+self.table_name+" already exists"
					print(msg)
					is_exists=True
					break
				
				no+=1
			if(is_exists==False):
				msg=self.table_name +": 50 Columns successfully created"
				print(msg)



	def print_mytable_columns(self):
		if 'self.table' not in locals():
			raise TableDoesNotExist(self.table_name)
		else:
			for c in self.table.c:
				print(c)


	def build_mytable(self,eng):
		'''
		build this table using engine 'eng' & metadata
		'''

		self.meta_data.create_all(eng)
		

	def clear_mytable(self,eng):
		'''
			removes data from this table

			eng= database connection object
		'''

		with eng.connect() as conn:
			conn.execute(text(("DELETE FROM %s")%(self.table_name)))
		
#******************FUNCTIONS USED IN UNIT TESTS***********************************

	def get_data_from_table(self,eng,col_name=''):
		'''
			fetch the top 3 rows from tables whose column name 'col_name' are provided.
			Else fetch all queries

			eng= database connection object

			return: row object
		'''
		if(col_name!=''):
			with eng.connect() as conn:
				query=conn.execute(text(("SELECT %s FROM %s LIMIT 3")%(col_name,self.table_name)))	
		else:
			with eng.connect() as conn:
				query=conn.execute(text(("SELECT * FROM %s")%(self.table_name)))	
		return query.fetchall()

		

#**********DEBUG FUNC****************

	def drop_mytable(self,eng):	
		'''
		delete this table from database using engine 'eng'
		'''
		return self.meta_data.drop_all(eng)


	def print_table(self,eng):
		'''
		read data from table and print it
		'''
		with eng.connect() as con:
			data=con.execute(text(("SELECT %s FROM %s")%(self.table.c[0],self.table_name)))
			for d in data:
				print("{0}".format(d.x))


	def print_single_column(self,eng,col_name):
		'''
			print the tables having only 1 column with name 'col_name'
		'''
		with eng.connect() as conn:
			#print("COL_NAME: {0} Table_name: {1}".format(col_name,self.table_name))
			query=conn.execute(text(("SELECT * FROM %s")%(self.table_name)))
			for q in query:
				if(col_name=='dev'):
					print(q.dev)
				elif(col_name=='sum_dev'):
					print(q.sum_dev)
				elif(col_name=='a_dev'):
					print(q.a_dev)
			
	def print_map_table(self,eng):
		#print("INSIDE1:")
		#print(self.table_name)
		with eng.connect() as conn:
			query=conn.execute(text(("SELECT * FROM %s")%(self.table_name)))
			#print(query.fetchall())
			
			for q in query:
				print("{0}  {1} {2}".format(q.train_no,q.ideal_no,q.abs_dev))
			
				

	def print_diff_table(self,eng):
		with eng.connect() as conn:
			query=conn.execute(text(("SELECT * FROM %s")%(self.table_name)))
			for q in query:
				print("{0}  {1} {2}".format(q.n,q.test_ideal_diff,q.ideal_train_diff))

	def print_test_table(self,eng):
		with eng.connect() as conn:
			query=conn.execute(text(("SELECT * FROM %s")%(self.table_name)))
			#print(query.fetchall())
			
			for q in query:
				print("{0}   {1}    {2}   {3}".format(q.x,q.y,q.dev,q.no_ideal))
			











