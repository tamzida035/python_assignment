from sqlalchemy import MetaData,create_engine,Table,Column
#import sqlalchemy
import my_tables,created_metadata,dataframe_to_db,created_Exceptions,load_tables,db_CRUD_funcs
from sqlalchemy.sql import text
import unittest
#unittest.TestLoader.sortTestMethodsUsing = None

class TestTable(unittest.TestCase):
	'''
	test whether all the functions in the main files are generating correct output
	'''
	@classmethod
	def setUpClass(cls):
		# The Engine is how SQLAlchemy communicates with database
		cls.engine=create_engine('sqlite:///:memory:')

		# table to store data from 'train.csv'
		cls.train=load_tables.load_table('train',cls.engine,True)
		#train.get_column_name()

		# table to store data from 'ideal.csv'
		cls.ideal=load_tables.load_table('ideal',cls.engine,True,'x')

		# table to store mapping of train function to ideal function and absolute value of maximum deviation between them
		cls.mapp=load_tables.load_table('train_to_ideal',cls.engine)
		#mapp.drop_mytable(engine)

		# table to store squared deviations between train function and ideal functions
		cls.sq_dev=load_tables.load_table('sq_dev',cls.engine,False,'dev')
		#sq_dev.drop_mytable(engine)

		# table to store sum of squared deviations between train function and ideal functions
		cls.sum_sq_dev=load_tables.load_table('sum_squared_dev',cls.engine)
		#sum_sq_dev.drop_mytable(engine)

		# table to store the mappable test points to chosen ideal dunctions, the mapping error and the number of corresponding ideal functions
		cls.test_map=load_tables.load_table('test_map',cls.engine)

		print("Inside setUpClass")


	
	def test_func_1_calculate_squared_dev(self):
		'''
			 test whether function 'calculate_squared_dev' works correctly. 
		'''

		print("Inside func 1 ")
		expected_result=[(366.4942943232564,), (361.2871812916248,), (342.6161118470057,)]
		train_no=ideal_no=1
		self.sq_dev=db_CRUD_funcs.calculate_squared_dev(self.sq_dev,'dev',self.train,self.ideal,train_no,ideal_no,'x','y',self.engine)
		result=self.sq_dev.get_data_from_table(self.engine,'dev')
		self.assertEqual(result,expected_result,"Should have been equal")
		self.sq_dev.clear_mytable(self.engine)

	
	def test_func_2_calculate_sum_squared_dev(self):
		'''
			test whether function 'calculate_sum_squared_dev' works correctly.
			find sum_squared_dev of train function no=1 with each of 50 ideal funcs
	
		'''
		print("Inside func 2 ")
		expected_result=[(54009.91619612495,), (53751.16933138226,), (94448.51480944248,)]
		NO_OF_IDEAL_FUNCTIONS=50
		t_no='1'
		i=1
		while i<=NO_OF_IDEAL_FUNCTIONS:
			ideal_no=str(i)
			self.sq_dev=db_CRUD_funcs.calculate_squared_dev(self.sq_dev,'dev',self.train,self.ideal,t_no,ideal_no,'x','y',self.engine)
			self.sum_sq_dev=db_CRUD_funcs.calculate_sum_squared_dev(self.sq_dev,'dev',self.sum_sq_dev,'sum_dev',self.engine)
			self.sq_dev.clear_mytable(self.engine)
			i+=1

		result=self.sum_sq_dev.get_data_from_table(self.engine,'sum_dev')
		self.assertEqual(result,expected_result,"Should have been equal")

	def test_func_3_train_to_ideal_mapping(self):
		'''
		 test whether function 'train_to_ideal_mapping' works correctly.
		 Find the best fitting ideal function for train_func_no=1
		'''

		print("Inside func 3 ")
		expected_result=[(1, 11, None)]
		#expected_result=[(1, 11, 2)]
		self.mapp=db_CRUD_funcs.train_to_ideal_mapping(self.mapp,'ideal_no',self.sum_sq_dev,'no','sum_dev',self.engine)
		result=self.mapp.get_data_from_table(self.engine)
		self.sum_sq_dev.clear_mytable(self.engine)
		self.assertEqual(result,expected_result,"Should have been equal")
		
	def test_func_4_get_list_from_column(self):
		'''
			test whether function 'get_list_from_column' works correctly
			Use ideal function number stored in table of Mytable object to verify it
		'''

		expected_result=[11]
		print("Inside func 4 ")
		result=db_CRUD_funcs.get_list_from_column(self.mapp,self.engine,'ideal_no')
		self.assertEqual(result,expected_result,"Should have been equal")
		#self.mapp.print_map_table(self.engine)

	
	def test_func_5_calculate_max_deviation(self):
		'''
		test whether 'calculate_max_deviation' function works correctly
		'''

		expected_result=[(1, 11, 0.4989410000000003)]
		print("Inside func 5")
		li_train=db_CRUD_funcs.get_list_from_column(self.mapp,self.engine,'train_no')
		li_ideal=db_CRUD_funcs.get_list_from_column(self.mapp,self.engine,'ideal_no')
		#print(self.li_ideal)
		self.mapp=db_CRUD_funcs.calculate_max_deviation(self.mapp,'train_no','abs_dev',self.train,self.ideal,'x','y',li_train,li_ideal,self.engine,1)
		result=self.mapp.get_data_from_table(self.engine)
		#print(self.li_ideal)
		self.assertEqual(result,expected_result,"Should have been equal")
		

	
	def test_func_6_populate_test_table(self):
		'''
		test whether function 'populate_test_table' works correctly
		'''

		print("Inside func 6 ")
		li_ideal=db_CRUD_funcs.get_list_from_column(self.mapp,self.engine,'ideal_no')
		test_point_x=-17.4
		test_point_y=-17.979618
		expected_result=[(-17.4, -17.979618, 0.579618, 11)]
		self.test_map=db_CRUD_funcs.populate_test_table(0,li_ideal,self.test_map,self.mapp,'ideal_no','abs_dev',self.ideal,'x','y',test_point_x,test_point_y,self.engine)
		result=self.test_map.get_data_from_table(self.engine)
		self.assertEqual(result,expected_result,"Should have been equal")
		#self.test_map.print_test_table(self.engine)






		




    



    	
    	
    	
    	
    	
    	




	
	

	
	

	













