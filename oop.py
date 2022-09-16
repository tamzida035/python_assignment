from sqlalchemy import MetaData,create_engine
#import sqlalchemy
import my_tables,created_metadata,dataframe_to_db,created_Exceptions,load_tables,db_CRUD_funcs,data_visual_init
from sqlalchemy.sql import text
import csv
#import unittest


# The Engine is how SQLAlchemy communicates with database
engine = create_engine('sqlite:////Users/tamzidatarannum/Documents/assign/practice_1/include/rough1/data.db',echo=True)
print("hello")
# build a random table
#rand=load_tables.load_table('random',engine)
#rand.print_mytable_columns()

# table to store data from 'train.csv'
train=load_tables.load_table('train',engine,True)

# table to store data from 'ideal.csv'
ideal=load_tables.load_table('ideal',engine,True,'x')


# table to store mapping of train function to ideal function and absolute value of maximum deviation between them
mapp=load_tables.load_table('train_to_ideal',engine)
#mapp.drop_mytable(engine)

# table to store squared deviations between train function and ideal functions
sq_dev=load_tables.load_table('sq_dev',engine,False,'dev')
#sq_dev.drop_mytable(engine)

# table to store sum of squared deviations between train function and ideal functions
sum_sq_dev=load_tables.load_table('sum_squared_dev',engine)
#sum_sq_dev.drop_mytable(engine)


NO_OF_TRAINING_FUNCTIONS=4
NO_OF_IDEAL_FUNCTIONS=50
NO_OF_FITTING_IDEAL_FUNCTIONS=4


#sum_sq_dev=db_CRUD_funcs.calculate_deviations(NO_OF_TRAINING_FUNCTIONS,NO_OF_IDEAL_FUNCTIONS,train,ideal,train_no,ideal_no,'x','y',sum_sq_dev,'sum_dev',engine)

func_no=1
while func_no<=NO_OF_TRAINING_FUNCTIONS:
	i=1
	train_no=str(func_no)
	while i<=NO_OF_IDEAL_FUNCTIONS:
		ideal_no=str(i)

		# calculate squared deviations between 'func-no'th train function & 'ideal'th ideal function
		sq_dev=db_CRUD_funcs.calculate_squared_dev(sq_dev,'dev',train,ideal,train_no,ideal_no,'x','y',engine)
			
		# Calculate sum of squared deviations between 'func-no'th train function & 'ideal'th ideal function 
		sum_sq_dev=db_CRUD_funcs.calculate_sum_squared_dev(sq_dev,'dev',sum_sq_dev,'sum_dev',engine)
			
		# clear 'sq_dev' table
		sq_dev.clear_mytable(engine)
		#sq_dev.drop_mytable(engine)

		i+=1

	# find the best fitting ideal functions corresponding to the train functions
	mapp=db_CRUD_funcs.train_to_ideal_mapping(mapp,'ideal_no',sum_sq_dev,'no','sum_dev',engine)
	# clear 'sum_squared_dev' table
	sum_sq_dev.clear_mytable(engine)		
	func_no+=1

# mapp.print_map_table(engine)
li_train=db_CRUD_funcs.get_list_from_column(mapp,engine,'train_no')
li_ideal=db_CRUD_funcs.get_list_from_column(mapp,engine,'ideal_no')


# calculate max_deviation between chosen ideal functions & corresponding training functions
mapp=db_CRUD_funcs.calculate_max_deviation(mapp,'train_no','abs_dev',train,ideal,'x','y',li_train,li_ideal,engine,NO_OF_TRAINING_FUNCTIONS)
#mapp.print_map_table(engine)

# table to store the mappable test points to chosen ideal dunctions, the mapping error and the number of corresponding ideal functions
test_map=load_tables.load_table('test_map',engine)
#test_map.drop_mytable(engine)
#test_map.clear_mytable(engine)


# read test.csv line by line and check if each test point can be mapped to any of the 4 chosen ideal functions.The resulting table is returned
test_map=dataframe_to_db.read_csv_line_by_line('../','test.csv',li_ideal,test_map,mapp,'ideal_no','abs_dev',ideal,'x','y',engine,NO_OF_FITTING_IDEAL_FUNCTIONS)
#test_map.print_test_table(engine)

mapp.clear_mytable(engine)

# visualize training functions,corresponding chosen ideal functions & mappable test points to these ideal functions
data_visual_init.initialize_Visual('../','ideal.csv','train.csv',test_map,li_ideal,li_train,engine)
test_map.clear_mytable(engine)

	 		
