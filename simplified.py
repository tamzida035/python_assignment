
from sqlalchemy import MetaData,create_engine
#import sqlalchemy
import created_tables,created_metadata,dataframe_to_db,created_Exceptions
from sqlalchemy.sql import text
from sqlalchemy.orm.exc import MultipleResultsFound

# ALL TABLES ARE REMOVED TO A SEPARATE FILE TO SIMPLIFY THE FILE STRUCTURE


# The Engine is how SQLAlchemy communicates with your database
engine = create_engine('sqlite:////Users/tamzidatarannum/Documents/assign/practice_1/include/rough1/p_db.db',echo=True)



#***********LOAD TRAIN DATA INTO DATABASE*************

#metadata for training data table
meta_train = created_metadata.create_metadata()


#create training data table structure
train=created_tables.make_train_table('train',meta_train)

#temp_table=created_tables.MyTable('temp',meta_train)

#build Training data table 
created_tables.build_table(meta_train,engine)


#load train.csv into training  data table
dataframe_to_db.data_read_and_load('../','train.csv','train',engine)




#***********LOAD IDEAL DATA INTO DATABASE*************

#metadata for ideal data table
meta_ideal = created_metadata.create_metadata()

#create ideal data table structure
ideal=created_tables.make_single_column_table('ideal',meta_ideal,'x')


#build ideal data table 
created_tables.build_table(meta_ideal,engine)


#create 50 y-columns for ideal data table
created_tables.create_fifty_columns(engine,'ideal',True,'y')


#load ideal.csv into ideal data table
dataframe_to_db.data_read_and_load('../','ideal.csv','ideal',engine)



#***********FIND THE 4 IDEAL FUNCS*************
'''
	step 1a: create a table(squared_dev_1) of 50 columns storing squared deviation of training-func-1 with each ideal func at each x
	step 1b: load table(squared_dev_1) with data from train and ideal table
	step 2: find the sum of squared deviation for training-func-1 with each ideal func by summing each column obtained in step 1
	step 3: create a new table(sum_squared_deviation) and store the sums obtained in step 2 in 1st column.This table should have n values (no of ideal func)
	step 4: repeat the steps 1-3 for training-funcs-2,3,4
	step 5: find the minimum value of each column of table .Find the corresponding ideal func no using n

'''

#******Step 3 Create the 2-column table that will store the train func--> ideal func mapping

#metadata for sum_squared_deviation,squared_dev_1 to 4 tables
meta_mapping= created_metadata.create_metadata()

#create train_to_ideal table structure
train_to_ideal_table=created_tables.make_mapping_table('train_to_ideal',meta_mapping)

#build 'train_to_ideal' table 
created_tables.build_table(meta_mapping,engine)

'''
with engine.connect() as conn:
	conn.execute(text("DROP TABLE ideal"))
	conn.execute(text("DROP TABLE train"))
	#conn.execute(text("DELETE FROM train_to_ideal"))
'''

	






NO_OF_TRAINING_FUNCTIONS=4
NO_OF_IDEAL_FUNCTIONS=50

func_no=1
while func_no<=1:

	#print("func_no={0}".format(func_no))

	#metadata for sq_dev table for 'func_no'th train func
	meta_sq_dev=created_metadata.create_metadata()

    #create a table 'sum_squared_dev' to hold the sum of all(with each ideal func) squared deviations for 'func-no'th train func
	sum_squared_deviation_table=created_tables.make_sum_squared_dev_table('sum_squared_dev',meta_sq_dev)

	
	#create a table 'sq_dev' table to hold squared deviations of 'func-no'th train func with each 'ideal' func
	sq_dev_table=created_tables.make_single_column_table('sq_dev',meta_sq_dev,'dev')

	
	#build sq_dev table
	created_tables.build_table(meta_sq_dev,engine)
	
	'''
	with engine.connect() as conn:
		conn.execute(text("DROP TABLE sum_squared_dev"))
		conn.execute(text("DROP TABLE sq_dev"))
	'''
	


	#now insert values from 'train' & 'ideal' to 'sq_dev'
	
	
	no=str(func_no)
	with engine.connect() as conn:

		ideal=1
		train_col=no
		while ideal<=1:

			ideal_col=str(ideal)
			#calculate squared deviation between 'func-no'th train func & 'ideal'th ideal func
			conn.execute(text(("INSERT INTO sq_dev (dev) SELECT (train.y%s-ideal.y%s)*(train.y%s-ideal.y%s) FROM train INNER JOIN ideal ON train.x=ideal.x")%(train_col,ideal_col,train_col,ideal_col)))
			
			#created_tables.verify_column_is_filled2('sq_dev','dev',engine)

			#Calculate & store sum of squared deviations between 'func-no'th train func & 'ideal'th ideal func at 'sum_squared_dev' table
			conn.execute(text("INSERT INTO sum_squared_dev (sum_dev) SELECT SUM(dev) FROM sq_dev"))
			
			#clear data from 'sq_dev'	
			conn.execute(text("DELETE FROM sq_dev"))

			query1=conn.execute(text("SELECT * FROM sum_squared_dev"))
			for q in query1:
				print("{0}   {1}".format(q.n,q.sum_dev))

	
			
			ideal+=1
		    #created_tables.verify_column_is_filled('sum_squared_dev',sum_squared_col,engine)

	    
		query4=conn.execute(text("INSERT INTO train_to_ideal (ideal_no) SELECT n FROM sum_squared_dev WHERE sum_dev= (SELECT MIN(sum_dev) FROM sum_squared_dev)"))
		#print("FITTING IDEAL FUNC for {0}th train func is".format(func_no))

		conn.execute(text("DELETE FROM sum_squared_dev"))
	
	func_no+=1
	
#DEBUG
'''
 with engine.connect() as conn:
		query1=conn.execute(text("SELECT * FROM sum_squared_dev"))
		for q in query1:
			print("{0}   {1}".format(q.n,q.sum_dev))
'''
'''
		THIS CODE WORKS
		query2=conn.execute(text("SELECT MIN(sum_dev) FROM sum_squared_dev"))
		print("MIN SUM_SQ_DEV for {0}th train func is".format(func_no))
		
		for q in query2:
			print(q)
'''

'''
		THIS CODE WORKS
		query3=conn.execute(text("SELECT n FROM sum_squared_dev WHERE sum_dev= (SELECT MIN(sum_dev) FROM sum_squared_dev)"))
		print("FITTING IDEAL FUNC for {0}th train func is".format(func_no))
		
		for q in query3:
			print(q)
'''

#DEBUG train_to_ideal
'''
with engine.connect() as conn:
	query5=conn.execute(text("SELECT * FROM train_to_ideal"))
	for q in query5:
		print("{0}   {1}".format(q.train_no,q.ideal_no))
'''


	
 
	
	
	



    
    











