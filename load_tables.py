from sqlalchemy import MetaData,create_engine
#import sqlalchemy
import my_tables,created_metadata,dataframe_to_db,created_Exceptions
from sqlalchemy.sql import text

def load_table(table_name,engine,read_data_from_csv=False,col_name=''):
	'''
		build structure of table and create the table

		table_name= name of table
		engine= database connection object
		if read_data_from_csv=True, read data from csv file
		col_name=name of column in the table having single column
		no_of_int_parameters=how many int parameters will the table have
	'''
	
	if(table_name=='ideal' or table_name=='sq_dev' or table_name=='temp'):
		table=my_tables.MyTable(table_name,col_name)
	else:
		table=my_tables.MyTable(table_name)
	table.build_mytable(engine)
	if(table_name=='ideal'):
		table.build_fifty_columns(engine)
	if(read_data_from_csv):
		file_name=table_name+'.csv'
		path='../'
		dataframe_to_db.data_read_and_load('../',file_name,table_name,engine)
	return table



	
	
	