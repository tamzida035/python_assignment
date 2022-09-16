from sqlalchemy import MetaData,create_engine
#import sqlalchemy
import created_tables,created_metadata,dataframe_to_db
from sqlalchemy.sql import text
from sqlalchemy.exc import OperationalError

class TableBuildFailedError(OperationalError):
	'''
	Exception raised for table building failure
	'''
	
	def __init__(self,table_name,msg="Failed to build table"):

		self.message=msg
		self.table_name=table_name
		print("Table {0}: {1}".format(table_name,msg))
		super().__init__(table_name,self.message)


class TableDoesNotExist(AttributeError):
	'''
	exception raised for trying to create a table that already exists
	'''

	def __init__(self,table_name,msg="Table does not exist"):
		self.message=msg
		self.table_name=table_name
		print("Table {0}: {1}".format(table_name,msg))
		super().__init__(table_name,self.message)










	
	



