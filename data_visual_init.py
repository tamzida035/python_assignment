import sqlalchemy
import dataframe_to_db,data_visualise,my_tables



def initialize_Visual(path,ideal_file,train_file,test_map,li_ideal,li_train,engine):
	'''
	 convert the csv files ideal_file & train_file into dataframe.

	 li_train=list containing train functions numbers
	 li_ideal=list containing mapped ideal functions numbers
	 path=path to the csv files
	 engine= database connection object
	 test_map=MyTable object containing table storing mappable test points,mapping error & corresponding ideal function number
	'''
	
	ideal_df=dataframe_to_db.convert_csv_to_dataframe(path,ideal_file)
	train_df=dataframe_to_db.convert_csv_to_dataframe(path,train_file)
	
	length=len(li_ideal)
	i=0
	while i<length:
		train_no=li_train[i]
		ideal_no=li_ideal[i]
		no=i+1
		name=str(no)
		output_file_name='graph'+name+'.html'
		test_map_df=dataframe_to_db.convert_table_to_dataframe(test_map.table_name,li_ideal[i],engine)
		#print(test_map_df['x'],test_map_df['y'])
		data_visualise.setup_plotting(output_file_name,ideal_df,ideal_no,train_df,train_no,test_map_df)
		i+=1

	
	


	


