import pandas as pd
from bokeh.plotting import figure, output_file, show
from bokeh.models import ColumnDataSource
from bokeh.models.tools import HoverTool



class DrawFunction():
	'''
		class to draw the figures 
	'''

	def __init__(self,train_index,ideal_index,msg=" train function,corresponding ideal function & mappable test points to the ideal function"):
		self.train_no=str(train_index)
		self.ideal_no=str(ideal_index)
		suffix=['st','nd','rd','th']
		title_name="The "+self.train_no+suffix[train_index-1]+msg
		self.p=figure(width=1000, height=1000,title = title_name,x_axis_label="x-values", y_axis_label="y-values")

	def plot_function(self,plot_name,df,clr,sz=2):
		'''
		plot the graph where plot_name='ideal','train' or 'test_map'
		df=dataframe object storing x-y values for train & ideal functions or test points for mappable test points
		clr=color of the figure
		sz=size of the figure
		'''

		x=df['x']
		name=plot_name
		if(plot_name=='train'):
			train_col='y'+self.train_no
			y=df[train_col]
			#self.p.line(x,y,line_width=0.5,color='green')
			TOOLTIPS = [("(x,y)", "($x, $y)")]
			'''
			TOOLTIPS ="""
			<div>
            <span style="font-size: 15px;">Training Function</span>
            <span style="font-size: 10px; color: #696;">($x, $y)</span>
        	</div>

			"""
			'''
			title_name='training function '+self.train_no
			self.p.circle(x,y,size=sz,color=clr,selection_color='deepskyblue',legend_label=title_name)
			#self.p.add_tools(HoverTool(tooltips=TOOLTIPS))
		elif(plot_name=='ideal'):
			ideal_col='y'+self.ideal_no
			y=df[ideal_col]
			TOOLTIPS = [("(x,y)", "($x, $y)")]
			'''
			TOOLTIPS ="""
			<div>
            <span style="font-size: 15px;">Ideal Function</span>
            <span style="font-size: 10px; color: #696;">($x, $y)</span>
        	</div>

			"""
			'''
			title_name='ideal function '+self.ideal_no
			self.p.circle(x,y,size=sz,color=clr,legend_label=title_name)
		elif(plot_name=='test_map'):
			y=df['y']
			TOOLTIPS = [("(x,y)", "($x, $y)")]
			'''
			TOOLTIPS ="""
			<div>
            <span style="font-size: 15px;">Test Point</span>
            <span style="font-size: 10px; color: #696;">($x, $y)</span>
        	</div>

			"""
			'''
			title_name='test points'
			self.p.circle(x,y,size=sz,color=clr,legend_label=title_name)
		self.p.add_tools(HoverTool(tooltips=TOOLTIPS))


	def display_functions(self):
		show(self.p)






def setup_plotting(output_file_name,ideal_df,ideal_no,train_df,train_no,test_map_df):
	'''
		plot ideal function, corresponding train function & mapped test points in a graph
		The graph is ouput in file 'output_file_name'

		ideal_df='ideal.csv' stored as dataframe object
		train_df='train.csv' stored as dataframe object
		test_map_df= dataframe object storing the test points corresponding to mappable ideal function 'ideal_no'
		ideal_no=number of the ideal function
		train_no=number of the train function

	'''

	output_file(output_file_name)
	graph=DrawFunction(train_no,ideal_no)
	graph.plot_function('train',train_df,'red',8)
	graph.plot_function('ideal',ideal_df,'green',4)
	graph.plot_function('test_map',test_map_df,'yellow',6)
	graph.display_functions()

   
	


	
	


	
	
	
	
	
	

