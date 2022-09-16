
Unit Test Func 1

#expected results for 'sq_dev' table when train_no=1 & ideal_no=1 (total rows returned=400, but show only top 3)
expected_result=[(366.4942943232564,), (361.2871812916248,), (342.6161118470057,)](use LIMIT)
#SELECT TOP 3 * FROM sq_dev

Unit Test Func 2

#expected results for 'sum_squared_dev' table for train_no=1 func with each of 50 ideal_no func(total rows returned=50, but show only top 3)
expected_result=[(54009.91619612495,), (53751.16933138226,), (94448.51480944248,)](use LIMIT)
#SELECT TOP 3 * FROM sum_squared_dev


Unit Test Func 3

#expected results for 'train_to_ideal' table for mapping  train_no=1 func to best fitting ideal func(total rows returned=1, show all)
expected_result=[(1, 11)]

#SELECT * FROM 'train_to_ideal'


1  11
2  27
3  24
4  20

Unit Test FUnc 4
expected_result=[11]


Unit Test FUnc 5
expected_result=[(1, 11, 0.4989410000000003)]
[(1, 11, 0.4989410000000003)]


Unit Test Func 6
expected_result=[(-17.4, -17.979618, 0.579618, 11)]









-17.4   -17.979618    0.579618   11

def plot_training_function(self,train_df):
 		'''
 		plot training function
 		'''
 		x=train_df['x']
 		train_col='y'+self.train_no
 		y=train_df[train_col]
 		self.p.line(x,y,line_width=0.5,color='green')
 		title_name='training function '+self.train_no
 		self.p.circle(x,y,size=2,color='red',legend=title_name)

 	def display_functions(self):
 		show(self.p)
