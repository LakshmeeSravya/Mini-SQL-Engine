import sys
import re
import sqlparse


table_dict = {}
join_cond = []

def str_to_int(line):
	val = line.strip('"')
	val = val.strip()
	val = val.split(",")
	ans = []
	for value in val:
		temp = value.replace(",","")
		ans.append(int(temp.strip()))
	return ans

def query_conversion(query):
	query = query.strip('"').strip()
	query = query.replace("SELECT ", "select ")
	query = query.replace("DISTINCT ", "distinct ")
	query = query.replace("FROM ", "from ")
	query = query.replace("WHERE ", "where ")
	query = query.replace("AND ", "and ")
	query = query.replace("OR ", "or ")
	query = query.replace("MIN", "min")
	query = query.replace("MAX", "max")
	query = query.replace("AVG", "avg")
	query = query.replace("SUM", "sum")
	return query


def read_data():
	with open("./metadata.txt","r") as f:
		while True:
			line = f.readline().strip()
			if line == "<begin_table>":
				table_name = f.readline().strip()
				table_dict[table_name] = {}
				table_dict[table_name]['name'] = table_name
				table_dict[table_name]['info'] = []
				table_dict[table_name]['table']=[]
				while True:
					attr = f.readline().strip()
					if attr == "<end_table>":
						break
					else:
						table_dict[table_name]['info'].append(attr)
			if line == "":
				break
			
	for table_name in table_dict.keys():
		with open("./" + table_name + ".csv","r") as f:
			for line in f:
				conv_line = str_to_int(line)
				table_dict[table_name]['table'].append(conv_line)


def semicolon_error(query):
	if str(query[-1]) == ';' or str(query[-1])[-1] ==';':
		return False
	else:
		return True
		
def format_error(query):
	if not re.match('^select.*from.*', query):
		return True
	return False

def check_field_validity(column_list,tables):
	for field in column_list:
		field_flag = 0
		field_val = field.split(".")
		if len(field_val) == 2:
			if field_val[1] in table_dict[field_val[0]]['info'] and field_val[0] in tables:
				field_flag += 1
		else:
			for table in tables:
				if field_val[0] in table_dict[table]['info']:
					field_flag +=1

		if field_flag != 1:
			return 0
	return 1 

def display_result(table):
	print(','.join(table['info']))
	for row in table['table']:
		print(','.join([str(x) for x in row]))


def cartesian_prod(table1,table2):
	product_table = {}
	product_table['table']=[]
	product_table['info']=[]

	for col in table1['info']:
		if len(col.split('.')) == 1:
			product_table['info'].append(table1['name'] + '.' + col)
		else:
			product_table['info'].append(col)

	for col in table2['info']:
		if len(col.split('.')) == 1:
			product_table['info'].append(table2['name'] + '.' + col)
		else:
			product_table['info'].append(col)

	for row1 in table1['table']:
		for row2 in table2['table']:
			product_table['table'].append(row1 + row2)

	return product_table

def check(lhs, op, rhs):
        if op == '=':
            return lhs == rhs
        elif op == '>':
            return lhs > rhs
        elif op == '<':
            return lhs < rhs
        elif op == '>=':
            return lhs >= rhs
        elif op == '<=':
            return lhs <= rhs
        elif op == '<>':
            return lhs != rhs
        elif op == '==':
        	return lhs == rhs

def select(tables,condition_str):
	result_table = {}
	join_table = {}
	if len(tables) == 1:
		join_table = cartesian_prod(table_dict[tables[0]], {'info': [], 'table': [[]]})
	else:
		join_table = cartesian_prod(table_dict[tables[0]],table_dict[tables[1]])

	result_table['info'] = []
	for x in join_table['info']:
		result_table['info'].append(x)

	condition_str = re.sub('(?<=[\w ])(=)(?=[\w ])', '==', condition_str)
	conditions = condition_str.replace(" and ", ",").replace(" or ", ",").replace('(', '').replace(')', '')
	conditions = conditions.split(',')

	for condition in conditions:
		if bool(re.match('.*==.*[a-zA-Z]+.*', condition.strip())):
			condition = condition.strip()
			temp1 = condition.split('==')[0].strip()
			temp2 = condition.split('==')[1].strip()
			join_cond.append((temp1,temp2))
	for field in join_table['info']:
		condition_str = condition_str.replace(field, 'row[' + str(join_table['info'].index(field)) + ']')
	lhs = ""
	op = ""
	rhs = ""
	
	for i in range(len(condition_str)):
		if condition_str[i] == "=" or condition_str[i] == "<" or condition_str[i] == ">":
			op+=condition_str[i]
			if condition_str[i+1] == "=" or condition_str[i+1] == "<" or condition_str[i+1] == ">":
				op+=condition_str[i]
				for j in range(i+2,len(condition_str)):
					rhs+=condition_str[j]
			else:
				for j in range(i+1,len(condition_str)):
					rhs+=condition_str[j]
			break
		else:
			lhs += condition_str[i]
	lhs = lhs.strip()
	op = op.strip()
	rhs = rhs.strip()
	if op == "=":
		op = op.replace("=","==")
	condition_str = lhs + op + rhs


	result_table['table'] = []
	for row in join_table['table']:
		if eval(condition_str):
			result_table['table'].append(row)

	return result_table

def project(table,column_list,dist_flag,aggr_flag):
	final_table = {}
	final_table['info'] = []
	final_table['table'] = []

	if aggr_flag is not None:
		value = aggr_flag + "(" + column_list[0] + ")"
		final_table['info'].append(value)

		col_index = table['info'].index(column_list[0])

		temp_col = []
		for val in table['table']:
			temp_col.append(val[col_index])
		final = []
		if aggr_flag == "sum":
			final.append(sum(temp_col))
		elif aggr_flag == "min":
			final.append(min(temp_col))
		elif aggr_flag == "max":
			final.append(max(temp_col))
		elif aggr_flag == "avg":
			final.append((1.0*sum(temp_col))/len(temp_col))
		else:
			print("Invalid Function")
			return 

		final_table['table'].append(final)
	else:
		if column_list[0] == '*':
			temp = []
			for x in table['info']:
				temp.append(x)
			column_list[:] = temp[:]

			for field_pair in join_cond:
				temp[:] = []
				for x in column_list:
					if x != field_pair[1]:
						temp.append(x)

				column_list[:] = temp[:]
		final_table['info'] += column_list
		field_indices = []

		for field in column_list:
			ind = table['info'].index(field)
			field_indices.append(ind)

		for row in table['table']:
			result_row = []
			for i in field_indices:
				result_row.append(row[i])
			final_table['table'].append(result_row)

		if dist_flag:
			temp = sorted(final_table['table'])
			final_table['table'][:] = []
			for i in range(len(temp)):
				if i == 0 or temp[i] != temp[i-1]:
					final_table['table'].append(temp[i])	
	return final_table


def parse(query):
	dist_flag = None
	aggr_flag = None
	star_flag = False

	parsed_query = sqlparse.parse(query)
	parsed_query = parsed_query[0].tokens
	parse_val = []

	for val in parsed_query:
		if str(val) != " ":
			parse_val.append(val)

	if semicolon_error(parse_val):
		print("Semicolon missing.")
		return
	
	query = query.strip(';')
	if format_error(query):
		print("Invalid query")
		return

	parse_val.remove(parse_val[0])
	from_idx = None
	
	for i in range(len(parse_val)):
		if str(parse_val[i]) =='from':
			from_idx = i
			break

	if from_idx > 2 or from_idx == 0:
		print("Invalid Syntax")
		return

	
	column_list = []

	if from_idx == 2:
		if str(parse_val[0]) == 'distinct':
			dist_flag = True
			column_list = str(parse_val[1]).split(",")
		else:
			print("Invalid Syntax")
			return

	elif from_idx == 1:
		if bool(re.match('^(sum|max|min|avg)\(.*\)', str(parse_val[0]))):
			term = str(parse_val[0])
			aggr_flag = term.split("(")[0].strip()
			term = term.replace(aggr_flag+"(","")
			term = term.replace(")","")
			column_list = term.split(",")
			
			if len(column_list) > 1:
				print("Invalid Syntax - Too many arguments.")
			elif len(column_list) == 0:
				print("Invalid Syntax - Too few arguments")

		elif bool(re.match('^distinct.*', str(parse_val[0]))):
			dist_flag = True
			term  = str(parse_val[0])

			term = term.replace("distinct(","")
			term = term.replace(")","")
			column_list = term.split(",")
			if len(column_list) == 0:
				print("Invalid Syntax - Too few arguments")
		else:
			column_list = str(parse_val[0]).split(",")

	table_list = []

	if(str(parse_val[-1]) == ";"):
		parse_val.remove(parse_val[-1])
	if len(parse_val) <= from_idx+1:
		print("Invalid Syntax")
		return 

	table_list = str(parse_val[from_idx+1]).split(",")

	for table in table_list:
		if table not in table_dict:
			print("No such table exist in database")
			return
	
	if column_list[0] == '*':
		star_flag = True

	if star_flag is True:
		column_list = []
		for table in table_list:
			for val in table_dict[table]['info']:
				if len(table_list)==1:
					column_list.append(val)
				else:
					column_list.append(table + "." + val)		
	value = check_field_validity(column_list,table_list)
	if not value:
		print("Field error")
		return 0

	if len(parse_val) > from_idx + 3:
		print("Invalid Syntax")
		return

	if len(parse_val) == from_idx + 3:
		condition_str = str(parse_val[-1]).strip()
		condition_str = condition_str.replace(";","")
		condition_str = condition_str.replace("where","")
		temp = condition_str
		temp = temp.replace(' and ', ' ').replace(' or ', ' ')
		cond_cols = re.findall(r"[a-zA-Z][\w\.]*", temp)
		cond_cols = list(set(cond_cols))
		if check_field_validity(cond_cols, table_list) == 0:
			print("field error")
			return

		for field in cond_cols:
			if len(field.split('.')) == 1:
				for table in table_list:
					if field not in table_dict[table]['info']:
						continue
					else:
						temp1 = table + '.' + field
						temp2 = ' ' + condition_str
						condition_str = condition_str.replace(field,temp1)
						condition_str = condition_str.strip(' ')
		column_list = change_field(column_list,table_list)
		display_result(project(select(table_list, condition_str), column_list, dist_flag, aggr_flag))
	else:
		if len(table_list) == 2:
			column_list = change_field(column_list,table_list)
			join_table = cartesian_prod(table_dict[table_list[0]],table_dict[table_list[1]])
			display_result(project(join_table, column_list, dist_flag, aggr_flag)) 
		else:
			display_result(project(table_dict[table_list[0]], column_list, dist_flag, aggr_flag)) 



def change_field(column_list,table_list):
	new_list = []
	for val in column_list:
		value = val.split('.')
		if len(value)==1:
			for table in table_list:
				if val in table_dict[table]['info']:
					new_list.append(table + '.' + val)
					break
		else:
			new_list.append(val)
	return new_list
if __name__ == "__main__":
	read_data();
	query = sys.argv[1]
	query = query_conversion(query)
	
	parse(query)