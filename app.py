import psycopg2
from flask import Flask, request, render_template, make_response
import csv
from io import StringIO

def get_connection():
	try:
		conn = psycopg2.connect(database="postgres", user='postgres', password='Anmol:123', host='34.93.36.117', port= '5432')
		conn.autocommit = True
		return conn
	except Exception as ex:
		print("Unable to connect to database due to {}".format(ex))
		raise Exception("Database connection failed!") from ex

def close_connection(conn, cursor):
	try:
		cursor.close()
		conn.close()
	except Exception as ex:
		print("Unable to close cursor/connection due to {}".format(ex))
		raise Exception("Chances of memory being leaked!") from ex

def get_results_per_filter_att(att_name, att_value):
	try:
		conn = get_connection()
		cursor = conn.cursor()
		query_string = "select phone_number from parsed_data.person_attributes where attribute_name = '{attribute_name}' and attribute_value = '{attribute_value}';".format(attribute_name=att_name, attribute_value=att_value)
		if att_name == "name":
			query_string = "select phone_number from parsed_data.person where name = '{}'".format(att_value)
		if att_name == "phone_number":
			return [(att_value,)]
		cursor.execute(query_string)
		results = cursor.fetchall()
		close_connection(conn, cursor)
		return results
	except Exception as ex:
		print("Unable to retrieve data due to {}".format(ex))
		raise Exception("Error while retrieving data!") from ex

def combine_results(filter_dict, table_headers, page_num=1, limit=10000):
	try:
		offset = (page_num - 1) * limit
		common_phone_numbers = set()
		att_names = set()
		att_values = set()
		for filter_att in filter_dict:
			att_names.add(filter_att)
			att_values.add(filter_dict[filter_att])
			phone_numbers = get_results_per_filter_att(filter_att, filter_dict[filter_att])
			resulted_ph_nos = set([phone_number_tup[0] for phone_number_tup in phone_numbers])
			if not resulted_ph_nos:
				return [{}]
			if common_phone_numbers:
				common_phone_numbers = common_phone_numbers.intersection(resulted_ph_nos)
			else:
				common_phone_numbers = resulted_ph_nos
		formatted_ph_nos = str(common_phone_numbers).replace("{", "").replace("}", "")
		final_results = [{}]
		if common_phone_numbers:
			conn = get_connection()
			cursor = conn.cursor()
			query_string = "select * from parsed_data.person as t1 INNER JOIN parsed_data.person_attributes as t2 on t1.phone_number = t2.phone_number where t2.phone_number IN ({phone_nums}) ORDER BY t2.phone_number LIMIT {limit} OFFSET {offset};".format(phone_nums=formatted_ph_nos, limit=limit, offset=offset)
			cursor.execute(query_string)
			results = cursor.fetchall()
			close_connection(conn, cursor)
			final_results = []
			start_idx = 0
			if len(results) < limit:
				limit = len(results)
			while start_idx < limit:
				phone_number = results[start_idx][0]
				tmp = {"phone_number": phone_number, "name": results[start_idx][1], "source_file": results[start_idx][2]}
				pos = start_idx
				while pos<limit and results[pos][0] == phone_number:
					# print(results[pos])
					table_headers.add(results[pos][5])
					tmp.update({results[pos][5]: results[pos][6]})
					pos = pos + 1
				start_idx = pos
				final_results.append(tmp)
		return final_results
	except Exception as ex:
		print("Unable to combine data due to {}".format(ex))
		raise Exception("Error while combining data!") from ex

def get_distinct_filter_atts():
	try:
		conn = get_connection()
		cursor = conn.cursor()
		cursor.execute("select distinct attribute_name from parsed_data.person_attributes;")
		results = cursor.fetchall()
		close_connection(conn, cursor)
		results = [ result[0] for result in results ]
		return results
	except Exception as ex:
		print("Unable to retrieve distinct filter attributes due to {}".format(ex))
		raise Exception("Error while retrieving filter attributes!") from ex

def get_filter_att_distinct_values(att_name):
	try:
		conn = get_connection()
		cursor = conn.cursor()
		cursor.execute("select distinct attribute_value from parsed_data.person_attributes pa where attribute_name = '{filter}';".format(filter=att_name))
		results = cursor.fetchall()
		close_connection(conn, cursor)
		results = [ result[0] for result in results ]
		return results
	except Exception as ex:
		print("Unable to retrieve distinct filter attribute values due to {}".format(ex))
		raise Exception("Error while retrieving filter attribute values!") from ex


# Note: These are not global variables, it's value can be accessed and modified but can't be changed with global aspect
filter_att_val_dict = {}
filter_headers = get_distinct_filter_atts()
unwanted_filters = ["name", "father_name", "father name", "phone_number", "roll_no", "date_of_birth", "email", "email2", "pincode", "application_no", "area", "address"]
for unwanted_header in unwanted_filters:
	if unwanted_header in filter_headers:
		filter_headers.remove(unwanted_header)
for header in filter_headers:
	filter_values = get_filter_att_distinct_values(header)
	filter_att_val_dict.update({header: filter_values})

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return render_template('basic_table.html', title='Query Data', filter_atts=filter_att_val_dict)

@app.route('/submit_query', methods=['POST'])
def get_query_results():
    request_data = request.form.to_dict()
    request_data = {k:v for k,v in request_data.items() if v not in ['ANY VALUE', '']}
    if not request_data:
    	return render_template('basic_table.html', title='Basic Table', table_headers=filter_headers,
    		filter_atts=filter_att_val_dict)
    table_headers = set(["phone_number", "name", "source_file"])
    query_result = combine_results(request_data, table_headers, 1)
    table_headers = list(table_headers)
    table_headers.sort()
    final_query_result = []
    for item in query_result:
    	final_query_result.append([item.get(key, "N/A") for key in table_headers])
    global QUERY_RESULT_HEADERS, QUERY_RESULT_DATA
    QUERY_RESULT_HEADERS = table_headers
    QUERY_RESULT_DATA = final_query_result
    return render_template('basic_table.html', title='Query Data', table_headers=table_headers,
    	table_data_values=final_query_result, filter_atts=filter_att_val_dict)

@app.route('/download_data', methods=['POST'])
def download_data():
	si = StringIO()
	cw = csv.writer(si)
	cw.writerow(QUERY_RESULT_HEADERS)
	cw.writerows(QUERY_RESULT_DATA)
	output = make_response(si.getvalue())
	output.headers["Content-Disposition"] = "attachment; filename=data.csv"
	output.headers["Content-type"] = "text/csv"
	return output

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)