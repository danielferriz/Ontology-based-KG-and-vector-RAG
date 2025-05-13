from base_logger import logger
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import adapt
import pandas as pd

from tools import cleanWords, handle_logs
from lmstudio import get_embedding
from re import sub

def create_connection(config):
	"""Create a database connection to PostgreSQL"""
	connection = None
	try:
		connection = psycopg2.connect(
			database=config.db_name,
			user=config.db_user,
			password=config.db_password,
			host=config.db_host,
			port=config.db_port,
		)
		logger.info("Connection to PostgreSQL DB successful")
	except OperationalError as e:
		logger.critical(f"The error '{e}' occurred")
	return connection

def escape_string_for_sql(string_original:str, add_single_quotes:bool = False) -> str:
	string_original = sub(r'[^\x00-\xFF]', '', string_original)
	cleaned_string = str(adapt(string_original)).replace("\n", " ")
	cleaned_string = sub(r'\s+', ' ', cleaned_string.strip())
	if not add_single_quotes:
		cleaned_string=cleaned_string[1:-1]
	return cleaned_string


def create_insert_prompt_tables(config, connection):
	try:
		if not execute_sql_file(connection, config.createTables_sql):
			return handle_logs(504,"Due to this error, the SQL tables couldn't be updated",logger.CRITICAL)
		_converters={'prompt_id':int,'general_prompt_id':int, 'sequence_id':int, 'lang':str, 'type': str, 'description':str, 'prompt':str, 'variables':str  }
		df = pd.read_excel(config.prompts_xlsx, converters=_converters)
		df.fillna('', inplace=True)
		query = """ INSERT INTO Prompts ( 
			prompt_id, 		general_prompt_id,	sequence_id, 	lang, 	type, 
			description, 	prompt, 			variables )
		VALUES 
		"""
		string_columns = [k for k, v in _converters.items() if v==str]
		logger.debug(f"string_columns: {string_columns}")
		for col in string_columns:
			df[col]=df[col].apply(lambda x: escape_string_for_sql(x, add_single_quotes=True))

		for i in range(len(df)):
			insert_value=f""" ( {df.iloc[i]['prompt_id']}, {df.iloc[i]['general_prompt_id']}, 
								{df.iloc[i]['sequence_id']}, {df.iloc[i]['lang']}, {df.iloc[i]['type']},
								{df.iloc[i]['description']}, {df.iloc[i]['prompt']}, 
								{df.iloc[i]['variables']} )"""
			query+=insert_value+', '

		query=query[:-2]+'; '
		if execute_non_query(connection, query)<-1:
			return handle_logs(505,"Error while inserting values in table 'Prompts'",logger.CRITICAL)

		df = pd.read_excel(config.examples_xlsx, converters={'example_id':int, 'general_example_id':int, 'sequence_id':int, 'lang':str, 'prompt_id':int, 'example':str, 'variables':str })
		df.fillna('', inplace=True)
		query = """ INSERT INTO Examples ( 
			example_id, 		general_example_id,	sequence_id, 	lang, 	prompt_id, 
			example, 			variables )
		VALUES 
		"""
		string_columns = [col for col in df.columns if df[col].dtype == 'object' or df[col].dtype.name == 'string']
		for col in string_columns:
			df[col]=df[col].apply(lambda x: escape_string_for_sql(x, add_single_quotes=True))

		for i in range(len(df)):
			insert_value=f""" ( {df.iloc[i]['example_id']}, {df.iloc[i]['general_example_id']}, 
								{df.iloc[i]['sequence_id']}, {df.iloc[i]['lang']}, {df.iloc[i]['prompt_id']},
								{df.iloc[i]['example']}, {df.iloc[i]['variables']} )"""
			query+=insert_value+', '

		query=query[:-2]+'; '
		if execute_non_query(connection, query)<-1:
			return handle_logs(506,"Error while inserting values in table 'Examples'",logger.CRITICAL)
		# Uncomment for debugging
		#prompt=select_prompt(connection, config, 5, variables={'nodeLeftName':'nodeLeftName', 'nodeRightName':'nodeRightName', 'relation':'relation', 'comment': 'comment'})
		#print(prompt)
	
	except Exception as ex:
		return handle_logs(503, f"Error in line {ex.__traceback__.tb_lineno}: {str(ex)}", logger.CRITICAL)

	return handle_logs()

def execute_query(connection, query, df_columns):
	"""Execute a query and return results as a pandas DataFrame"""
	cursor = connection.cursor()
	df = None
	try:
		cursor.execute(query)
		df = pd.DataFrame(cursor.fetchall(), columns=df_columns)
	except Exception as e:
		logger.error(f"The error '{e}' occurred. Rolling back...")
	finally:
		cursor.close()
	return df

def initialize_vector_table(connection, config):
	logger.debug("Initializing vector table. This should not be running every search!")

	query="DROP TABLE IF EXISTS Vectors CASCADE;"
	if execute_non_query(connection, query)<-1:
		return handle_logs(501,"Error while dropping vector table",logger.CRITICAL)

	query="CREATE EXTENSION IF NOT EXISTS vector;"
	if execute_non_query(connection, query)<-1:
		return handle_logs(502,"Failure at creating vector extension",logger.CRITICAL)

	query=f"""
		CREATE TABLE Vectors (
			chunk_id    VARCHAR(100) PRIMARY KEY,
			filename    TEXT,
			chunk       TEXT,
			embedding   vector({config.llm_embedding_vector_len})
		);
	"""
	if execute_non_query(connection, query)<-1:
		return handle_logs(503,"Error while creating vector table",logger.CRITICAL)

	return handle_logs()

def execute_non_query(connection, query):
	"""Execute a non-SELECT query (INSERT, UPDATE, DELETE, etc.)"""
	cursor = connection.cursor()
	row_count = -1
	try:
		cursor.execute(query)
		connection.commit()
		logger.debug("Query executed successfully. Committing changes...")
		# Get number of affected rows
		row_count = cursor.rowcount
		logger.debug(f"Affected rows: {row_count}")
	except Exception as e:
		logger.error(f"The error '{e}' occurred. Rolling back...")
		connection.rollback()
		row_count = -2
	finally:
		cursor.close()
	return row_count

def execute_sql_file(connection, sql_file):
	"""Execute a non-SELECT query (INSERT, UPDATE, DELETE, etc.)"""
	cursor = connection.cursor()
	sql_successful=True
	try:
		cursor.execute(open(sql_file, "r").read())
		connection.commit()
		logger.debug("Query executed successfully. Committing changes...")
	except Exception as e:
		logger.error(f"The error '{e}' occurred. Rolling back...")
		connection.rollback()
		sql_successful=False
	finally:
		cursor.close()
	return sql_successful

def insert_chunks_with_vectors(connection, chunk):
	new_text=escape_string_for_sql(chunk['text'], add_single_quotes=False)
	query=f"""
		INSERT INTO Vectors (
			chunk_id, 				filename, 				chunk, 			embedding
		) VALUES (
			'{chunk['chunkId']}', 	'{chunk['filename']}', 	'{new_text}', '{chunk['embedding']}'
		);
	"""
	if execute_non_query(connection, query)<-1:
		return handle_logs(504,"Error while inserting in vector table",logger.CRITICAL)

	return handle_logs()

def cosine_vector_search(connection, config, vector):
	query = f"""
	SELECT chunk_id, filename, chunk FROM Vectors
	ORDER BY embedding <=> '{vector}'
	FETCH FIRST {config.k_most_similar} ROW ONLY;
	"""
	df = execute_query(connection, query, ['chunk_id', 'filename', 'chunk'])
	return df

def llm_input_string(config, connection, df, table_name, general_id_var_name, general_id, df_var_field_name, df_llm_input_field_name, variables):
	if len(df)==0:
		query = f"""
		SELECT DISTINCT lang
		FROM {table_name}
		WHERE {general_id_var_name}={general_id};
		"""
		df = execute_query(connection, query, ['lang'])
		if df is None:
			logger.error('Failed at retrieving data from database when searching for valid languages.')
		elif len(df)==0:
			logger.error(f"{general_id_var_name} {general_id} does not have registered prompts on any language")
		else:
			logger.error(f"There aren't prompts of required operation in language {config.language}. However, the following can be used {df['lang'].tolist()}")
		return ''

	prompt=""
	df.fillna('',inplace=True)
	for i in range(len(df)):
		subprompt_variables = {}
		skip_prompt=False
		if df.iloc[i][df_var_field_name]:
			sql_variables=df.iloc[i][df_var_field_name].split(',')
			for sql_variable in sql_variables:
				if sql_variable.strip() not in variables.keys():
					logger.warning(f'Missing variable {sql_variable.strip()}. Skipping prompt...')
					skip_prompt=True
				else:
					subprompt_variables[ sql_variable.strip() ]=variables[ sql_variable.strip() ]
		if not skip_prompt:
			partial_prompt=df.iloc[i][df_llm_input_field_name]
			if subprompt_variables:
				partial_prompt=partial_prompt.format_map(subprompt_variables)
			prompt+=partial_prompt+'\n'
	return prompt

def select_prompt(connection, config, general_prompt_id, variables={}):
	query = f"""
	SELECT prompt_id, sequence_id, prompt, variables
	FROM Prompts
	WHERE general_prompt_id={general_prompt_id} AND lang='{config.language}'
	ORDER BY sequence_id;
	"""
	df = execute_query(connection, query, ['prompt_id', 'sequence_id', 'prompt', 'variables'])
	if df is None:
		logger.error('Failed at retrieving data from database when searching for related prompts .')
		return ''
	if 'examples' in df['variables'].unique().tolist() and 'examples' not in variables:
		variables['examples']=select_example(connection, config, general_prompt_id, variables )
	prompt=llm_input_string(config, connection, df, 'Prompts', 'general_prompt_id', general_prompt_id, 'variables', 'prompt', variables)
	return prompt

def select_example(connection, config, general_prompt_id, variables={} ):
	query=f"""
	SELECT 		P.prompt_id AS prompt_id, E.example_id AS example_id, 
				E.general_example_id AS general_example_id,
				E.sequence_id AS sequence_id, E.example AS example,
				E.variables AS variables
	FROM 		Examples E
	INNER JOIN 	Prompts P ON E.prompt_id=P.prompt_id
	WHERE P.general_prompt_id={general_prompt_id} AND E.lang='{config.language}'
	ORDER BY E.sequence_id;
	"""
	df = execute_query(connection, query, ['prompt_id', 'example_id', 'general_example_id', 'sequence_id', 'example', 'variables'])
	if df is None:
		logger.error('Failed at retrieving data from database when searching for related examples .')
		return ''
	
	prompt=""
	for general_id in df['general_example_id'].unique().tolist():
		example_text=llm_input_string(config, connection, df, 'Examples', 'general_example_id', general_id, 'variables', 'example', variables)
		if example_text:
			if not prompt:
				prompt='['
			prompt+=example_text+', '
	if prompt:
		prompt=prompt[:-2]+']'
	
	return prompt
