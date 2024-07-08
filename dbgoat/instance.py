import time
from typing import Callable, Union, Literal, List
from collections import namedtuple

from mysql.connector import (
	connection as mysql_conn, 
	Error as mysql_error, 
	errorcode as mysql_errorcode
)


class DBInstance:
	def __init__(self, *args, **kwargs):
		# Declarar atributos compartidos
		self.cnx = None  # connection
		self.status = None
		self.db_name = kwargs.get('db_name')
		self.schema = kwargs.get('schema')


	def _connect(self):
		raise Exception('You must create a "connect" function in subclass')


	def write(self, query, params=None, many=False):
		raise Exception('You must create a "write" function in subclass')


	def read(self, query, params=None, many=False):
		raise Exception('You must create a "read" function in subclass')


	def initialize(self):
		raise Exception('You must create a "initialize" function in subclass')


	def delete(self):
		raise Exception('You must create a "delete" function in subclass')


	def __del__(self):
		# print('Connection closed.')
		# if self.cnx:
		# 	self.cnx.close() # Produces error
		pass



class MySQLDBInstance(DBInstance):
	def __init__(
			self, host='', port='', user='', password='', 
			database='', schema=None
		):
		super().__init__(db_name=database, schema=schema)
		self._connect(host, port, user, password, database)


	def _connect(self, host, port, user, password, database):
		try:
			cnx = mysql_conn.MySQLConnection(
				host=host, port=port, user=user,
				password=password, database=database)
		except mysql_error as err:
			if err.errno == mysql_errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
			elif err.errno == mysql_errorcode.ER_BAD_DB_ERROR:
				print(f"Database '{database}' does not exist")
			else:
				print(err)
		else:
			print(f"Connection established to MySQL Database named: '{database}'")
			self.cnx = cnx


	def clear(self, delay=1):
		# Reversed order to evade constraints
		tables = reversed(list(self.schema['tables'].keys()))
		for table in tables:
			try:
				self.write(f'DROP TABLE {table}')
				print(f'Table {table} successfully dropped')
			except Exception as e:
				print(f'Skipped {table} table drop')
				print(e, '\n')
				#print(e)
			time.sleep(delay)


	def initialize(self, delay=1):
		self.clear(delay=delay)
		for table, query in self.schema['tables'].items():
			try:
				self.write(query)
				print(f'Table {table} successfully created')
			except Exception as e:
				print(f'Skipped {table} table creation')
				print(e, '\n')
			time.sleep(delay)


	def delete(self):
		self.write(f'DROP DATABASE IF EXISTS {self.db_name}')
		print(f'Database {self.db_name} successfully deleted')


	def write(self, query, params=None, many=False, multi=False):
		cur = self.cnx.cursor()
		if many:
			cur.executemany(query, params)
		else:
			cur.execute(query, params, multi=multi)

		self.cnx.commit()
		cur.close()


	def createColumn(
		self, 
		table_name: str,
		col_name: str, 
		data_type: str, 
		options: Union[str, None] = None,
		position: Literal['first', 'last'] = 'last'
	) -> None:
		statement = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {data_type}"

		if options is not None:
			statement += f" {options}"

		if position == 'first':
			statement += " FIRST"
		self.write(statement)


	def read(self, query, params=None, many=False, multi=False):
		cur = self.cnx.cursor()
		if many:
			pass
		else:
			cur.execute(query, params, multi=multi)

		results = cur.fetchall()
		cur.close()
		return results
	

	def listAllColumns(self, table: str) -> List[namedtuple]:
		query = f"SHOW COLUMNS FROM {table}"
		res = self.read(query)

		Column = namedtuple('Column', 'name, type')
		columns_list = [Column(name=item[0], type=item[1]) for item in res]
		
		return columns_list


	def listAllTables(self, type: Literal['base', 'view', 'all'] = 'all') -> List[str]:
		query = "SHOW FULL TABLES"
		if type == 'base':
			query += " WHERE Table_type = 'BASE TABLE'"
		elif type == 'view':
			query += " WHERE Table_type = 'VIEW'"
		
		res = self.read(query)
		tables_list = [item[0] for item in res]
		return tables_list


	def read_to_pandas(self, read_sql: Callable, query: str, params=None):
		df = read_sql(query, self.cnx, params=params)
		return df


class SQLiteDBInstance(DBInstance):
	pass
