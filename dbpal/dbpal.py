import time
import sqlite3
import subprocess
from abc import ABC, abstractmethod

from mysql.connector import (
	connection as mysql_conn, 
	Error as mysql_error, 
	errorcode as mysql_errorcode)

from scripts.db_schema import SCHEMAS
from scripts.creds import creds


class DBAdmin(ABC):
	"""This class is used to create, delete, duplicate and
	rename databases"""
	def __init__(self, cred_entry, *args, **kwargs):
		self.tools = dict()
		self.options_flags = {
			'host': None,
			'port': None,
			'user': None,
			'password': None,
			'database': None,
			'statement': None
		}
		self.type = kwargs['type']
		self.options_values = creds[cred_entry]

	def buildCommand(self, tool, **kwargs):
		# Build command
		executable = self.tools[tool]
		cmd_chain = [executable]

		# Combines the default options with the ones passed as arguments
		options = self.options_values.copy()
		options.update(kwargs)

		def flatAppend(flat_list, something):
			if isinstance(something, list):
				for item in something:
					if isinstance(item, list):
						flatAppend(flat_list, item)
					else:
						flat_list.append(item)
			else:
				flat_list.append(something)
		
		# Compose default commands
		for key, value in options.items():
			if key in self.options_flags.keys():
				if value:
					cmd_chain.append(f'{self.options_flags[key]}={value}')
				else:
					cmd_chain.append(self.options_flags[key])
			elif key.startswith('__'):
				key = key.replace('_', '-')
				if value is not None:
					cmd_chain.append(f'{key}={value}')
				else:
					cmd_chain.append(key)
			else:
				flatAppend(cmd_chain, value)
		
		return cmd_chain

	def issueCommand(self, tool, shell=False, **kwargs):
		"""This function issues a command using one of
		the tools in self.tools
		Example:
		For dumping a database
			issueCommand(
				'export',
				database='test',
				output='test.sql
			)
		"""
		# Build command
		cmd_chain = self.buildCommand(tool, **kwargs)
		# Execute command
		print('Executing the following command:')
		print(cmd_chain, '\n')
		subprocess.run(cmd_chain, shell=shell)

	@abstractmethod
	def create(self, db_name):
		pass

	@abstractmethod
	def delete(self, db_name):
		pass

	@abstractmethod
	def rename(self, db_name, db_new_name):
		pass
	
	@abstractmethod
	def duplicate(self, db_name, db_new_name):
		pass

	@abstractmethod
	def export(self, db_name, output_file):
		pass


class MySQLDBAdmin(DBAdmin):
	def __init__(self, *args, **kwargs):
		kwargs['type'] = 'mysql'
		super().__init__(*args, **kwargs)

		self.tools.update({
			'main': 'mysql',
			'admin': 'mysqladmin',
			'export': 'mysqldump',
		})

		self.options_flags.update({
			'host': '--host',
			'port': '--port',
			'user': '--user',
			'password': '--password',
			'database': '--database',  # For connecting
			'statement': '--execute',
			'databases': '--databases',  # For exporting
			'output': '--result-file'
		})

	def create(self, db_name):
		"""Create a MySQL database"""
		self.issueCommand(
			'main',
			statement=f'CREATE DATABASE {db_name}'
		)

	def delete(self, db_name):
		"""Delete a MySQL database"""
		self.issueCommand(
			'main',
			statement=f'DROP DATABASE IF EXISTS {db_name}'
		)
	
	def backup(self, output_file='backup.sql'):
		"""Backup all databases to a file"""
		self.issueCommand(
			'export',
			output=output_file,
			__column_statistics=0,
			__all_databases=None,
		)
	
	def export(self, db_name, output_file=None):
		"""Export one MySQL database
		The resulting file does not have either:
		- the CREATE DATABASE statement
		- the USE statement
		"""
		if output_file is None:
			output_file = db_name
		
		if not output_file.endswith('.sql'):
			output_file += '.sql'

		self.issueCommand(
			'export',
			output=output_file,
			__column_statistics=0,
			db_name=db_name,
		)
	

	
	def restore(self, input_file, db_name):
		"Restore a MySQL database from a file"

		self.create(db_name)

		"""Restore one MySQL database from a file"""
		cmd_restore = self.buildCommand(
			'main',
			db_name=db_name
		)

		cmd_restore.append(f'< {input_file}')

		cmd_restore = ' '.join(cmd_restore)

		# print(cmd_restore)
		subprocess.run(cmd_restore, shell=True)


	def rename(self, db_name, db_new_name):
		# TODO: Fix this. It doesn't work

		# Create a new database with new name
		self.create(db_new_name)

		# Get all table names from old database
		cmd_dump = self.buildCommand(
			'export',
			__column_statistics=0,
			databases=None,
			db_name=db_name,
		)

		cmd_import = self.buildCommand(
			'main',
			database=db_new_name
		)

		cmd = f'{" ".join(cmd_dump)} | {" ".join(cmd_import)}'
		print(cmd)

		subprocess.run(cmd, shell=True)

		# Move tables from old database to new database


	
	def duplicate(self, db_name, db_new_name):
		pass


class DBInstance:
	def __init__(self, *args, **kwargs):
		# Declarar atributos compartidos
		self.cnx = None  # connection
		self.status = None

	def connect(self):
		raise Exception('You must create a "connect" function in subclass')

	def write(self, query, params=None, many=False):
		raise Exception('You must create a "write" function in subclass')

	def read(self, query, params=None, many=False):
		raise Exception('You must create a "read" function in subclass')

	def initialize(self):
		raise Exception('You must create a "initialize" function in subclass')

	def __del__(self):
		print('Connection closed.')
		if self.cnx:
			self.cnx.close()


class SQLiteDBInstance(DBInstance):
	def __init__(self, path=None):
		super().__init__()
		self.path = path
		self.schema = SCHEMAS['sqlite']

		self.connect()

	def initialize(self):
		query = self.schema['tables']['Records']
		self.write(query)

	def connect(self):
		# Inicializar conexión
		try:
			if self.path:
				cnx = sqlite3.connect(self.path)
			else:
				cnx = sqlite3.connect(':memory:')
		except Exception as e:
			print(e)
			if cnx: cnx.close()
		else:
			print(cnx)
			self.cnx = cnx

	def write(self, query, params='', many=False):
		cur = self.cnx.cursor()
		if many:
			pass
		else:
			cur.execute(query, params)

		self.cnx.commit()
		cur.close()

	def read(self, query, params='', many=False):
		cur = self.cnx.cursor()
		if many:
			pass
		else:
			cur.execute(query, params)

		results = cur.fetchall()
		cur.close()
		return results


class MySQLDBInstance(DBInstance):
	def __init__(self, host='', port='', user='', password='', database=''):
		super().__init__()
		self.schema = SCHEMAS['mysql']
		self.connect(host, port, user, password, database)

	def connect(self, host, port, user, password, database):
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


	def write(self, query, params=None, many=False, multi=False):
		cur = self.cnx.cursor()
		if many:
			cur.executemany(query, params)
		else:
			cur.execute(query, params, multi=multi)

		self.cnx.commit()
		cur.close()

	def read(self, query, params=None, many=False, multi=False):
		cur = self.cnx.cursor()
		if many:
			pass
		else:
			cur.execute(query, params, multi=multi)

		results = cur.fetchall()
		cur.close()
		return results

	def read_to_pandas(self, read_sql, query, params=None, many=False):
		cur = self.cnx.cursor()
		if many:
			pass
		else:
			#cur.execute(query, params)
			df = read_sql(query, self.cnx)

		#results = cur.fetchall()
		cur.close()
		return df
