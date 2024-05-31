import subprocess
from abc import ABC, abstractmethod
import warnings
import re
import logging

from mysql.connector import (
	connection as mysql_conn,
	Error as mysql_error,
	errorcode as mysql_errorcode
)

logger = logging.getLogger('dbgoat')


class DBAdmin(ABC):
	"""This class is used to create, delete, duplicate and
	rename databases"""
	def __init__(self, creds, *args, **kwargs):
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
		self.options_values = creds
		self.cnx = None
		self._connect(**creds)


	def closeConnection(self):
		if self.cnx:
			if self.cnx.is_connected():
				id_cnx = id(self.cnx)
				self.cnx.close()
			self.cnx = None
			logger.info(f'Connection id={id_cnx} closed')


	def __del__(self):
		self.closeConnection()
	
	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.closeConnection()


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


	def issueCommand(self, tool, shell=False, input=None, **kwargs):
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
		
		cmd_chain = self.buildCommand(tool, **kwargs)
		
		logger.info(f'Executing the following command:\n{cmd_chain}\n')
		self.runShellCommand(cmd_chain, shell=shell, input=input)
	

	def runShellCommand(self, command, shell=False, input=None):
		subprocess.run(command, shell=shell, input=input)


	@abstractmethod
	def create(self, db_name):
		pass

	@abstractmethod
	def delete(self, db_name):
		pass

	@abstractmethod
	def listAllDBs(self):
		pass
	
	@abstractmethod
	def backup(self, db_name):
		pass

	@abstractmethod
	def export(self, db_name, output_file):
		pass
	
	@abstractmethod
	def duplicate(self, db_name, db_new_name):
		pass

	@abstractmethod
	def rename(self, db_name, db_new_name):
		pass

	@abstractmethod
	def _connect(self, **creds):
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


	def _connect(self, **creds):
		keys = ['host', 'port', 'user', 'password']
		creds_subset = {key: creds[key] for key in keys}

		try:
			cnx = mysql_conn.MySQLConnection(**creds_subset)
		except mysql_error as err:
			if err.errno == mysql_errorcode.ER_ACCESS_DENIED_ERROR:
				logger.error("Something is wrong with your user name or password")
			else:
				logger.error(err)
		else:
			id_cnx = id(cnx)
			logger.info(f"Connection id={id_cnx} established to MySQL Server")
			self.cnx = cnx


	def create(self, db_name, overwrite=False) -> None:
		"""Create a MySQL database"""
		if db_name in self.listAllDBs():
			if overwrite:
				logger.warn(f"Database '{db_name}' already exists. It will be deleted and recreated")
				self.delete(db_name)
			else:
				raise ValueError(f"Database '{db_name}' already exists")
		
		statement = f'CREATE DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'
		# self.issueCommand(
		# 	'main',
		# 	statement=statement
		# )
		
		with self.cnx.cursor() as cur:
			cur.execute(statement)
		
		self.cnx.commit()
		logger.info(f"Database '{db_name}' successfully created")


	def delete(self, db_name) -> None:
		"""Delete a MySQL database"""
		if db_name in self.listAllDBs():
			statement = f'DROP DATABASE IF EXISTS {db_name}'
			with self.cnx.cursor() as cur:
				cur.execute(statement)
			
			self.cnx.commit()

			
			logger.info(f'Database {db_name} successfully deleted')
		else:
			logger.warn(f"No operation was performed. The database '{db_name}' does not exist")
			warnings.warn(
				message=f"No operation was performed. The database '{db_name}' does not exist",
				category=RuntimeWarning
			)


	def listAllDBs(self) -> list:
		"""Show all MySQL databases"""
		statement = (
			"SELECT schema_name "
			"FROM information_schema.schemata "
			"WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')"
			"ORDER BY schema_name"
		)

		with self.cnx.cursor() as cur:
			cur.execute(statement)
			databases = []
			for item in cur.fetchall():
				databases.append(item[0])

		self.cnx.commit()
		
		return databases


	def restore(self, input_file, db_name=None) -> str:
		"""Restore a MySQL database from a file
		It is extpected that the input file contains at least a
		CREATE DATABASE statement from which the name of the database can be
		extracted
		Returns the name of the database restored
		"""

		with open(input_file, 'r', encoding='utf-8') as f:
			sql_text = f.read()

		# Check if text contains exactly one instance of a regex pattern
		matches = re.findall("CREATE (?:SCHEMA|DATABASE) [^;\n]+;\n", sql_text)
		if len(matches) > 1:
			raise ValueError('The file is creating multiple databases')
		elif len(matches) < 1:
			raise ValueError('The file does not contain the CREATE DATABASE statement')
		
		
		# Extract dabase name
		matches = re.findall("USE (?P<backtick>`?)([^`;\n]+)(?P=backtick);\n", sql_text)
		
		if len(matches) > 1:
			raise ValueError('The file is using multiple USE statements')
		elif len(matches) < 1:
			raise ValueError('The file does not contain the USE statement')

		# The first slice finds the first full match, the second finds the last captured group
		db_name_extracted = matches[0][-1]
		
		if db_name and db_name != db_name_extracted:
			# Replace db_name in SQL text
			sql_text = sql_text.replace(db_name_extracted, db_name)
			pass
		else:
			db_name = db_name_extracted
		
		self.create(db_name)
		
		def hasPattern(line):
			patterns = [
				r"DROP (?:DATABASE|SCHEMA) IF EXISTS [^;]+;",
				r"CREATE (?:DATABASE|SCHEMA) [^;]+;",
				r"USE [^;]+;"
			]
			for pattern in patterns:
				match = re.search(pattern, line)
				if match:
					return True
			return False

		# Remove the following statements from the SQL text:
		# - DROP DATABASE IF EXISTS
		# - CREATE DATABASE
		# - USE DATABASE
		lines = sql_text.splitlines()
		sql_text = '\n'.join([line for line in lines if not hasPattern(line)])
		
		self.issueCommand(
			'main',
			input=sql_text.encode('utf-8'),
			db_name=db_name
		)

		logger.info(f'Database {db_name} successfully restored')

		return db_name


	def export(self, db_name, output_file=None) -> str:
		"""Export one MySQL database
		The resulting file does not have either:
		- the CREATE DATABASE statement
		- the USE statement
		Returns the absolute path of the output file
		"""
		if output_file is None:
			output_file = db_name
		
		if not output_file.endswith('.sql'):
			output_file += '.sql'

		self.issueCommand(
			'export',
			output=output_file,
			__column_statistics=0,
			databases=None,
			db_name=db_name
		)

		return output_file


	def duplicate(self, db_name, new_db_name) -> None:
		"""Duplicate a MySQL database"""

		# Create a new database with new name
		self.create(new_db_name)

		# Command for dumping the database
		cmd_dump = self.buildCommand(
			'export',
			__column_statistics=0,
			db_name=db_name,
		)

		# Command for importing the database
		cmd_import = self.buildCommand(
			'main',
			database=new_db_name
		)

		# Composing the full command
		cmd = f'{" ".join(cmd_dump)} | {" ".join(cmd_import)}'

		self.runShellCommand(cmd, shell=True)


	def rename(self, db_name, new_db_name) -> None:
		"""Rename a MySQL database
		Behind the scenes, it duplicates the database
		and deletes the old one
		"""
		self.duplicate(db_name, new_db_name)
		self.delete(db_name)


	def backup(self, output_file='backup.sql') -> None:
		"""Backup all databases to a file"""
		self.issueCommand(
			'export',
			output=output_file,
			__column_statistics=0,
			__all_databases=None,
		)
