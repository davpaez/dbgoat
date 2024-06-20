import os
from abc import ABC, abstractmethod
import warnings
import re
import logging
from typing import Optional

from mysql.connector import (
	connection as mysql_conn,
	Error as mysql_error,
	errorcode as mysql_errorcode
)

from . import auxiliar as aux


logger = logging.getLogger('dbgoat')


class DBAdmin(ABC):
	"""This class is used to create, delete, duplicate and
	rename databases"""
	def __init__(self, dbms, creds):
		# This argument determines the DBMS being used (mysql, sqlite, etc)
		self.dbms = dbms

		required_keys = ['host', 'port', 'user', 'password']
		if required_keys != creds.keys():
			if not (set(required_keys) <= set(creds.keys())):
				raise ValueError("Invalid parameters to construct DBAdmin instance")
			else:
				logger.warn("The 'database' argument was ignored for constructing the DBAdmin instance")
		
		sanitized_creds = {key: creds[key] for key in required_keys}
		self.options_values = sanitized_creds.copy()
		
		self.tools = dict()
		self.options_flags = {
			'host': None,
			'port': None,
			'user': None,
			'password': None,
			'database': None,
			'statement': None
		}

		self.cnx = None
		self._connect(sanitized_creds)


	def closeConnection(self):
		if self.cnx:
			if self.cnx.is_connected():
				id_cnx = id(self.cnx)
				self.cnx.close()
			self.cnx = None
			logger.info(f'Connection id={id_cnx} closed')


	def __del__(self):
		# self.closeConnection() # Produces error!
		pass
	

	def __enter__(self):
		return self
	

	def __exit__(self, exc_type, exc_value, traceback):
		self.closeConnection()
	
	
	def buildCommand(self, tool: str, **kwargs):
		# Build command
		executable = self.tools[tool]

		cmd_chain = aux.buildCommand(
			executable, 
			self.options_values,
			self.options_flags,
			**kwargs
		)
		
		return cmd_chain


	def issueCommand(self, tool: str, shell=False, input=None, encoding='utf-8', **kwargs):
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

		env = os.environ.copy()
		passwd = self.options_values.pop('password')
		env['MYSQL_PWD'] = passwd
		
		cmd_chain = self.buildCommand(tool, **kwargs)
		
		logger.info(f'Executing the following command:\n{cmd_chain}\n')
		cp = aux.runShellCommand(cmd_chain, shell=shell, input=input, encoding=encoding, env=env)
		
		self.options_values['password'] = passwd
		return cp


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
	def restore(self):
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
	def _connect(self, creds):
		pass



class MySQLDBAdmin(DBAdmin):
	def __init__(self, *args, **kwargs):
		super().__init__('mysql', *args, **kwargs)

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


	def _connect(self, creds):
		try:
			cnx = mysql_conn.MySQLConnection(**creds)
		except mysql_error as err:
			if err.errno == mysql_errorcode.ER_ACCESS_DENIED_ERROR:
				logger.error("Something is wrong with your user name or password")
			else:
				logger.error(err)
		else:
			id_cnx = id(cnx)
			logger.info(f"Connection id={id_cnx} established to MySQL Server")
			self.cnx = cnx


	def create(self, db_name: str, overwrite=False) -> None:
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


	def delete(self, db_name: str) -> None:
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
	

	@staticmethod
	def transformDump(db_name, sql_text):
		# Check if text contains exactly one instance of a regex pattern
		matches = re.findall("CREATE (?:SCHEMA|DATABASE) [^;\n]+;\n", sql_text)
		if len(matches) > 1:
			raise ValueError('The file is creating multiple databases')
		elif len(matches) < 1:
			raise ValueError('The file does not contain the CREATE DATABASE statement')
		
		
		# Extract dabase name
		matches = re.findall("USE (?P<backtick>`?)([^`;\n]+)(?P=backtick);\n", sql_text)
		
		# if len(matches) > 1:
		# 	raise ValueError('The file is using multiple USE statements')
		if len(matches) < 1:
			raise ValueError('The file does not contain the USE statement')

		# The first slice finds the first full match, the second finds the last captured group
		db_name_extracted = matches[0][-1]
		
		if db_name and db_name != db_name_extracted:
			# Replace db_name in SQL text
			sql_text = sql_text.replace(db_name_extracted, db_name)
			pass
		else:
			db_name = db_name_extracted
		
		def hasPattern(line: str):
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

		return db_name, sql_text


	def restore(self, input_file: str, db_name: Optional[str] = None) -> str:
		"""Restore a MySQL database from a file
		It is extpected that the input file contains at least a
		CREATE DATABASE statement from which the name of the database can be
		extracted
		Returns the name of the database restored
		"""

		with open(input_file, 'r', encoding='utf-8') as f:
			sql_text = f.read()

		db_name, sql_text = self.transformDump(db_name, sql_text)

		self.create(db_name)

		self.issueCommand(
			'main',
			input=sql_text,
			database=db_name
		)

		logger.info(f'Database {db_name} successfully restored')

		return db_name


	def export(self, db_name: str, output_file: Optional[str] = None) -> str:
		"""Export one MySQL database
		The resulting file will have:
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


	def duplicate(self, db_name: str, new_db_name: str, overwrite=False) -> None:
		"""Duplicate a MySQL database"""

		# Create a new database with new name
		if overwrite is True:
			self.delete(new_db_name)
		self.create(new_db_name)

		# Remove password option temporarily
		env = os.environ.copy()
		passwd = self.options_values.pop('password')
		env['MYSQL_PWD'] = passwd

		# Command for dumping the database
		# without CREATE or USE statements
		cmd_dump = self.buildCommand(
			'export',
			__column_statistics=0,
			databases=None,
			db_name=db_name
		)

		# Command for importing the database
		cmd_import = self.buildCommand(
			'main',
			database=new_db_name
		)

		# Dump and then import
		completed_process = aux.runShellCommand(cmd_dump, encoding='utf-8', env=env)
		sql_text = completed_process.stdout
		_, sql_text = self.transformDump(new_db_name, sql_text)
		aux.runShellCommand(cmd_import, input=sql_text, encoding='utf-8', env=env)

		# Restore password option
		self.options_values['password'] = passwd
	

	def rename(self, db_name: str, new_db_name: str) -> None:
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


class SQLiteDBAdmin(DBAdmin):
	pass
