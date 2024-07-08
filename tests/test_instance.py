""" Tests for the instance.py module

Using the terminal from the project's root folder,
run one the following commands:

python -m tests.test_instance
python -m unittest tests/test_instance.py

"""

import unittest

from dbgoat import instance, admin


creds = {
	'host':"192.168.100.101",
	'port':"10002",
	'user':"root",
	'password':"123456"
}

test_dumps = {
	'mysql': {
		'classic': 'tests/mysql/CLASSIC_MODELS.sql'
	}
}


class TestMySQLDBInstance(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		cls.dba = admin.MySQLDBAdmin(creds)


	@classmethod
	def tearDownClass(cls):
		del cls.dba


	def setUp(self) -> None:
		# Create empty database
		self.dba.create('TEST_DB')

		# Restore sample database
		self.dba.restore(test_dumps['mysql']['classic'])
	

	def tearDown(self) -> None:
		dbs = self.dba.listAllDBs()
		for db in dbs:
			self.dba.delete(db)
	

	def test_constructor(self):
		db = instance.MySQLDBInstance(**creds, database='TEST_DB')

		# Test name
		self.assertEqual(db.db_name, 'TEST_DB')

		# Test empty schema
		self.assertIsNone(db.schema)


	def test_listAllColumns(self):
		db = instance.MySQLDBInstance(**creds, database='CLASSIC_MODELS')
		
		expected_names = ['productLine', 'textDescription', 'htmlDescription', 'image']
		expected_types = [
			'varchar(50)',
			'varchar(4000)',
			'mediumtext',
			'mediumblob'
		]

		columns = db.listAllColumns('productlines')
		
		# Check column names
		self.assertListEqual([column.name for column in columns], expected_names)
		# Check column types
		self.assertListEqual([column.type for column in columns], expected_types)
		

	def test_createColumn(self):
		db = instance.MySQLDBInstance(**creds, database='CLASSIC_MODELS')

		# Add column in the last position, without further options
		db.createColumn('products', 'field1', 'VARCHAR(50)')
		columns = db.listAllColumns('products')
		columns_names = [column.name for column in columns]
		self.assertEqual('field1', columns_names[-1])

		# Add column in the last position, with UNIQUE constraint
		db.createColumn('products', 'field2', 'VARCHAR(50)', 'UNIQUE')
		columns = db.listAllColumns('products')
		columns_names = [column.name for column in columns]
		self.assertEqual('field2', columns_names[-1])

		# Add column in the last position, with NOT NULLconstraint
		db.createColumn('products', 'field3', 'VARCHAR(50)', 'NOT NULL')
		columns = db.listAllColumns('products')
		columns_names = [column.name for column in columns]
		self.assertEqual('field3', columns_names[-1])

		# Add column in the first position, without further options
		db.createColumn('products', 'field4', 'VARCHAR(50)', position='first')
		columns = db.listAllColumns('products')
		columns_names = [column.name for column in columns]
		self.assertEqual('field4', columns_names[0])


	def test_listAllTables(self):
		db = instance.MySQLDBInstance(**creds, database='CLASSIC_MODELS')
		
		# Test base tables
		expected_basetables = set(['customers', 'employees', 'offices', 'orderdetails', 'orders', 'payments', 'productlines', 'products'])
		basetables = set(db.listAllTables(type='base'))
		self.assertSetEqual(basetables, expected_basetables)

		# Test views
		expected_views = set(['OrderSummary', 'EmployeeDetails', 'ProductSales'])
		views = set(db.listAllTables(type='view'))
		self.assertSetEqual(views, expected_views)

		# Test all tables
		expected_tables = expected_basetables.union(views)
		tables = set(db.listAllTables())
		self.assertSetEqual(tables, expected_tables)
