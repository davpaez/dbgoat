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


class CursorMock:
	statement = None
	column_names = None
	description = None
	lastrowid = None
	rowcount = None
	with_rows = None

	def __init__(self, data):
		def fun():
			return data
		
		num_attributes = len(data[0])

		self.rowcount = len(data)
		#TODO Fix this: Generalize the ability to create arbitrary number of
		# name columns. Right now, it is limited to the letters 'asdfghjkl'
		self.column_names = [attr_name for row, attr_name in zip(range(num_attributes), 'asdfghjkl')]

		self.fetchall = fun


class TestMySQLDBInstance(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		cls.dba = admin.MySQLDBAdmin(creds)


	@classmethod
	def tearDownClass(cls):
		del cls.dba


	def setUp(self) -> None:
		print('\n ' + '-'*50 + '\n')

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


	def test_read(self):
		db = instance.MySQLDBInstance(**creds, database='CLASSIC_MODELS')

		text = "hello world"
		number = 123
		params_num = [(1,), (2,), (3,)]
		
		query_params = "SELECT %s"
		query_multi = f"SELECT {number}; SELECT '{text}'"
		query_params_multi = f"SELECT %d; SELECT '{text}'"

		# Test params
		result = db.read(query_params, params=(number,))
		self.assertEqual(result.data[0][0], number)

		# Test many=True
		results = db.read(query_params, params=params_num, many=True)
		self.assertEqual(results[0].data[0][0], params_num[0][0])
		self.assertEqual(results[1].data[0][0], params_num[1][0])
		self.assertEqual(results[2].data[0][0], params_num[2][0])

		# Test multi=True
		results = db.read(query_multi, multi=True)
		self.assertEqual(results[0].data[0][0], number)
		self.assertEqual(results[1].data[0][0], text)

		# Test many=True and multi=True
		with self.assertRaises(Exception):
			db.read(query_params_multi, params=params_num, many=True, multi=True)


	def test_write_single(self):
		with instance.MySQLDBInstance(**creds, database='TEST_DB') as db:
			# Create tables
			db.write("CREATE TABLE Classes (id INTEGER PRIMARY KEY, name VARCHAR(20))")
			db.write("""
				CREATE TABLE Students (
					id INTEGER, 
					name VARCHAR(20), 
					class_id INTEGER,
					FOREIGN KEY (class_id) REFERENCES Classes (id)
				)
			""")
			self.assertIn('Classes', db.listAllTables())
			self.assertIn('Students', db.listAllTables())
			self.assertEqual(len(db.listAllTables()), 2)

			# Insert records
			db.write("INSERT INTO Classes VALUES (1, 'Painting'), (2, 'Math')")
			db.write("INSERT INTO Students VALUES (1, 'Bob', 1), (2, 'Maria', 1)")
			result = db.read("SELECT * from Students")
			self.assertEqual(result.data[0], (1, 'Bob', 1))
			self.assertEqual(result.data[1], (2, 'Maria', 1))

			# Delete records
			db.write("DELETE FROM Students")
			result = db.read("SELECT COUNT(id) FROM Students")
			self.assertEqual(result.data[0][0], 0)
			
			# Drop tables
			db.write("DROP TABLE Students")
			self.assertNotIn('Students', db.listAllTables())
			self.assertEqual(len(db.listAllTables()), 1)
	

	def test_write_multi(self):
		query_1 = "CREATE TABLE TestTable (field_1 INTEGER, field_2 VARCHAR(20))"
		query_2 = "INSERT INTO TestTable VALUES (123, 'Hello world')"
		query_multi = f"{query_1} ; {query_2}"

		with instance.MySQLDBInstance(**creds, database='CLASSIC_MODELS') as db:
			db.write(query_multi, multi=True)
			result = db.read("SELECT * FROM TestTable")
			self.assertTupleEqual(result.data[0], (123, 'Hello world'))


	def test_write_many(self):
		#TODO Implement this test
		pass


class TestResult(unittest.TestCase):

	def test_getDataSimplified(self):
		# ene row, one column
		data = [(123,)]
		cursor = CursorMock(data)
		result = instance.Result(cursor)
		self.assertEqual(result.getDataSimplified(), 123)

		# two rows, one column
		data = [(123,), (456,)]
		cursor = CursorMock(data)
		result = instance.Result(cursor)
		self.assertListEqual(result.getDataSimplified(), [123, 456])

		# one row, two columns
		data = [(123, 456)]
		cursor = CursorMock(data)
		result = instance.Result(cursor)
		self.assertTupleEqual(result.getDataSimplified(), (123, 456))
		
		# two rows, two columns
		data = [(123, 456), ('abc', 'def')]
		cursor = CursorMock(data)
		result = instance.Result(cursor)
		self.assertListEqual(result.getDataSimplified(), [(123, 456), ('abc', 'def')])


if __name__ == '__main__':
	unittest.main()
