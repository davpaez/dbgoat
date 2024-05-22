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
	'password':"123456",
	'database': 'TEST_DB'
}

test_dumps = {
	'mysql': {
		'classic': 'tests/mysql/CLASSIC_MODELS.sql'
	}
}


class TestMySQLDBInstance(unittest.TestCase):

	def setUp(self) -> None:
		self.dba = admin.MySQLDBAdmin(creds)
		self.dba.create(creds['database'])
	

	def tearDown(self) -> None:
		self.dba.delete(creds['database'])
	

	def test_constructor(self):
		db = instance.MySQLDBInstance(**creds)

		# Test name
		self.assertEqual(db.db_name, creds['database'])

		# Test empty schema
		self.assertIsNone(db.schema)
