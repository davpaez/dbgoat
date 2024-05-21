""" Tests for the admin.py module

Using the terminal from the project's root folder,
run one the following commands:

python -m tests.test_admin
python -m unittest tests/test_admin.py

"""

import unittest
import os
import shutil
import warnings

from dbgoat import admin


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


class TestMySQLDBAdmin(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		cls.dba = admin.MySQLDBAdmin(creds)

	@classmethod
	def tearDownClass(cls):
		del cls.dba


	def setUp(self):
		print('\n ' + '-'*50 + '\n')
		# Create temp folder for saving database files
		self.temp_folder = os.path.join(os.getcwd(), '.temp')
		os.makedirs(self.temp_folder, exist_ok=True)


	def tearDown(self):
		# Delete temp folder and all its contents recursively
		shutil.rmtree(self.temp_folder)


	def test_constructor(self):
		dba = admin.MySQLDBAdmin(creds)
		self.assertIsInstance(dba, admin.MySQLDBAdmin)
	

	def test_create(self):
		db_name = 'TEST_DB'
		self.dba.create(db_name)

		# Send SQL command to USE the database
		cur = self.dba.cnx.cursor()
		cur.execute(f"USE {db_name}")
		self.dba.cnx.commit()
		cur.close()

		# Clean up
		self.dba.delete(db_name)
	

	def test_delete(self):
		db_name = 'TEST_DB'

		# Test creating and deleting a database
		self.dba.create(db_name)
		self.dba.delete(db_name)
		self.assertNotIn(db_name, self.dba.listAllDBs())

		# Test trying to delete an inexistent database
		with warnings.catch_warnings(record=True) as w:
			self.dba.delete('INEXISTENT_DB')
			self.assertIs(w[-1].category, RuntimeWarning)


	def test_listAllDBs(self):
		
		# Check if the return value is a list
		list_dbs = self.dba.listAllDBs()
		self.assertIsInstance(list_dbs, list)

		# Delete all databases, to start from a known state
		# and check if the list is empty
		for db_name in list_dbs:
			self.dba.delete(db_name)
		self.assertTrue(len(self.dba.listAllDBs()) == 0)
		
		# Create database names
		db_names = [f'TEST_DB_{i}' for i in range(1, 10)]

		# Create databases and check if they are in the list
		for db_name in db_names:
			self.dba.create(db_name)
		
		list_dbs = self.dba.listAllDBs()

		for db_name in db_names:
			self.assertIn(db_name, list_dbs)
		
		# Clean up
		for db_name in db_names:
			self.dba.delete(db_name)


	def test_restore(self):
		abs_path_temp_file = test_dumps['mysql']['classic']

		# Restore without specifying the database name
		db_name = self.dba.restore(abs_path_temp_file)
		self.dba.delete(db_name)

		# Restore specifying the database name. This replaces the original database name
		db_name = self.dba.restore(abs_path_temp_file, db_name="TEST_DB")
		self.dba.delete(db_name)

		# Trying to restore an inexistent database file
		with self.assertRaises(FileNotFoundError):
			self.dba.restore(os.path.join(self.temp_folder, 'inexistent.sql'))


	def test_restore_export(self):
		abs_path_temp_file = test_dumps['mysql']['classic']
		try:
			# Restore DB
			db_name_1 = self.dba.restore(abs_path_temp_file)
			
			# Export and delete DB
			output_abs_path = os.path.join(self.temp_folder, db_name_1 + '.sql')
			self.dba.export(db_name_1, output_file=output_abs_path)  # Without filename
			self.dba.delete(db_name_1)

			self.assertNotIn(db_name_1, self.dba.listAllDBs())

			# Restore DB from the exported file
			self.dba.restore(output_abs_path)

			# Check if the restored DB exists
			self.assertIn(db_name_1, self.dba.listAllDBs())
		finally:
			self.dba.delete(db_name_1)


	def test_export_with_extension(self):
		db_name = 'TEST_DB'
		self.dba.create(db_name)

		filename = 'testdb.sql'  # With extension
		abs_path = os.path.join(self.temp_folder, filename)
		self.dba.export(db_name,output_file=abs_path)
		self.dba.delete(db_name)

		self.assertTrue(os.path.exists(abs_path))


	def test_export_without_extension(self):
		db_name = 'TEST_DB'
		self.dba.create(db_name)

		filename = 'testdb'  # Without extension
		abs_path = os.path.join(self.temp_folder, filename)
		self.dba.export(db_name, output_file=abs_path)
		self.dba.delete(db_name)

		self.assertTrue(
			os.path.exists(abs_path + '.sql')
		)


	def test_export_without_filename(self):
		db_name = 'TEST_DB'
		self.dba.create(db_name)

		abs_path = os.path.join(self.temp_folder, db_name)
		root_folder = os.getcwd()  # Save root directory

		try:
			os.chdir(self.temp_folder)  # Change directory to temp folder
			self.dba.export(db_name)  # Without filename
			self.dba.delete(db_name)
			self.assertTrue(
				os.path.exists(abs_path + '.sql')
			)
		finally:
			os.chdir(root_folder)  # Change directory back to root folder


	def test_duplicate(self):
		abs_path_temp_file = test_dumps['mysql']['classic']

		db_name = 'TEST_DB_1'
		self.dba.restore(abs_path_temp_file, db_name=db_name)
		
		new_db_name = 'TEST_DB_2'
		self.dba.duplicate(db_name, new_db_name)

		list_dbs = self.dba.listAllDBs()
		self.assertIn(db_name, list_dbs)
		self.assertIn(new_db_name, list_dbs)

		self.dba.delete(db_name)
		self.dba.delete(new_db_name)


if __name__ == '__main__':
	unittest.main()
