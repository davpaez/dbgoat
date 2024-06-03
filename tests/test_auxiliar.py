

import unittest


from dbgoat import auxiliar as aux


class TestFunctions(unittest.TestCase):

	def test_runShellCommand(self):
		# Test capture of stdout
		word = 'hola'
		cp = aux.runShellCommand(f"echo {word}")
		self.assertEqual(cp.stdout.decode().strip(), word)

		word = '123'
		cp = aux.runShellCommand(f"echo {word}")
		self.assertEqual(cp.stdout.decode().strip(), word)

		# Test drop database if exists
		sql_command = 'DROP SCHEMA IF EXISTS sakila; CREATE DATABASE sakila;'
		cp = aux.runShellCommand(
			'mysql -h 192.168.100.101 -P 10001 -u root -p123456',
			input=sql_command,
			encoding='utf-8'
		)

		sql_command = (
			"SELECT schema_name "
			"FROM information_schema.schemata "
			"WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')"
			"ORDER BY schema_name"
		)
		cp = aux.runShellCommand(
			'mysql -h 192.168.100.101 -P 10001 -u root -p123456',
			input=sql_command,
			encoding='utf-8'
		)
		self.assertTrue('sakila' in cp.stdout)


	def test_buildCommand(self):
		pass
		# TODO
