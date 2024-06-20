# High level tasks

[ ] Finish implementing the MySQLDBInstance.read method in the case where the 'many' param is True

[ ] Fix (remove, implement or somehow solve) all parts of the DBInstance and child classes that depend on a definition of a schema (a dictionary passed during object construction). That includes:
	- Constructor methods
	- clear method
	- initialize method

[ ] Add support for multiple databases
	[ ] SQLite
	[ ] PostgreSQL

[ ] Create long-lived, self-healing connections
