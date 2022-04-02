# hw6_db_explorer

db_explorer program

This simple web service will be a MySQL database manager that allows you to make CRUD requests (create, read, update, delete) to it via HTTP

In this assignment, we continue to practice HTTP skills and interact with the database.

*In this task, you cannot use global variables, store what you need in the fields of the structure that lives in the closure*

For the user it looks like this:
* GET / - returns a list of all tables (which we can use in further queries)
* GET /$table?limit=5&offset=7 - returns a list of 5 records (limit) starting from the 7th (offset) from table $table. limit by default 5, offset 0
* GET /$table/$id - returns information about the entry itself or 404
* PUT /$table - creates a new entry given the entry in the request body (POST parameters)
* POST /$table/$id - updates the record, the data comes in the request body (POST parameters)
* DELETE /$table/$id - deletes an entry
* GET, PUT, POST, DELETE is the http method by which the request was sent

Features of the program:
* Request routing - by hand, no external libraries can be used.
* Full dynamics. when initializing in NewDbExplorer, we read a list of tables and fields from the database (queries below), then we work with them during validation. No hadcode in the form of a bunch of conditions and written code for validation-filling. If you add a third table, everything should work for it.
* We assume that while the program is running, the list of tables does not change
* Requests will have to be constructed dynamically, data from there will also be retrieved dynamically - you do not have a fixed list of parameters - you load it during initialization.
* Validation at the level of "string - int - float - null", without problems. Remember that json in an empty interface is unpacked as a float if no specials are specified. options.
* All work happens through database/sql, you get a working connection to the database as input. No orms or anything.
* All field names as they are in the database.
* In case an error occurs - just return 500 in the http status
* Don't Forget About SQL Injections
Ignore unknown fields
* The use of global variables is prohibited in this task. Everything you want to store - store in the fields of the structure that lives in the closure
