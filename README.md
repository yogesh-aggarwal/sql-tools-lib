---
description: Python package to help out in working with SQL database-related operation.
---

# SQL-TOOLS

 It is a ython package which uses database \(SQL\) functionality to help the developer to perform SQL operations on their desired database. This project _aims_ the developer ring to focus on their main code rather than focusing on the database related headaches.

 This python package uses the `sqlite` functionality to store databases on the local and `My SQL` for directly connect with the database setup on the host also some built-in features for working with `MongoDB` related operations.

## sqlite

Sometimes the sqlite databases are very useful when we have to store the data in some separate files for separate works. There are many features in this library which will help you out in easy & fast operation of SQL queries for this kind of work.

`sqlite` is a sub-module of `sql-tools` package which includes many types of functionality which include the fast execution of command & also some tools for `on-the-fly` result generation to get rid of writing SQL queries for small works. This library is divided into some separate files so that you can only import what you need and get performance as well as speed in your program\(s\).

### **Importing the library**

`sqlite` is a submodule of `sql-tools` which can be imported in your program as follows:

```python
>>> from sql_tools import sqlite
```

In this way, all the functionalities of the library get imported into your program.

### **Connecting the database**

Before the execution of any command, the database\(s\) should be connected. There is a function in connect file called `connect` which will connect the database\(s\).

```python
>>> sqlite.connect("users.db")
```

Not only one, but you can also connect as many databases as you want by providing a collection of it.

```python
>>> sqlite.connect(["users.db", "staff.db"])
```

> Keep in mind that you should execute the same no. of command as the database\(s\) connected.

### **Execution of command**

The library includes a function called `execute` which resides in the execute file, helps you to execute the desired command.

```python
>>> sqlite.execute("SELECT * FROM WORLD WHERE SOMEONE LIKE %YOU%")
```

The above code will execute the command on the connected database and return the result as a numpy array. For eg:

```text
[
 [Result for first database]
 [Result for second database]
 .  .  .  .  .  .  .  .  .  .
 .  .  .  .  .  .  .  .  .  .
]
```

You can also run multiple commands for multiple databases as well:

> \#Example 1

```python
# Connect the databases
sqlite.connect(["users.db", "staff.db"])

# Executing the command
sqlite.execute(
  ["SELECT first_name FROM user WHERE username LIKE '%uname%'", "SELECT first_name FROM staff WHERE id<2"]
)
```

Will return something like:

```text
[[["user 1"]
  ["user 2"]]
  
 [["member 1"]
  ["member 2"]]]
```

### **Analyzing the result**

```text
# Result for first database (At 0 index of execution result)
[["user 1"]
 ["user 2"]]

# Result for the second database (At 1 index of execution result)
[["member 1"]
 ["member 2"]]
```

The execute function converts the result to a numpy array for faster data analysis with the result. Result as an array which positions the result of execution\(s\) as its elements.

### **Parameters**

* **`command`**: The command to be executed. It can be a string for a single command & an array for multiple commands.
* **`databPath`**: When we want to operate a specific database apart from the connected ones, then we provide it the name of the database\(s\). Accepts string for single database & an array for multiple databases. By default, it sets to the connected databases.
* **`matrix`**: Whether to convert the result to a numpy array. Accepts a boolean value. Default is `True`
* **`inlineData`**: Whether to inline the data or not. We have noticed in `#Example 1` that for the same database, it is returning separate arrays for every result. To prevent this, we provide `inlineData=True` to inline the data. Accepts a boolean value. Default is `False`.
* **`splitByColumns`**: Whether to split the data column-wise or not. We have noticed that the result is row-wise. Accepts boolean value. Default is `False`.
* **`pathJSON`**: Accepts the path of the JSON file which contains the information about the execution of the command\(s\). Accepts only one path at a time.
* **`asyncExec`**: Whether to perform the execution asynchronously or not. This will help us to avoid the block time of the program & run the SQL queries in the background. Accepts boolean value. Default is `False`.
* **`splitExec`**: Whether to separately execute every command by reconnecting the database every time. Useful when we have to commit the changes per execution. Accepts boolean value. Default is `False`.
* **`returnDict`**: Whether to return a dictionary of result with keys as database name and value as the result. Accepts boolean value. Default is `False`.
* **`logConsole`**: Whether to log the execution steps to the console with process id. Accepts boolean value. Default is `False`.
* **`raiseError`**: Whether to raise error if something goes wrong during execution. Accepts boolean value. Default is `True`.
* **`commit`**: Whether to commit the changes after the execution of a command. Accepts boolean value. Default is `True`.

**Note**: If we want to manually commit the changes then there is a function in the library called `commit` which will commit the changes to the provided database.

```python
sqlite.commit("users.db")
```

### **Disconnecting the database**

To free up the memory & run programs faster, it's a good practice to disconnect the database that is not in use.

```python
sqlite.disconnect("users.db")
```

Disconnecting multiple databases

```python
sqlite.disconnect(["users.db", "staff.db"])
```

### Resources

Read the full [documentation](https://yogesh-aggarwal.github.io/sql-tools-lib) for more information

You can contribute to it through [GitHub](https://github.com/yogesh-aggarwal/sql-tools-lib)

### View project at [GitHub](https://github.com/users/yogesh-aggarwal/projects/2)

