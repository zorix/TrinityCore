* Apply all patches found in this directory
* Run the modified ddl2cpp.py on database structure dumps
* Hotfix database dump should only contain hotfix_data table, all other statements are generated automatically and don't need typesafety this library provides
* ddl2cpp.py was modified to escape column/table names that are c++ keywords and to make use of unsigned fields
