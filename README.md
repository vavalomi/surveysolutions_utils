# Utilities to work with Survey Solutions API


* Generate SQL database from the (Tabular) file export

call it from the command line:
```shell
python ./tabular_to_sql.py ./example_1_Tabular.zip  "sqlite:///example_1.db" ./quest.json
```

or in your script:
```python
import tabular_to_sql

tabular_to_sql.convert("example_1_Tabular.zip",  "sqlite:///example_1.db", "quest.json")
```