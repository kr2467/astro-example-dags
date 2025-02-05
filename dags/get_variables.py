from airflow.models import variable
my_var=variable.get("my_json_var")
my_josn_var=variable.get("my_regular_var",deserialize_json=True)
