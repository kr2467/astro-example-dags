from airflow.models import Variable
my_var=variable.get("my_json_var")
my_regular_var=variable.get("my_regular_var",deserialize_json=True)
