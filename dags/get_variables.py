from airflow.models import Variable
my_var = Variable.get("my_json_var")
my_regular_var = Variable.get("my_regular_var", deserialize_json=True)
