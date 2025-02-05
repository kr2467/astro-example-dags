from airflow.models import Variable

def set_var():
    Variable.set(key="my_regular_var", value="Hello!")
    Variable.set(key="my_json_var", value={"num1": 32, "num2": 23}, serialize_json=True)
