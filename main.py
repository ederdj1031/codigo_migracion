import requests
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://admin:adminintegra@integra-db.cixttkwrfvm3.us-east-1.rds.amazonaws.com/users')
conexion = mysql.connector.connect(
    host='integra-db.cixttkwrfvm3.us-east-1.rds.amazonaws.com',
    user='admin',
    password='adminintegra',
    database='users'
)
cursor = conexion.cursor()

res = requests.get('https://62433a7fd126926d0c5d296b.mockapi.io/api/v1/usuarios')
df = pd.DataFrame( data=res.json() )

table_name = "users_info"

tabla_integradb = {}

for llave, tipo in df.dtypes.to_dict().items():
    if tipo == "object" and llave != 'id':
        tabla_integradb[llave] = "VARCHAR(100)"
        continue
    
    elif tipo == "int64" or llave == 'id':
        tabla_integradb[llave] = "INT"


sentencia = f"CREATE TABLE {table_name} ("
for columna, tipo in tabla_integradb.items():
    sentencia += f"{columna} {tipo}, "
sentencia = sentencia.rstrip(', ') + ")"


cursor.execute(sentencia) 

df.to_sql(name=table_name, con=engine, if_exists="replace", index=False) 

conexion.close()