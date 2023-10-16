# Databricks notebook source
#dbutils.notebook.help()
#dbutils.notebook.exit('O pipeline só poderá ser executado após as 12:00')
#print("passou do exit?")

# COMMAND ----------

#instalando biblioteca necessária para processamento dos datasets
!pip install adal

# COMMAND ----------

import base64
import requests
import adal
import json
import time
import pandas as pd

# COMMAND ----------

#CONEXÃO COM O BANCO DE DADOS
StartProcess = time.time()

host_name = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001HostName")
port = 1433
database = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DatabaseName")
user = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001UserName")
password = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DBPass")

url = f'jdbc:sqlserver://{host_name}:{port};databaseName={database};user={user};password={password}' 

# COMMAND ----------

#Busca no banco de dados a data e hora referente ao período de freezing

query = \
"""
select top 1 IterationSprintStartDate,convert(varchar,dateadd(hour,-3,getdate()),112) as dataAtual,
  datepart(HOUR,dateadd(hour,-3,getdate())) as horaAtual
from DimIterationSprint dim
where IterationSprintStartDate >=
(select convert(varchar,getdate(),112))
order by IterationSprintStartDate asc
"""


Params = spark.read\
.format('jdbc')\
.option('url', url)\
.option('query', query)\
.load().toPandas()

# COMMAND ----------

#valiando informações do período de freezing
Params

# COMMAND ----------

#Variáveis que irão receber o dataset e workspace id para processamento no ADF

dbutils.widgets.text("datesetId","")
datesetId = dbutils.widgets.get("datesetId")
dbutils.widgets.text("workspaceId","")
workspaceId = dbutils.widgets.get("workspaceId")

# COMMAND ----------

#Condição que identifica se estamos no perído de freezing, se estiver, o pipeline irá sair informando que o processamento ocorrerá apenas após as 12:00 remove o id do SIT do processo
if Params.dataAtual[0]==Params.IterationSprintStartDate[0] and Params.horaAtual[0] < 12 and datesetId != 'ab432cba-6087-4ec9-a30c-80dbf361a22e':
  v_accept = 0
  print("Não iremos executar!")
  dbutils.notebook.exit('O pipeline só poderá ser executado após as 12:00')
else:
  v_accept = 1
  print("Liberado para processamento!")


# COMMAND ----------

#Abrindo conexão com o powerbi para processamento do dos datasets
tenant_id = 'coleaqui o tenant_id do powerbi'
authority_url = f'https://login.microsoftonline.com/{tenant_id}'
resource_url = 'https://analysis.windows.net/powerbi/api'
client_id = 'cole aqui o client_id'
client_secret = 'cole aqui o client_secret'
context = adal.AuthenticationContext(authority=authority_url,
                                     validate_authority=True,
                                     api_version=None)
token = context.acquire_token_with_client_credentials(resource_url, client_id, client_secret)

access_token = token.get('accessToken')

refresh_url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/datasets/{datesetId}/refreshes'

header = {'Authorization': f'Bearer {access_token}'}
r = requests.post(url=refresh_url, headers=header)
r.raise_for_status()
jujuba = r.raise_for_status()
print(jujuba)

# COMMAND ----------

#Acompanha as execuções para verificar se o processo foi finalizado com sucesso, se a api retornar saída de erro, vai sair com erro no pipeline
v_value = ""
while v_value != "Completed":
  if v_value != "Failed":
    time.sleep(10)
    get_url = refresh_url+"?$top=1"
    v_get = requests.get(url=get_url, headers=header)
    jsRefresh = json.loads(v_get.content)
    df = pd.DataFrame(jsRefresh['value'])
    v_value = df.status.values
    print(f"Execução {v_value}")
  else:
    raise ValueError(f"Erro ao Executar o dataset {v_value}!")
print(f"Sucesso na execução do dataset {v_value}!!!!")  


