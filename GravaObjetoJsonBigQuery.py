# -*- coding: iso-8859-1 -*-

# Exemplo de Gravacao de Dados Json no BigQuery

from google.cloud import bigquery
import pandas as pd
import os, json

# Cria dados ficticios para carga
df = pd.DataFrame([['valor_campo1', 'valor_campo2', 'valor_campo3', 'valor_campo4', 'valor_campo5']],columns=['nome_campo1', 'nome_campo2', 'nome_campo3','nome_campo4','nome_campo5'])

# Converte o data frame para objeto json
json_data = df.to_json(orient = 'records')
json_object = json.loads(json_data)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".meu_arquivo_com_as_credenciais.json"

# Define schema de tabela do BigQuery
table_schema = {
          'name': 'nome_campo1',
          'type': 'STRING',
          'mode': 'REQUIRED'
          },{
          'name': 'nome_campo2',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'nome_campo3',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'nome_campo4',
          'type': 'DATETIME',
          'mode': 'NULLABLE'
          },{
          'name': 'nome_campo5',
          'type': 'STRING',
          'mode': 'NULLABLE'
          }
          
project_id = 'id_projeto'
dataset_id = 'nome_dataset'
table_id = 'nome_tabela'

client  = bigquery.Client(project = project_id)
dataset  = client.dataset(dataset_id)
table = dataset.table(table_id)

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.schema = table_schema
job = client.load_table_from_json(json_object, table, job_config = job_config)

print(job.result())