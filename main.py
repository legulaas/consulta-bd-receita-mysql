import os
import pandas as pd
from sqlalchemy import create_engine
import time

# Input de intervalo de linhas e total de linhas
intervalo = int(input("Informe o intervalo de linhas de cada consulta: "))
max_linhas = int(input("Informe o máximo de linhas no arquivo .csv: "))

# Configuração conexão servidor MySQL
host = '127.0.0.1'
user = 'USER_NAME'
password = 'PASSWORD'
database = 'DATABASE_NAME'

# Conexão com o banco de dados
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

# Variáveis de controle
num_rows = 0
limit_min = 0
limit_max = intervalo

# Define o tempo de início do script
start_time = time.time()
print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}]: Iniciando script...')

# Define o nome do arquivo .csv
arquivo = f'./output/result-{time.strftime("%Y%m%d_%H-%M-%S")}.csv'

while limit_min <= max_linhas:

	# Consulta MySQL
	qry = f'''
	SELECT 
		CONCAT(
		INSERT(
			INSERT(
				LPAD(CONVERT(e.cnpj_basico,char), 8, '0'), 6, 0, '.'
		), 3, 0, '.'
		), 
		'/', 
		LPAD(CONVERT(e.cnpj_ordem,char), 4, '0'), 
		'-', 
		LPAD(CONVERT(e.cnpj_dv,char), 2, '0')) AS cnpj,
		e2.razao_social,
		so.nome_socio,
		e.correio_eletronico as email,
		CONCAT('(', e.ddd1, ')',e.telefone1) as telefone,
		e.cnae_fiscal as cnae
	FROM estabelecimento e 
	INNER JOIN empresas e2 ON e.cnpj_basico = e2.cnpj_basico 
	INNER JOIN socios_original so ON e.cnpj_basico = so.cnpj_basico 
	WHERE e.situacao_cadastral = '02' AND e.cnae_fiscal IN ('6920603', '7810800', '8599604', '7020400') AND e.telefone1 <> '' AND e.correio_eletronico <> ''
	LIMIT {limit_min}, {limit_max}
	'''
	### exit(qry)

	# Armazenando tempo de início da consulta
	start_sql = time.time()

	# Execução da query
	df = (pd.read_sql_query(qry, engine))
	
	# Armazenando tempo de fim da consulta
	end_sql = time.time()

	# Retorna o número de linhas da consulta
	num_rows = df.shape[0]

	# Verifica e cria o arquivo .csv
	if(limit_min == 0):
		df.to_csv(arquivo, index=False, sep=';')
		print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}]: Arquivo .csv criado. Intervalo {limit_min} - {limit_max+limit_min}. Tempo de execução: {round(end_sql - start_sql, 2)}s')
		print(df)
	else:
		df.to_csv(arquivo, mode='a', header=False, index=False, sep=';')
		print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}]: Arquivo .csv atualizado. Intervalo {limit_min} - {limit_max+limit_min}. Tempo de execução: {round(end_sql - start_sql, 2)}s')
		print(df)

	# Incrementa o intervalo
	limit_min += limit_max

	# Define o limite máximo de linhas no CSV
	if(limit_min == max_linhas):
		break
	elif (limit_min > max_linhas):
		limit_max  = limit_min - max_linhas 
		limit_min -= intervalo
	else:
		if (max_linhas - limit_min) < intervalo:
			limit_max = max_linhas - limit_min
		else :
			continue

# Armazenando tempo de fim do script
end_time = time.time()

print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}]: Script finalizado!')
print(f'Tempo de execução: {round(end_time - start_time,2)}s')