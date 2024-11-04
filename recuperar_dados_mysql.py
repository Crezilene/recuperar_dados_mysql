import os
import mysql.connector
import pandas as pd

host = os.getenv("MYSQL_HOST", "host")
user = os.getenv("MYSQL_USER", "user")
password = os.getenv("MYSQL_PASSWORD", "senha")
database = os.getenv("MYSQL_DATABASE", "databese")

conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
cursor = conn.cursor()

query = """
SELECT
    dc.NOME_COLABORADOR,
    dc.PERFIL_COLABORADOR,
    dc.PERFIL_ACESSO,
    dc.COLABORADOR_SUPERIOR AS SUPERVISOR,
    dc.UF AS ESTADO,
    dc.CIDADE,
    dp.NOME_PDG,
    CASE fr.TIPO
        WHEN 0 THEN 'PERIODICO'
        WHEN 1 THEN 'AGENDADO'
    END AS TIPO_ROTEIRO,
    CASE fr.DIA_SEMANA
        WHEN 1 THEN 'DOMINGO'
        WHEN 2 THEN 'SEGUNDA-FEIRA'
        WHEN 3 THEN 'TERCA-FEIRA'
        WHEN 4 THEN 'QUARTA-FEIRA'
        WHEN 5 THEN 'QUINTA-FEIRA'
        WHEN 6 THEN 'SEXTA-FEIRA'
        WHEN 7 THEN 'SABADO'
    END AS DIA_SEMANA,
    IF(fr.TIPO = 0, fr.SEMANA, '') AS SEMANA,
    IF(fr.TIPO = 1, DATE_FORMAT(fr.DATA, '%Y-%m-%d'), '') AS DATA,
    CASE fr.EXECUCAO_UNICA
        WHEN 0 THEN 'NÃO'
        WHEN 1 THEN 'SIM'
        ELSE ''
    END AS EXECUCAO_UNICA,
    DATE_FORMAT(ADDDATE(fr.DT_LIMITE, INTERVAL 1 DAY), '%Y-%m-%d') AS DATA_LIMITE,
    fr.ORDEM,
    TIME_FORMAT(ADDTIME(fr.HORA_ENTRADA, '03:00'), '%H:%i') AS HORA_ENTRADA,
    TIME_FORMAT(ADDTIME(fr.HORA_SAIDA, '03:00'), '%H:%i') AS HORA_SAIDA,
    TIMEDIFF(fr.HORA_SAIDA, fr.HORA_ENTRADA) AS DURACAO,
    TIME(FROM_UNIXTIME(fr.TEMPO_PLANEJADO_PDV / 1000)) AS TEMPO_PLANEJADO_PDV,
    fr.DESCRICAO
FROM FT_ROTEIRO fr
LEFT JOIN DIM_COLABORADOR dc
    ON fr.ID_DIM_COLABORADOR = dc.ID_DIM_COLABORADOR
LEFT JOIN DIM_PDV dp
    ON fr.ID_DIM_PDV = dp.ID_DIM_PDV
WHERE (DATA IS NULL OR DATA >= DATE_FORMAT(NOW(), '%Y-%m-%d'))
ORDER BY dc.NOME_COLABORADOR,
    fr.SEMANA,
    fr.DIA_SEMANA,
    fr.ORDEM
"""
cursor.execute(query)

dados = cursor.fetchall()
colunas = [desc[0] for desc in cursor.description]

df = pd.DataFrame(dados, columns=colunas)

df['HORA_ENTRADA'] = pd.to_datetime(df['HORA_ENTRADA'], format='%H:%M')
df['HORA_SAIDA'] = pd.to_datetime(df['HORA_SAIDA'], format='%H:%M')
df['DURACAO'] = (df['HORA_SAIDA'] - df['HORA_ENTRADA']
                 ).dt.total_seconds() / 3600

caminho_csv = r"C:\Users\iebt 0365\Documents\Connecta\roteiros\dados_roteiro.csv"
caminho_excel = r"C:\Users\iebt 0365\Documents\Connecta\roteiros\dados_roteiro.xlsx"

df.to_csv(caminho_csv, index=False)

df.to_excel(caminho_excel, index=False, sheet_name="Dados Roteiro")

cursor.close()
conn.close()

print(f"Dados exportados para '{caminho_csv}' e '{
      caminho_excel}' com sucesso!")
