import io
import sqlite3
import pandas as pd
import streamlit as st

def create_or_connect_database():
  conn = sqlite3.connect('/tmp/consult.db')
  cursor = conn.cursor()

  f = io.open('./tabelas.sql', 'r', encoding='utf-8')
  sql = f.read()
  try:
    cursor.executescript(sql)
  except sqlite3.Error as e:
    print("SQLite error:", e)

  tables = ['OCORRENCIA', 'ACIDENTE', 'AERODROMO', 'AERONAVE', 'DESCRICAO', 'LOCAL']
  for table in tables:
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
    if not cursor.fetchone():
      st.write(f"A tabela {table} não existe no banco de dados")
      return None

  return conn

def main():
  st.title("Ocorrências Aeronáuticas")

  block_op = st.selectbox('Selecione a consulta: ', ['ano_morte_tripulantes', 'regiao_ocorrencia_acidentes', 'fabricantes_falha_componentes',
                                                     'categ_aeronaves_mais_trip_ilesos', 'horarios_maior_ocorrencia', 'aerodromo_acidentes_mais_graves',
                                                     'frequencias_ocorrencias', 'rotas_casos_fatais_excursao'])

  conn = create_or_connect_database()

  if conn is None:
    st.write("Verifique o banco de dados")
    return

  if block_op == 'ano_morte_tripulantes':
    query = """
      SELECT SUBSTR(Data_da_Ocorrencia, 7) AS Ano,
            SUM(CASE WHEN Lesoes_Fatais_Tripulantes IS NOT NULL THEN 1 ELSE 0 END) AS Total_Mortes_Tripulantes
      FROM OCORRENCIA NATURAL JOIN ACIDENTE
      GROUP BY Ano
      ORDER BY Total_Mortes_Tripulantes DESC
      LIMIT 5;
    """
    df = pd.read_sql_query(query, conn)
    df

  if block_op == 'regiao_ocorrencia_acidentes':
    query = """
      SELECT Regiao AS Regiao,
            COUNT(ID) AS Total_Ocorrencias
      FROM OCORRENCIA NATURAL JOIN LOCAL
      GROUP BY Regiao
      ORDER BY Total_Ocorrencias DESC
      LIMIT 5;
    """
    df = pd.read_sql_query(query, conn)
    df

  if block_op == 'fabricantes_falha_componentes':
    query = """
      SELECT Nome_do_Fabricante AS Fabricante,
          COUNT(ID) AS Total_Ocorrencias
      FROM OCORRENCIA NATURAL JOIN AERONAVE NATURAL JOIN DESCRICAO
      WHERE Tipo_de_Ocorrencia IN ('SCF-NP', 'SCF-PP')
      GROUP BY Fabricante
      ORDER BY Total_Ocorrencias DESC
      LIMIT 5;
    """
    df = pd.read_sql_query(query, conn)
    df

  if block_op == 'categ_aeronaves_mais_trip_ilesos':
    query = """
    SELECT Categoria_da_Aeronave AS Categoria_Aeronave,
       COUNT(ID) AS Total_Tripulantes_Ilesos
    FROM AERONAVE
    WHERE "Ilesos_Tripulantes" IS NOT NULL
    GROUP BY Categoria_da_Aeronave
    ORDER BY Total_Tripulantes_Ilesos DESC
    LIMIT 5;
    """
    df = pd.read_sql_query(query, conn)
    df

  if block_op == 'horarios_maior_ocorrencia':
    query = """
    SELECT
      Intervalo_Horario,
      COUNT(ID) AS Total_Ocorrencias
    FROM (
      SELECT "ID",
            CASE
              WHEN strftime('%H:%M', "Hora_da_Ocorrencia") BETWEEN '00:00' AND '05:59' THEN '00:00 - 05:59'
              WHEN strftime('%H:%M', "Hora_da_Ocorrencia") BETWEEN '06:00' AND '11:59' THEN '06:00 - 11:59'
              WHEN strftime('%H:%M', "Hora_da_Ocorrencia") BETWEEN '12:00' AND '17:59' THEN '12:00 - 17:59'
              WHEN strftime('%H:%M', "Hora_da_Ocorrencia") BETWEEN '18:00' AND '23:59' THEN '18:00 - 23:59'
              ELSE 'Nulo'
            END AS Intervalo_Horario
      FROM "OCORRENCIA"
    )
    WHERE Intervalo_Horario <> 'Nulo'
    GROUP BY Intervalo_Horario
    ORDER BY Total_Ocorrencias DESC;
    """
    df = pd.read_sql_query(query, conn)
    df

  if block_op == 'aerodromo_acidentes_mais_graves':
    query = """
      SELECT Tipo_de_Aerodromo, COUNT(*) AS QUANTIDADES_DE_OCORRENCIAS
      FROM OCORRENCIA
      JOIN DESCRICAO ON OCORRENCIA.ID = DESCRICAO.ID
      JOIN AERODROMO ON DESCRICAO.ID = AERODROMO.ID
      WHERE (Classificacao_da_Ocorrencia = 'Incidente Grave' AND
            Tipo_de_Aerodromo <> 'null' AND
            Tipo_de_Aerodromo <> '-')
      GROUP BY Tipo_de_Aerodromo
      ORDER BY QUANTIDADES_DE_OCORRENCIAS DESC
    """
    df = pd.read_sql_query(query, conn)
    df

  if block_op == 'frequencias_ocorrencias':
    query = """
      SELECT Tipo_de_Ocorrencia, Descricao_do_Tipo, COUNT (*) AS QUANTIDADE_DE_OCORRENCIAS
      FROM OCORRENCIA
      JOIN DESCRICAO ON OCORRENCIA.ID = DESCRICAO.ID
      WHERE(Classificacao_da_Ocorrencia = 'Incidente Grave'
            AND Tipo_de_Ocorrencia <> 'null')
      GROUP BY Tipo_de_Ocorrencia
      ORDER BY
          QUANTIDADE_DE_OCORRENCIAS DESC
    """
    df = pd.read_sql_query(query, conn)
    df

  if block_op == 'rotas_casos_fatais_excursao':
    query = """
      SELECT
          Aerodromo_de_Destino,
          Aerodromo_de_Origem,
          COUNT(OCORRENCIA.ID) AS Numero_de_Ocorrencias
      FROM LOCAL JOIN ACIDENTE ON LOCAL.ID = ACIDENTE.ID
                JOIN OCORRENCIA ON OCORRENCIA.ID = ACIDENTE.ID
                JOIN DESCRICAO ON OCORRENCIA.ID = DESCRICAO.ID
      WHERE
          Lesoes_Fatais_Tripulantes IS NOT NULL AND Lesoes_Fatais_Tripulantes <> 0 AND
          Lesoes_Fatais_Passageiros IS NOT NULL AND Lesoes_Fatais_Passageiros <> 0 AND
          Lesoes_Fatais_Terceiros IS NOT NULL AND Lesoes_Fatais_Terceiros <> 0 AND
          Aerodromo_de_Destino IS NOT NULL AND Aerodromo_de_Destino <> '***' AND
          Aerodromo_de_Destino <> 'null' AND Aerodromo_de_Origem <> 'null' AND
          Aerodromo_de_Destino <> '****' AND Aerodromo_de_Origem <> '****' AND
          Aerodromo_de_Origem IS NOT NULL AND Aerodromo_de_Origem <> '***' AND
          Tipo_de_Ocorrencia = 'RE'

      GROUP BY Aerodromo_de_Destino, Aerodromo_de_Origem
      ORDER BY Numero_de_Ocorrencias DESC
      LIMIT 10;
    """
    df = pd.read_sql_query(query, conn)
    df

  conn.close()

if __name__ == '__main__':
  main()
