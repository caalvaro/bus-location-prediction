# -*- coding: utf-8 -*-
"""
Script-Ferramenta para Inserir ou Atualizar Terminais em Lote.

Este script lê uma lista de terminais predefinida e insere ou atualiza
cada um na base de dados. É ideal para inserir vários terminais de uma só vez
de forma rápida e repetível.

Para usar:
1. Preencha a lista 'DADOS_PARA_INSERIR' com os dados das linhas.
2. Execute o script.
"""
import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- Configurações e Constantes ---
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "bus_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

# --- PARÂMETROS ---
# Especifique o nome da tabela onde os terminais validados devem ser guardados.
TABELA_TERMINAIS_DESTINO = "terminais_selecionados_validacao_v2"

# --- DADOS PARA INSERÇÃO EM LOTE ---
# Adicione ou modifique os dicionários nesta lista para inserir/atualizar os terminais.
# Cada dicionário representa uma linha de autocarro.
DADOS_PARA_INSERIR = [
    {
        "linha": "100",
        "lat_a": -22.98551,
        "lon_a": -43.19702,
        "lat_b": -22.90434,
        "lon_b": -43.19196,
    },
    {
        "linha": "355",
        "lat_a": -22.89684,
        "lon_a": -43.18807,
        "lat_b": -22.87083,
        "lon_b": -43.34178,
    },
    {
        "linha": "550",
        "lat_a": -22.97816,
        "lon_a": -43.22701,
        "lat_b": -22.94708,
        "lon_b": -43.36018,
    },
    {
        "linha": "779",
        "lat_a": -22.80645,
        "lon_a": -43.36478,
        "lat_b": -22.87784,
        "lon_b": -43.33596,
    },
    {
        "linha": "864",
        "lat_a": -22.90239,
        "lon_a": -43.55574,
        "lat_b": -22.87624,
        "lon_b": -43.46340,
    },
    {
        "linha": "371",
        "lat_a": -22.90284,
        "lon_a": -43.34859,
        "lat_b": -22.90841,
        "lon_b": -43.18929,
    },
    {
        "linha": "415",
        "lat_a": -22.97964,
        "lon_a": -43.22249,
        "lat_b": -22.94627,
        "lon_b": -43.25676,
    },
    {
        "linha": "638",
        "lat_a": -22.92530,
        "lon_a": -43.23307,
        "lat_b": -22.85364,
        "lon_b": -43.37626,
    },
    {
        "linha": "852",
        "lat_a": -22.90133,
        "lon_a": -43.55489,
        "lat_b": -22.99887,
        "lon_b": -43.64907,
    },
]


# --- Configuração Inicial ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
)
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def processar_insercao_em_lote(dados: list):
    """
    Itera sobre a lista de dados e insere ou atualiza cada terminal na base de dados.

    Args:
        dados (list): Uma lista de dicionários, onde cada dicionário contém
                      as informações de uma linha e seus terminais.
    """
    if not dados:
        logging.warning(
            "A lista 'DADOS_PARA_INSERIR' está vazia. Nenhum dado foi processado."
        )
        return

    logging.info(f"Iniciando processamento em lote para {len(dados)} linha(s)...")

    # A consulta usa ST_MakePoint para criar a geometria a partir das coordenadas.
    # ON CONFLICT (linha) DO UPDATE torna a operação "upsert":
    # - Se a linha não existe, insere.
    # - Se a linha já existe, atualiza os valores.
    query = text(
        f"""
        INSERT INTO {TABELA_TERMINAIS_DESTINO} (linha, geom_a, geom_b, score) 
        VALUES (
            :linha, 
            ST_SetSRID(ST_MakePoint(:lon_a, :lat_a), 4326), 
            ST_SetSRID(ST_MakePoint(:lon_b, :lat_b), 4326),
            -1.0 -- Usamos um score de -1 para indicar que foi uma inserção manual
        )
        ON CONFLICT (linha) DO UPDATE SET 
            geom_a = EXCLUDED.geom_a, 
            geom_b = EXCLUDED.geom_b,
            score = EXCLUDED.score;
    """
    )

    with engine.connect() as connection:
        for item in dados:
            try:
                params = {
                    "linha": item["linha"],
                    "lon_a": item["lon_a"],
                    "lat_a": item["lat_a"],
                    "lon_b": item["lon_b"],
                    "lat_b": item["lat_b"],
                }
                connection.execute(query, params)
                logging.info(
                    f"Dados para a linha '{item['linha']}' processados com sucesso."
                )
            except KeyError as e:
                logging.error(
                    f"Erro de formatação no item para a linha '{item.get('linha', 'DESCONHECIDA')}'. Chave em falta: {e}"
                )
            except Exception as e:
                logging.error(
                    f"Ocorreu um erro ao tentar guardar os dados para a linha '{item.get('linha', 'DESCONHECIDA')}': {e}"
                )

        # Confirma todas as transações de uma só vez no final.
        connection.commit()

    logging.info("Processamento em lote concluído.")


if __name__ == "__main__":
    processar_insercao_em_lote(DADOS_PARA_INSERIR)
