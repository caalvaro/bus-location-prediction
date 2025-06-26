import os
import logging
import time
import io
from functools import partial

import geopandas as gpd
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from multiprocessing import Pool

# --- Configurações e Constantes ---
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "bus_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

# --- Parâmetros Técnicos ---
CRS_GEOGRAFICO = "EPSG:4326"
CRS_METRICO = "EPSG:32723"
TAMANHO_LOTE = 100_000

# --- Parâmetros de Execução ---
MAX_WORKERS = max(1, os.cpu_count() - 1)

# --- Configuração Inicial ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - PID:%(process)d - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        logging.FileHandler("log_feature_engineering_final.log", mode="w"),
        logging.StreamHandler(),
    ],
)
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
MAIN_ENGINE = create_engine(DB_URL)


def preparar_tabelas(engine: Engine):
    """Cria as tabelas de destino e as limpa para uma nova execução."""
    logging.info("Iniciando a criação/verificação das tabelas de destino...")
    with engine.connect() as connection:
        # Usar uma transação explícita para agrupar os comandos DDL
        trans = connection.begin()
        try:
            connection.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS feature_table (
                    id BIGSERIAL PRIMARY KEY, id_ping_original BIGINT NOT NULL, id_rota_canonica INT NOT NULL,
                    sentido VARCHAR(10) NOT NULL, progresso_rota_m FLOAT NOT NULL, hora_do_dia INT,
                    dia_da_semana INT, fim_de_semana BOOLEAN
                );
            """
                )
            )
            connection.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS velocity_model (
                    id_rota_canonica INT NOT NULL, id_segmento INT NOT NULL, dia_da_semana INT NOT NULL,
                    faixa_horaria INT NOT NULL, velocidade_media_kmh FLOAT NOT NULL, contagem_pings INT NOT NULL,
                    PRIMARY KEY (id_rota_canonica, id_segmento, dia_da_semana, faixa_horaria)
                );
            """
                )
            )
            logging.info("Limpando tabelas para a nova execução...")
            connection.execute(
                text("TRUNCATE TABLE feature_table, velocity_model RESTART IDENTITY;")
            )
            trans.commit()
        except Exception:
            trans.rollback()
            raise
    logging.info("Tabelas de destino prontas e limpas.")


def bulk_insert_dataframe(df: pd.DataFrame, table_name: str, engine: Engine):
    """Versão robusta do bulk insert, com transação explícita para segurança em multiprocessamento."""
    if df.empty:
        return 0

    num_rows = len(df)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    inserted_rows = 0
    with engine.connect() as connection:
        # Iniciar a transação explicitamente é mais seguro em contextos paralelos
        trans = connection.begin()
        try:
            dbapi_conn = connection.connection
            with dbapi_conn.cursor() as cursor:
                cursor.copy_expert(
                    f"COPY {table_name} ({','.join(df.columns)}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                    buffer,
                )
                # O cursor.rowcount pode ser -1 dependendo do driver, por isso usamos num_rows como fallback
                inserted_rows = cursor.rowcount if cursor.rowcount != -1 else num_rows
            trans.commit()
        except Exception as e:
            logging.error(
                f"Erro CRÍTICO durante o bulk insert, revertendo a transação: {e}",
                exc_info=True,
            )
            trans.rollback()
            raise  # Propagar o erro para que o processo principal saiba que falhou
    return inserted_rows


def tarefa_processar_linha(linha: str, db_url: str):
    """Worker que executa o pipeline de engenharia de atributos para uma linha de ônibus."""
    logging.info(f"[{linha}] Iniciando processo.")
    engine = create_engine(db_url)
    total_processado = 0

    try:
        # 1. Carregar e validar rotas canônicas
        query_rotas = text(
            "SELECT id_rota, sentido, trajeto FROM rotas_canonicas WHERE linha = :linha"
        )
        rotas_df = gpd.read_postgis(
            query_rotas,
            engine,
            params={"linha": linha},
            geom_col="trajeto",
            crs=CRS_GEOGRAFICO,
        )

        rotas_df_ida = rotas_df[rotas_df["sentido"] == "ida"]
        rotas_df_volta = rotas_df[rotas_df["sentido"] == "volta"]
        if not (len(rotas_df_ida) == 1 and len(rotas_df_volta) == 1):
            msg = "Rota de ida/volta não encontrada ou duplicada."
            logging.warning(f"[{linha}] {msg}")
            return {"linha": linha, "status": "FALHA_ROTA", "mensagem": msg}

        rota_ida_info = rotas_df_ida.iloc[0]
        rota_volta_info = rotas_df_volta.iloc[0]
        rota_metric_ida = rotas_df_ida.to_crs(CRS_METRICO).geometry.iloc[0]
        rota_metric_volta = rotas_df_volta.to_crs(CRS_METRICO).geometry.iloc[0]

        offset = 0
        while True:
            query_pings = text(
                """
                SELECT id, data_hora_servidor, geom FROM cleaned_gps_pings 
                WHERE linha = :linha ORDER BY id LIMIT :limit OFFSET :offset
            """
            )
            gdf_lote = gpd.read_postgis(
                query_pings,
                engine,
                params={"linha": linha, "limit": TAMANHO_LOTE, "offset": offset},
                geom_col="geom",
                crs=CRS_GEOGRAFICO,
            )

            if gdf_lote.empty:
                break

            gdf_lote_metric = gdf_lote.to_crs(CRS_METRICO)
            gdf_lote["sentido"] = np.where(
                gdf_lote_metric.distance(rota_metric_ida)
                < gdf_lote_metric.distance(rota_metric_volta),
                "ida",
                "volta",
            )
            progresso = pd.Series(index=gdf_lote.index, dtype=float)
            ida_mask = gdf_lote["sentido"] == "ida"
            if ida_mask.any():
                progresso[ida_mask] = rota_metric_ida.project(
                    gdf_lote_metric.loc[ida_mask].geometry
                )
            if (~ida_mask).any():
                progresso[~ida_mask] = rota_metric_volta.project(
                    gdf_lote_metric.loc[~ida_mask].geometry
                )
            gdf_lote["progresso_rota_m"] = progresso
            id_map = {
                "ida": rota_ida_info["id_rota"],
                "volta": rota_volta_info["id_rota"],
            }
            gdf_lote["id_rota_canonica"] = gdf_lote["sentido"].map(id_map)
            gdf_lote["hora_do_dia"] = gdf_lote["data_hora_servidor"].dt.hour
            gdf_lote["dia_da_semana"] = gdf_lote["data_hora_servidor"].dt.weekday + 1
            gdf_lote["fim_de_semana"] = gdf_lote["dia_da_semana"].isin([6, 7])

            df_para_inserir = gdf_lote[
                [
                    "id",
                    "id_rota_canonica",
                    "sentido",
                    "progresso_rota_m",
                    "hora_do_dia",
                    "dia_da_semana",
                    "fim_de_semana",
                ]
            ].rename(columns={"id": "id_ping_original"})

            linhas_inseridas = bulk_insert_dataframe(
                df_para_inserir, "feature_table", engine
            )
            total_processado += linhas_inseridas
            offset += TAMANHO_LOTE

        if total_processado > 0:
            logging.info(f"[{linha}] Concluído. Total processado: {total_processado}")
            return {
                "linha": linha,
                "status": "SUCESSO",
                "pings_processados": total_processado,
            }
        else:
            logging.warning(
                f"[{linha}] Concluído, mas nenhum ping encontrado para esta linha."
            )
            return {"linha": linha, "status": "SEM_DADOS", "pings_processados": 0}

    except Exception as e:
        logging.critical(
            f"[{linha}] Erro fatal no processo trabalhador: {e}", exc_info=True
        )
        return {"linha": linha, "status": "ERRO_FATAL", "mensagem": str(e)}
    finally:
        if "engine" in locals():
            engine.dispose()


def construir_modelo_velocidade(engine: Engine):
    """Executa a agregação final em SQL para popular a tabela `velocity_model`."""
    logging.info("Iniciando a construção do modelo de velocidade (agregação em SQL)...")
    query = text(
        """
        INSERT INTO velocity_model (id_rota_canonica, id_segmento, dia_da_semana, faixa_horaria, velocidade_media_kmh, contagem_pings)
        SELECT
            ft.id_rota_canonica,
            FLOOR(ft.progresso_rota_m / 100)::INT AS id_segmento,
            ft.dia_da_semana,
            ft.hora_do_dia AS faixa_horaria,
            COALESCE(AVG(p.velocidade), 0) AS velocidade_media_kmh,
            COUNT(p.id) AS contagem_pings
        FROM feature_table ft
        JOIN cleaned_gps_pings p ON ft.id_ping_original = p.id
        WHERE p.velocidade IS NOT NULL AND p.velocidade > 0
        GROUP BY ft.id_rota_canonica, id_segmento, ft.dia_da_semana, faixa_horaria
        ON CONFLICT (id_rota_canonica, id_segmento, dia_da_semana, faixa_horaria) DO UPDATE 
        SET 
          velocidade_media_kmh = (velocity_model.velocidade_media_kmh * velocity_model.contagem_pings + EXCLUDED.velocidade_media_kmh * EXCLUDED.contagem_pings) / (velocity_model.contagem_pings + EXCLUDED.contagem_pings),
          contagem_pings = velocity_model.contagem_pings + EXCLUDED.contagem_pings;
    """
    )
    try:
        with engine.connect() as connection:
            trans = connection.begin()
            connection.execute(query)
            trans.commit()
        logging.info("Modelo de velocidade construído com sucesso.")
    except Exception as e:
        logging.error(f"Falha ao construir o modelo de velocidade: {e}", exc_info=True)


def main():
    """Função principal para orquestrar o pipeline."""
    start_time = time.time()
    logging.info("--- INICIANDO FASE 2: ENGENHARIA DE ATRIBUTOS (VERSÃO FINAL) ---")

    preparar_tabelas(MAIN_ENGINE)

    with MAIN_ENGINE.connect() as connection:
        result = connection.execute(
            text("SELECT DISTINCT linha FROM rotas_canonicas ORDER BY linha;")
        )
        linhas_para_processar = [row[0] for row in result]

    if not linhas_para_processar:
        logging.warning("Nenhuma linha encontrada em 'rotas_canonicas'. Encerrando.")
        return

    logging.info(
        f"Encontradas {len(linhas_para_processar)} linhas. Usando {MAX_WORKERS} workers."
    )

    # --- Processamento Paralelo com Pool ---
    with Pool(processes=MAX_WORKERS) as pool:
        worker_func = partial(tarefa_processar_linha, db_url=DB_URL)
        resultados = pool.map(worker_func, linhas_para_processar)

    # --- Resumo da Execução ---
    logging.info("--- RESUMO DA EXECUÇÃO DA ENGENHARIA DE ATRIBUTOS ---")
    sucessos = sum(1 for r in resultados if r["status"] == "SUCESSO")
    sem_dados = sum(1 for r in resultados if r["status"] == "SEM_DADOS")
    falhas_rota = sum(1 for r in resultados if r["status"] == "FALHA_ROTA")
    erros_fatais = sum(1 for r in resultados if r["status"] == "ERRO_FATAL")
    total_pings = sum(
        r.get("pings_processados", 0) for r in resultados if r["status"] == "SUCESSO"
    )

    logging.info(f"Total de Linhas: {len(linhas_para_processar)}")
    logging.info(f"  - Sucesso (com dados): {sucessos}")
    logging.info(f"  - Sucesso (sem dados): {sem_dados}")
    logging.info(f"  - Falha (rota inválida): {falhas_rota}")
    logging.info(f"  - Falha (erro fatal): {erros_fatais}")
    logging.info(f"Total de Pings Inseridos na feature_table: {total_pings}")

    if total_pings > 0:
        construir_modelo_velocidade(MAIN_ENGINE)
    else:
        logging.warning(
            "Nenhum dado foi inserido na feature_table. Pulando a construção do modelo de velocidade."
        )

    end_time = time.time()
    logging.info(f"--- FASE 2 FINALIZADA EM {end_time - start_time:.2f} SEGUNDOS. ---")


if __name__ == "__main__":
    main()
