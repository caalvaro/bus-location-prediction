# -*- coding: utf-8 -*-

import os
import re  # Importado para extrair a hora do nome do arquivo
import logging
import io
from datetime import datetime, date, timedelta, timezone
import pytz  # Usado para o fuso horário correto
from multiprocessing import Pool, cpu_count

import orjson
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# --- Configurações e Constantes ---
# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do Banco de Dados
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "bus_data")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configurações do Pipeline
# ATENÇÃO: Altere esta variável para o caminho onde estão seus dados descompactados.
PASTA_RAIZ_DADOS = "./gps"
FUSO_HORARIO_LOCAL = pytz.timezone("America/Sao_Paulo")

# Constantes de Validação e Limpeza
HORA_INICIO_OPERACAO = 8
HORA_FIM_OPERACAO = 23  # Inclui o horário das 23:00 até 23:59:59
VELOCIDADE_MAX_KMH = 120
# Bounding Box aproximado para a região metropolitana do Rio de Janeiro
LAT_MIN, LAT_MAX = -23.1, -22.7
LON_MIN, LON_MAX = -43.8, -43.1

# Configurações de Performance
TAMANHO_LOTE_ARQUIVOS_PARALELOS = (
    10  # Número de arquivos para processar em cada lote paralelo
)
NUM_PROCESSOS = max(1, cpu_count() - 1)

# Configuração do Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s",
    handlers=[
        logging.FileHandler("/logs/log_pipeline_completo.log"),
        logging.StreamHandler(),
    ],
)


def conectar_bd():
    """Conecta ao banco de dados PostgreSQL. Retorna um objeto de conexão."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Erro fatal ao conectar ao banco de dados: {e}")
        return None


def configurar_schema_completo(conn):
    """
    Cria a tabela particionada para os dados limpos, se ela não existir.
    Inclui otimizações como colunas geoespaciais e índices.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cleaned_gps_pings (
        id BIGSERIAL,
        ordem VARCHAR(15) NOT NULL,
        linha VARCHAR(20),
        data_hora_servidor TIMESTAMP WITH TIME ZONE NOT NULL,
        latitude NUMERIC(10, 7) NOT NULL,
        longitude NUMERIC(10, 7) NOT NULL,
        velocidade SMALLINT,
        geom GEOMETRY(Point, 4326) NOT NULL,
        -- Chave primária composta para garantir unicidade e performance
        CONSTRAINT cleaned_gps_pings_pkey PRIMARY KEY (ordem, data_hora_servidor)
    ) PARTITION BY RANGE (data_hora_servidor);
    """

    # Índices são criados na tabela mãe e propagados para as partições
    create_geom_index_query = "CREATE INDEX IF NOT EXISTS idx_cleaned_pings_geom ON cleaned_gps_pings USING GIST (geom);"
    create_line_index_query = "CREATE INDEX IF NOT EXISTS idx_cleaned_pings_linha ON cleaned_gps_pings (linha);"

    try:
        with conn.cursor() as cur:
            logging.info("Verificando/Criando a tabela mãe 'cleaned_gps_pings'...")
            cur.execute(create_table_query)
            logging.info("Verificando/Criando índice geoespacial GIST...")
            cur.execute(create_geom_index_query)
            logging.info("Verificando/Criando índice na coluna 'linha'...")
            cur.execute(create_line_index_query)
            conn.commit()
            logging.info("Schema e índices da tabela mãe configurados com sucesso.")
    except psycopg2.Error as e:
        logging.error(f"Erro ao configurar o schema: {e}")
        conn.rollback()
        raise  # Levanta a exceção para parar o script se o schema falhar


def criar_particao_para_data(conn, target_date):
    """Cria uma partição para um dia específico se ela ainda não existir."""
    partition_name = f"cleaned_gps_pings_{target_date.strftime('%Y_%m_%d')}"
    start_of_day = target_date
    end_of_day = start_of_day + timedelta(days=1)

    create_partition_query = f"""
    CREATE TABLE IF NOT EXISTS {partition_name}
    PARTITION OF cleaned_gps_pings
    FOR VALUES FROM ('{start_of_day.strftime('%Y-%m-%d')}') TO ('{end_of_day.strftime('%Y-%m-%d')}');
    """
    try:
        with conn.cursor() as cur:
            # Verifica se a partição já existe de forma mais eficiente
            cur.execute("SELECT 1 FROM pg_class WHERE relname = %s;", (partition_name,))
            if cur.fetchone():
                return

            logging.info(f"Criando partição '{partition_name}'...")
            cur.execute(create_partition_query)
            conn.commit()
    except psycopg2.Error as e:
        # Erro "duplicate table" é esperado se outro processo criar a partição ao mesmo tempo
        if e.pgcode == "42P07":
            conn.rollback()
        else:
            logging.error(f"Erro ao criar partição para {target_date}: {e}")
            conn.rollback()
            raise


def processar_e_limpar_arquivo_worker(caminho_arquivo):
    """
    Worker para processamento paralelo. Lê um arquivo JSON e retorna uma lista de tuplas
    com os dados limpos e validados.
    """
    registros_limpos = []
    try:
        with open(caminho_arquivo, "rb") as f:
            dados_json = orjson.loads(f.read())

        for r in dados_json:
            try:
                # 1. Confia apenas no timestamp do servidor
                timestamp_servidor = int(r["datahoraservidor"]) / 1000
                registro_dt = datetime.fromtimestamp(
                    timestamp_servidor, tz=FUSO_HORARIO_LOCAL
                )

                # 2. **Validação e Limpeza**
                # Filtra pelo horário de operação
                if not (HORA_INICIO_OPERACAO <= registro_dt.hour <= HORA_FIM_OPERACAO):
                    continue

                # Valida campos essenciais
                if not all(k in r for k in ["ordem", "latitude", "longitude"]):
                    continue
                if not r["ordem"] or not r["latitude"] or not r["longitude"]:
                    continue

                latitude = float(r["latitude"].replace(",", "."))
                longitude = float(r["longitude"].replace(",", "."))
                velocidade = int(r.get("velocidade", 0) or 0)

                # Validações de consistência
                if not (
                    LAT_MIN <= latitude <= LAT_MAX and LON_MIN <= longitude <= LON_MAX
                ):
                    continue  # Fora da bounding box do RJ
                if not (0 <= velocidade <= VELOCIDADE_MAX_KMH):
                    continue  # Velocidade irreal

                # Adiciona à lista se passar em todas as validações
                # Formato da tupla corresponde à ordem das colunas no COPY
                registros_limpos.append(
                    (
                        r["ordem"],
                        r.get("linha"),
                        registro_dt,
                        latitude,
                        longitude,
                        velocidade,
                    )
                )
            except (ValueError, TypeError, KeyError):
                # Ignora registros individuais malformados dentro do JSON
                continue

    except Exception as e:
        logging.warning(f"Falha crítica ao processar o arquivo {caminho_arquivo}: {e}")

    return registros_limpos


def carregar_dados_com_copy(conn, dados_limpos):
    """
    Usa o método COPY, o mais rápido para inserção em massa no PostgreSQL.
    Insere dados limpos na tabela final.
    """
    if not dados_limpos:
        return 0

    # Ordem das colunas para o COPY
    colunas = [
        "ordem",
        "linha",
        "data_hora_servidor",
        "latitude",
        "longitude",
        "velocidade",
    ]

    # Cria uma tabela temporária que espelha a estrutura de entrada
    # A coluna de geometria será calculada na inserção final
    create_temp_table_query = """
    CREATE TEMP TABLE temp_cleaned_pings (
        ordem VARCHAR(15),
        linha VARCHAR(20),
        data_hora_servidor TIMESTAMP WITH TIME ZONE,
        latitude NUMERIC(10, 7),
        longitude NUMERIC(10, 7),
        velocidade SMALLINT
    ) ON COMMIT DROP;
    """

    # Comando para inserir da tabela temporária na tabela final,
    # calculando a geometria e tratando conflitos
    insert_from_temp_query = """
    INSERT INTO cleaned_gps_pings (
        ordem, linha, data_hora_servidor, latitude, longitude, velocidade, geom
    )
    SELECT
        ordem,
        linha,
        data_hora_servidor,
        latitude,
        longitude,
        velocidade,
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) -- Calcula a geometria
    FROM temp_cleaned_pings
    ON CONFLICT (ordem, data_hora_servidor) DO NOTHING;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(create_temp_table_query)

            execute_values(
                cur,
                f"INSERT INTO temp_cleaned_pings ({', '.join(colunas)}) VALUES %s",
                dados_limpos,
            )

            cur.execute(insert_from_temp_query)

            num_registros = cur.rowcount
            conn.commit()
            logging.info(
                f"{num_registros} registros limpos inseridos/atualizados no banco de dados."
            )
            return num_registros
    except psycopg2.Error as e:
        logging.error(f"Erro ao carregar dados com COPY: {e}")
        conn.rollback()
        return 0


def processar_carga_completa():
    """Função principal que orquestra todo o pipeline."""
    logging.info("--- INICIANDO PIPELINE DE INGESTÃO E LIMPEZA ---")

    # Passo 1: Descobrir e FILTRAR arquivos
    todos_arquivos_encontrados = []
    for dirpath, _, filenames in os.walk(PASTA_RAIZ_DADOS):
        for filename in filenames:
            if filename.endswith(".json"):
                todos_arquivos_encontrados.append(os.path.join(dirpath, filename))

    logging.info(
        f"{len(todos_arquivos_encontrados)} arquivos .json encontrados no diretório."
    )

    arquivos_para_processar = []
    # Expressão regular para extrair a hora (ex: '2024-05-10_08.json' -> '08')
    padrao_hora = re.compile(r"_(\d{2})\.json$")

    for caminho_arquivo in todos_arquivos_encontrados:
        match = padrao_hora.search(os.path.basename(caminho_arquivo))
        if match:
            try:
                hora = int(match.group(1))
                if HORA_INICIO_OPERACAO <= hora <= HORA_FIM_OPERACAO:
                    arquivos_para_processar.append(caminho_arquivo)
            except ValueError:
                continue  # Ignora se o valor da hora não for um número

    if not arquivos_para_processar:
        logging.warning(
            "Nenhum arquivo encontrado no intervalo de horário de operação. Encerrando."
        )
        return

    logging.info(
        f"FILTRO: {len(arquivos_para_processar)} arquivos foram selecionados para processamento (horário {HORA_INICIO_OPERACAO}h-{HORA_FIM_OPERACAO}h)."
    )

    # Passo 2: Configurar o banco de dados
    conn = conectar_bd()
    if not conn:
        return

    try:
        configurar_schema_completo(conn)

        # Cria as partições necessárias com base nos nomes dos arquivos
        datas_necessarias = set(
            date.fromisoformat(os.path.basename(f).split("_")[0])
            for f in arquivos_para_processar
        )
        logging.info(
            f"Garantindo a existência de {len(datas_necessarias)} partições..."
        )
        for dt in datas_necessarias:
            criar_particao_para_data(conn, dt)

        # Passo 3: Processamento paralelo e carga em lotes
        total_registros_inseridos = 0
        with Pool(processes=NUM_PROCESSOS) as pool:
            for i in range(
                0, len(arquivos_para_processar), TAMANHO_LOTE_ARQUIVOS_PARALELOS
            ):
                lote_arquivos = arquivos_para_processar[
                    i : i + TAMANHO_LOTE_ARQUIVOS_PARALELOS
                ]
                logging.info(
                    f"Processando lote {i//TAMANHO_LOTE_ARQUIVOS_PARALELOS + 1} com {len(lote_arquivos)} arquivos..."
                )

                resultados_paralelos = pool.map(
                    processar_e_limpar_arquivo_worker, lote_arquivos
                )

                # Achata a lista de listas em uma única lista de tuplas
                dados_limpos_lote = [
                    registro
                    for sublista in resultados_paralelos
                    for registro in sublista
                ]

                if dados_limpos_lote:
                    total_registros_inseridos += carregar_dados_com_copy(
                        conn, dados_limpos_lote
                    )

    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

    logging.info("--- RESUMO DO PIPELINE ---")
    logging.info(
        f"Total de registros limpos inseridos no banco: {total_registros_inseridos}"
    )
    logging.info("--- PIPELINE FINALIZADO ---")


if __name__ == "__main__":
    if (
        not os.path.isdir(PASTA_RAIZ_DADOS)
        or PASTA_RAIZ_DADOS == "C:\\path\\to\\your\\unzipped\\data"
    ):
        logging.error(
            "!!! ERRO: A variável 'PASTA_RAIZ_DADOS' não foi configurada.         !!!"
        )
    else:
        processar_carga_completa()
