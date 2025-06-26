# -*- coding: utf-8 -*-
"""
Script Final e Refatorado para Identificação de Terminais com Validação Visual.

Estratégia Robusta e de Alto Desempenho:
1.  **Identificação do Esqueleto Completo:** Primeiro, o script identifica todas as
    células da grelha que uma linha visita, incluindo os outliers.
2.  **Remoção de Outliers por Conectividade:** Um DBSCAN é executado na grelha
    completa para encontrar o maior componente conectado (o "corredor principal") e remover
    as "ilhas" de células, como as garagens.
3.  **Identificação de Zonas Quentes:** Paralelamente, a análise de tempo contíguo
    identifica as células da grelha com paragens longas e frequentes.
4.  **Filtragem por Interseção:** O script mantém apenas as zonas quentes que fazem
    parte do corredor principal, eliminando eficazmente as garagens.
5.  **Seleção de Par Ótimo:** A heurística (Frequência * Distância) é aplicada
    neste conjunto de candidatos já limpo e validado.
6.  **Visualização Detalhada:** O mapa final contém camadas para cada etapa,
    permitindo uma depuração visual completa do processo.
"""
import os
import logging
from datetime import date, timedelta
from itertools import combinations
from multiprocessing import Process, Queue

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
from sklearn.cluster import DBSCAN

# Importa o tipo 'Engine' para a anotação de tipo correta.
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import folium

# Adicionado para a paleta de cores da visualização de depuração
import matplotlib.pyplot as plt
from matplotlib.colors import to_hex

# --- Configurações e Constantes ---
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "bus_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

# --- Parâmetros da Lógica de Negócio ---
VELOCIDADE_MAX_PARADA = 2
MIN_DURACAO_TERMINAL_MINUTOS = 5
MAX_DURACAO_TERMINAL_MINUTOS = 80
MAX_INTERVALO_ENTRE_PINGS_MIN = 30

# --- Parâmetros da Partição de Tempo ---
DATA_FIM_ANALISE = date(2024, 5, 12)
DATA_INICIO_ANALISE = DATA_FIM_ANALISE - timedelta(days=13)

# --- Parâmetros Técnicos ---
CRS_GEOGRAFICO = "EPSG:4326"
CRS_METRICO = "EPSG:32723"
TAMANHO_GRADE_GRAUS = 0.001
DENSIDADE_MINIMA_PONTOS_ESQUELETO = 10
# Parâmetros para o DBSCAN que encontra o corredor principal na grelha
DBSCAN_EPS_GRELHA = TAMANHO_GRADE_GRAUS * 3
DBSCAN_MIN_SAMPLES_GRELHA = (
    2  # Exige que o corredor principal tenha pelo menos n células
)
# Parâmetros para o DBSCAN que agrupa as zonas quentes em candidatos finais
DBSCAN_EPS_METROS_FINAL = 200
DBSCAN_MIN_SAMPLES_FINAL = 5

# --- Parâmetros de Execução ---
LINHAS_PARA_PROCESSAR = [
    "100",
    # "108",
    # "232",
    # "2336",
    # "2803",
    # "292",
    # "298",
    # "3",
    # "309",
    # "315",
    # "324",
    # "328",
    # "343",
    "355",
    "371",
    # "388",
    # "397",
    # "399",
    "415",
    # "422",
    # "457",
    # "483",
    # "497",
    "550",
    # "553",
    # "554",
    # "557",
    # "565",
    # "606",
    # "624",
    # "629",
    # "634",
    "638",
    # "639",
    # "665",
    # "756",
    # "759",
    # "774",
    "779",
    # "803",
    # "838",
    # "852", # linha circular
    "864",
    # "867",
    # "878",
    # "905",
    # "917",
    # "918",
]
ARQUIVO_SAIDA_MAPA = "mapa_validacao_terminais_v2.html"
NOME_TABELA = "terminais_selecionados_validacao_v2"

MAX_PROCESSOS_SIMULTANEOS = 4

# --- Configuração Inicial ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        logging.FileHandler("log_validacao_terminais.log"),
        logging.StreamHandler(),
    ],
)
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def criar_tabela_terminais_selecionados():
    """
    Cria a tabela de destino para os terminais selecionados, se ela não existir.
    Esta função garante que o script tenha onde guardar os resultados finais.
    """
    logging.info(f"Verificando/Criando tabela '{NOME_TABELA}'...")
    with engine.connect() as connection:
        connection.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {NOME_TABELA} (
                id SERIAL PRIMARY KEY,
                linha VARCHAR(20) NOT NULL UNIQUE,
                geom_a GEOMETRY(Point, 4326) NOT NULL,
                geom_b GEOMETRY(Point, 4326) NOT NULL,
                score FLOAT NOT NULL
            );
        """
            )
        )
        connection.commit()


def obter_esqueleto_da_grelha(linha: str, engine: Engine) -> gpd.GeoDataFrame | None:
    """Etapa 1: Identifica todas as células da grelha que a linha visita."""
    logging.info(f"Identificando o esqueleto completo da grelha...")
    query = """
        SELECT 
            ST_Centroid(ST_Collect(geom)) as geom,
            grid_id
        FROM (
            SELECT 
                geom,
                (TRUNC(ST_X(geom) / :tamanho_grade))::text || '_' || (TRUNC(ST_Y(geom) / :tamanho_grade))::text as grid_id
            FROM cleaned_gps_pings
            WHERE linha = :linha AND data_hora_servidor BETWEEN :data_inicio AND :data_fim
        ) as sub
        GROUP BY grid_id
        HAVING COUNT(*) > :densidade_minima;
    """
    params = {
        "linha": linha,
        "data_inicio": DATA_INICIO_ANALISE,
        "data_fim": DATA_FIM_ANALISE,
        "tamanho_grade": TAMANHO_GRADE_GRAUS,
        "densidade_minima": DENSIDADE_MINIMA_PONTOS_ESQUELETO,
    }
    try:
        gdf = gpd.read_postgis(
            text(query), engine, params=params, geom_col="geom", crs=CRS_GEOGRAFICO
        )
        if gdf.empty:
            logging.warning("Nenhum esqueleto de rota encontrado.")
            return None
        return gdf
    except Exception as e:
        logging.error(f"Falha ao buscar o esqueleto da grelha: {e}")
        return None


def identificar_corredor_principal(
    gdf_esqueleto: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame | None]:
    """Etapa 2: Clusteriza a grelha para encontrar o "corredor principal"."""
    logging.info("Clusterizando a grelha para encontrar o corredor principal...")
    coords = np.array([p.coords[0] for p in gdf_esqueleto.geometry])
    db = DBSCAN(
        eps=DBSCAN_EPS_GRELHA, min_samples=DBSCAN_MIN_SAMPLES_GRELHA, n_jobs=-1
    ).fit(coords)
    gdf_esqueleto["cluster"] = db.labels_

    valid_clusters = gdf_esqueleto[gdf_esqueleto["cluster"] != -1]
    if valid_clusters.empty:
        logging.warning("Nenhum corredor principal pôde ser identificado.")
        return gdf_esqueleto, None

    maiores_clusters_id = set(
        valid_clusters["cluster"].value_counts().nlargest(2).index
    )

    gdf_esqueleto_limpo = gdf_esqueleto[
        gdf_esqueleto["cluster"].isin(maiores_clusters_id)
    ].copy()

    gdf_esqueleto_valido = gdf_esqueleto_limpo[gdf_esqueleto_limpo["cluster"] != -1]

    logging.info(
        f"Corredor principal identificado com {len(gdf_esqueleto_valido)} células (removidos {len(gdf_esqueleto) - len(gdf_esqueleto_valido)} outliers)."
    )
    return gdf_esqueleto, gdf_esqueleto_valido


def identificar_zonas_quentes(linha: str, engine: Engine) -> gpd.GeoDataFrame | None:
    """Etapa 3: Identifica as "zonas quentes" (células com paragens longas)."""
    logging.info("Identificando zonas quentes (paragens longas)...")
    query = "SELECT ordem, data_hora_servidor, geom FROM cleaned_gps_pings WHERE linha = :l AND data_hora_servidor BETWEEN :d_ini AND :d_fim AND velocidade <= :v_max"
    gdf_parados = gpd.read_postgis(
        text(query),
        engine,
        params={
            "l": linha,
            "d_ini": DATA_INICIO_ANALISE,
            "d_fim": DATA_FIM_ANALISE,
            "v_max": VELOCIDADE_MAX_PARADA,
        },
        geom_col="geom",
        crs=CRS_GEOGRAFICO,
    )
    if gdf_parados.empty:
        logging.warning("Nenhum ponto parado encontrado para análise de zonas quentes.")
        return None

    # atribui cada ping em um ponto da grade
    gdf_parados["grid_id"] = gdf_parados.geometry.apply(
        lambda p: f"{int(p.x / TAMANHO_GRADE_GRAUS)}_{int(p.y / TAMANHO_GRADE_GRAUS)}"
    )
    gdf_parados.sort_values(by=["grid_id", "ordem", "data_hora_servidor"], inplace=True)

    # calcula a diferença de tempo entre um ping e o anterior
    gdf_parados["diff_min"] = (
        gdf_parados.groupby(["grid_id", "ordem"])["data_hora_servidor"]
        .diff()
        .dt.total_seconds()
        .fillna(0)
        / 60
    )

    # id da viagem: quando o gps para de enviar sinal por um tempo, considera uma viagem diferente
    # seria interessante fazer uma análise dos pontos em que o gps parou de enviar sinal
    gdf_parados["id_bloco"] = (
        gdf_parados["diff_min"] > MAX_INTERVALO_ENTRE_PINGS_MIN
    ).cumsum()

    # seleciona só os pontos da grade que ficaram parados pelo tempo definido
    eventos = [
        {"grid_id": gid}
        for (gid, o, _), g in gdf_parados.groupby(["grid_id", "ordem", "id_bloco"])
        if MIN_DURACAO_TERMINAL_MINUTOS
        <= (
            (
                g["data_hora_servidor"].max() - g["data_hora_servidor"].min()
            ).total_seconds()
            / 60
        )
        <= MAX_DURACAO_TERMINAL_MINUTOS
    ]
    if not eventos:
        return None

    df_eventos = pd.DataFrame(eventos)
    stats_zonas = df_eventos.groupby("grid_id").size().reset_index(name="frequencia")
    centroides = (
        gdf_parados.groupby("grid_id")["geom"]
        .apply(lambda geoms: geoms.union_all().centroid)
        .rename("geom")
    )
    return gpd.GeoDataFrame(
        stats_zonas.merge(centroides, on="grid_id"), geometry="geom", crs=CRS_GEOGRAFICO
    )


def selecionar_par_otimo(gdf_candidatos: gpd.GeoDataFrame) -> tuple | None:
    """Etapa 6: Avalia todos os pares de candidatos e escolhe o melhor."""
    if len(gdf_candidatos) < 2:
        logging.warning("Menos de 2 candidatos a terminal para selecionar.")
        return None

    melhor_par, max_score = None, -1
    for idx_a, idx_b in combinations(gdf_candidatos.index, 2):
        candidato_a = gdf_candidatos.iloc[idx_a]
        candidato_b = gdf_candidatos.iloc[idx_b]
        distancia = candidato_a["geom"].distance(candidato_b["geom"])
        hausdorff = candidato_a["geom"].hausdorff_distance(candidato_b["geom"])

        score = (
            (candidato_a["frequencia"] + candidato_b["frequencia"])
            * distancia
            * hausdorff
        )
        if score > max_score:
            max_score, melhor_par = score, (candidato_a, candidato_b)

    return melhor_par, max_score


def tarefa_de_processamento(linha: str, queue: Queue):
    """
    Função alvo para o processamento paralelo. Executa a análise completa para uma
    única linha e coloca o dicionário de resultados na fila para o processo principal.
    """
    resultado_final = {
        "linha": linha,
        "sucesso": False,
        "motivo": "Processo não concluído.",
    }
    try:
        engine = create_engine(db_url)
        logging.info(f"Iniciando processamento para a linha {linha}.")

        gdf_esqueleto = obter_esqueleto_da_grelha(linha, engine)
        if gdf_esqueleto is None:
            resultado_final["motivo"] = "Nenhum esqueleto de rota."
            queue.put(resultado_final)
            return

        gdf_esqueleto_com_clusters, gdf_esqueleto_limpo = (
            identificar_corredor_principal(gdf_esqueleto)
        )
        if gdf_esqueleto_limpo is None:
            resultado_final["motivo"] = "Nenhum corredor principal."
            queue.put(resultado_final)
            return

        gdf_zonas_quentes = identificar_zonas_quentes(linha, engine)
        if gdf_zonas_quentes is None:
            resultado_final["motivo"] = "Nenhuma zona quente."
            queue.put(resultado_final)
            return

        grid_ids_corredor = set(gdf_esqueleto_limpo["grid_id"])
        gdf_zonas_filtradas = gdf_zonas_quentes[
            gdf_zonas_quentes["grid_id"].isin(grid_ids_corredor)
        ].copy()
        if gdf_zonas_filtradas.empty:
            resultado_final["motivo"] = "Nenhuma zona quente no corredor."
            queue.put(resultado_final)
            return

        gdf_utm = gdf_zonas_filtradas.to_crs(CRS_METRICO)
        db_final = DBSCAN(
            eps=DBSCAN_EPS_METROS_FINAL, min_samples=DBSCAN_MIN_SAMPLES_FINAL, n_jobs=-1
        ).fit(np.array([p.coords[0] for p in gdf_utm.geometry]))
        gdf_zonas_filtradas["id_candidato"] = db_final.labels_
        gdf_zonas_validas = gdf_zonas_filtradas[
            gdf_zonas_filtradas["id_candidato"] != -1
        ]

        df_stats_candidatos = (
            gdf_zonas_validas.groupby("id_candidato")
            .agg(
                frequencia=("frequencia", "sum"),
                geom=("geom", lambda g: gpd.GeoSeries(g).union_all().centroid),
            )
            .reset_index()
        )
        stats_candidatos = gpd.GeoDataFrame(
            df_stats_candidatos, geometry="geom", crs=CRS_GEOGRAFICO
        )

        resultado_selecao = selecionar_par_otimo(stats_candidatos)
        if not resultado_selecao or resultado_selecao[0] is None:
            resultado_final["motivo"] = "Menos de 2 candidatos finais."
            queue.put(resultado_final)
            return

        melhor_par, max_score = resultado_selecao
        terminal_a, terminal_b = melhor_par

        with engine.connect() as connection:
            params_insert = {
                "linha": linha,
                "wkt_a": terminal_a["geom"].wkt,
                "wkt_b": terminal_b["geom"].wkt,
                "score": float(max_score),
            }
            connection.execute(
                text(
                    f"""
                INSERT INTO {NOME_TABELA} (linha, geom_a, geom_b, score) 
                VALUES (:linha, ST_SetSRID(ST_GeomFromText(:wkt_a), 4326), ST_SetSRID(ST_GeomFromText(:wkt_b), 4326), :score) 
                ON CONFLICT (linha) DO UPDATE
                SET geom_a = EXCLUDED.geom_a, geom_b = EXCLUDED.geom_b, score = EXCLUDED.score;
            """
                ),
                params_insert,
            )
            connection.commit()

        # Prepara o dicionário de resultados para a visualização
        resultado_final.update(
            {
                "sucesso": True,
                "esqueleto_clusterizado": gdf_esqueleto_com_clusters,
                "esqueleto_limpo": gdf_esqueleto_limpo,
                "candidatos_finais": stats_candidatos,
                "terminal_a": terminal_a,
                "terminal_b": terminal_b,
                "motivo": "Sucesso",
            }
        )
        logging.info(f"Processamento para a linha {linha} concluído com sucesso.")

    except Exception as e:
        logging.critical(f"Erro fatal no processo da linha {linha}: {e}", exc_info=True)
        resultado_final["motivo"] = str(e)

    # Garante que algo é sempre colocado na fila.
    queue.put(resultado_final)


def adicionar_camadas_ao_mapa(resultado: dict, mapa: folium.Map):
    """Adiciona as camadas de visualização para uma única linha ao mapa principal."""
    if not resultado.get("sucesso"):
        return

    linha = resultado["linha"]
    gdf_esqueleto_clusterizado = resultado["esqueleto_clusterizado"]
    gdf_esqueleto_limpo = resultado["esqueleto_limpo"]
    stats_candidatos = resultado["candidatos_finais"]
    terminal_a = resultado["terminal_a"]
    terminal_b = resultado["terminal_b"]

    # Camada de Depuração
    fg_debug = folium.FeatureGroup(
        name=f"Debug: Clusters Esqueleto - {linha}", show=False
    )
    unique_labels = np.unique(gdf_esqueleto_clusterizado["cluster"])
    cmap = plt.colormaps.get_cmap("viridis")
    colors = [to_hex(c) for c in cmap(np.linspace(0, 1, len(unique_labels)))]
    cluster_colors = {label: colors[i] for i, label in enumerate(unique_labels)}
    for _, ponto in gdf_esqueleto_clusterizado.iterrows():
        geom = ponto["geom"]
        cor = cluster_colors.get(ponto["cluster"], "#000000")
        folium.CircleMarker(
            location=[geom.y, geom.x],
            radius=3,
            color=cor,
            fill=True,
            fill_color=cor,
            tooltip=f"Cluster: {ponto['cluster']}",
        ).add_to(fg_debug)
    fg_debug.add_to(mapa)

    # Camada de Resultados
    fg_visualizacao = folium.FeatureGroup(name=f"Resultados - {linha}", show=True)
    for _, ponto in gdf_esqueleto_limpo.iterrows():
        geom = ponto["geom"]
        folium.CircleMarker(
            location=[geom.y, geom.x],
            radius=2,
            color="blue",
            fill=True,
            tooltip="Célula do Corredor",
        ).add_to(fg_visualizacao)
    for _, candidato in stats_candidatos.iterrows():
        is_a, is_b = (candidato["id_candidato"] == terminal_a["id_candidato"]), (
            candidato["id_candidato"] == terminal_b["id_candidato"]
        )
        cor, icone = (
            ("green", "star")
            if is_a
            else (("purple", "star") if is_b else ("red", "flag"))
        )
        folium.Marker(
            location=[candidato["geom"].y, candidato["geom"].x],
            popup=f"Candidato {candidato['id_candidato']}<br>Freq: {candidato['frequencia']}",
            icon=folium.Icon(color=cor, icon=icone),
        ).add_to(fg_visualizacao)
    fg_visualizacao.add_to(mapa)


if __name__ == "__main__":
    logging.info(
        f"--- INICIANDO PROCESSAMENTO PARALELO PARA {len(LINHAS_PARA_PROCESSAR)} LINHAS ---"
    )

    engine = create_engine(db_url)
    criar_tabela_terminais_selecionados()

    fila_de_resultados = Queue()
    resultados_finais = []

    # Lógica para processar em lotes e evitar sobrecarga do sistema.
    for i in range(0, len(LINHAS_PARA_PROCESSAR), MAX_PROCESSOS_SIMULTANEOS):
        lote_linhas = LINHAS_PARA_PROCESSAR[i : i + MAX_PROCESSOS_SIMULTANEOS]
        processos_lote = []

        logging.info(
            f"--- Iniciando lote de processamento para as linhas: {lote_linhas} ---"
        )

        for linha in lote_linhas:
            processo = Process(
                target=tarefa_de_processamento,
                args=(linha, fila_de_resultados),
                name=f"Processo-{linha}",
            )
            processos_lote.append(processo)
            processo.start()

        # Recolhe os resultados para o lote atual
        for _ in lote_linhas:
            resultado = fila_de_resultados.get()
            resultados_finais.append(resultado)

        # Garante que todos os processos do lote terminaram antes de continuar
        for processo in processos_lote:
            processo.join()

        logging.info(f"--- Lote de processamento concluído ---")

    logging.info("--- PROCESSAMENTO DE DADOS CONCLUÍDO. GERANDO VISUALIZAÇÃO ---")

    mapa_geral = folium.Map(
        location=[-22.9068, -43.1729], zoom_start=11, tiles="cartodbpositron"
    )
    for resultado in sorted(resultados_finais, key=lambda r: r.get("linha", "")):
        adicionar_camadas_ao_mapa(resultado, mapa_geral)

    folium.LayerControl().add_to(mapa_geral)
    mapa_geral.save(ARQUIVO_SAIDA_MAPA)

    logging.info(
        f"--- VISUALIZAÇÃO FINALIZADA. MAPA SALVO EM: {ARQUIVO_SAIDA_MAPA} ---"
    )
