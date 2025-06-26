# -*- coding: utf-8 -*-
"""
Script para a Fase 1: Construção de Rotas Canônicas (Versão Final e Otimizada).

Estratégia: Filtro Espacial Primeiro, Depois Análise Comportamental
1.  **Identificação do Esqueleto da Grelha:** Primeiro, o script identifica todas as
    células da grelha que uma linha visita, criando um "esqueleto" completo.
2.  **Remoção de Outliers por Conectividade:** Um DBSCAN é executado na grelha
    completa para encontrar o maior componente conectado (o "corredor principal")
    e remover as "ilhas" de células, como as garagens.
3.  **Criação de um Geofence Robusto:** Uma linha é traçada através do corredor
    principal e um buffer é aplicado para criar um polígono contínuo (geofence),
    representando a área de operação válida da linha.
4.  **Identificação de Viagens Dentro do Corredor:** Uma query SQL com funções de janela
    segmenta os pings de cada ônibus em viagens. Apenas as viagens que ocorrem
    dentro do geofence do corredor são consideradas.
5.  **Construção da Rota Final:** A rota canônica para cada sentido é construída
    usando apenas os pontos dos segmentos de viagem validados, garantindo alta precisão.
"""
import os
import logging
from datetime import date, timedelta
import time

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import LineString, Point, Polygon
from scipy.spatial.distance import pdist, squareform
from scipy.spatial import KDTree
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import folium
import networkx as nx

from sklearn.cluster import DBSCAN
from multiprocessing import Process, Queue

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
TABELA_TERMINAIS_VALIDADOS = "terminais_selecionados_validacao_v2"
TAMANHO_GRADE_GRAUS = 0.001  # Define o tamanho de cada célula da grelha em graus (aprox. 111 metros no equador).
DENSIDADE_MINIMA_PONTOS_ESQUELETO = 15  # Uma célula deve ter pelo menos 15 pings para ser considerada parte do esqueleto.
MAX_INTERVALO_VIAGEM_MINUTOS = 30  # Se um ônibus fica mais de 30 min sem enviar pings, considera-se o início de uma nova viagem.

# --- Parâmetros da Partição de Tempo ---
DATA_FIM_ANALISE = date(2024, 5, 12)
DATA_INICIO_ANALISE = DATA_FIM_ANALISE - timedelta(
    days=13
)  # Período de análise de aproximadamente duas semanas.

# --- Parâmetros Técnicos ---
CRS_GEOGRAFICO = (
    "EPSG:4326"  # Sistema de Coordenadas Geográficas (WGS84), padrão para GPS.
)
CRS_METRICO = "EPSG:32723"  # Sistema de Coordenadas Métrico (UTM Zona 23S), para cálculos de distância precisos no Rio de Janeiro.
RAIO_CONEXAO_GRAFO_METROS = (
    350  # Distância máxima em metros para conectar dois pontos do esqueleto no grafo.
)
JANELA_SUAVIZACAO = 5  # Número de pontos na janela da média móvel para suavizar a rota.
TOLERANCIA_SIMPLIFICACAO_GRAUS = 0.0001  # Tolerância para simplificação da rota (aprox. 11 metros). Remove vértices desnecessários.
DBSCAN_EPS_GRELHA = (
    TAMANHO_GRADE_GRAUS * 3.0
)  # Raio de busca do DBSCAN para agrupar células da grelha.
DBSCAN_MIN_SAMPLES_GRELHA = (
    3  # Número mínimo de células vizinhas para formar um cluster no corredor.
)
BUFFER_CORREDOR_METROS = (
    75  # Margem de segurança em metros para criar o geofence do corredor.
)

# --- Parâmetros de Execução ---
LINHAS_PARA_PROCESSAR = [
    # "100",
    "108",
    "232",
    "2336",
    "2803",
    "292",
    "298",
    "3",
    "309",
    # "315",
    "324",
    "328",
    "343",
    "355",
    # "371",
    "388",
    "397",
    "399",
    "415",
    "422",
    "457",
    # "483",
    "497",
    "550",
    "553",
    "554",
    "557",
    "565",
    "606",
    "624",
    "629",
    "634",
    "638",
    "639",
    "665",
    "756",
    "759",
    "774",
    "779",
    "803",
    "838",
    # "852", # linha circular
    # "864",
    "867",
    "878",
    "905",
    "917",
    "918",
]
ARQUIVO_SAIDA_MAPA = "mapa_rotas_canonicas_final.html"
MAX_WORKERS = 4  # Número máximo de processos a serem executados em paralelo.
MAX_PONTOS_VISUALIZACAO = 1000  # FIX: Limite de pontos para desenhar em camadas de debug para evitar arquivos HTML gigantes.

# --- Configuração Inicial ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - PID:%(process)d - %(levelname)s - [%(funcName)s] - %(message)s",
    # Salva os logs em um arquivo e também os exibe no console. 'w' sobrescreve o arquivo de log a cada execução.
    handlers=[
        logging.FileHandler("log_geracao_rotas_final.log", mode="w"),
        logging.StreamHandler(),
    ],
)
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
main_engine = create_engine(db_url)


def criar_tabela_rotas_canonicas(engine: Engine):
    """
    Cria a tabela 'rotas_canonicas' no banco de dados se ela não existir.
    Esta tabela armazena o resultado final do processamento.
    """
    logging.info("Verificando/Criando tabela 'rotas_canonicas'...")
    with engine.connect() as connection:
        connection.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS rotas_canonicas (
                id SERIAL PRIMARY KEY,
                linha VARCHAR(20) NOT NULL,
                sentido VARCHAR(10) NOT NULL,
                trajeto GEOMETRY(LineString, 4326) NOT NULL,
                UNIQUE(linha, sentido)
            );
        """
            )
        )
        connection.commit()


def obter_esqueleto_da_grelha(linha: str, engine: Engine) -> gpd.GeoDataFrame | None:
    """
    Etapa 1: Identifica todas as células densas da grelha que a linha visita.
    Agrupa os pings de GPS em uma grelha e calcula o centroide de cada célula que
    tenha um número mínimo de pontos, formando um "esqueleto" da rota.
    """
    logging.info(f"[{linha}] Obtendo esqueleto da grelha...")
    query = text(
        """
        SELECT 
            ST_Centroid(ST_Collect(geom)) as geometry
        FROM (
            SELECT 
                geom,
                TRUNC(ST_X(geom) / :tamanho_grade) as grid_x,
                TRUNC(ST_Y(geom) / :tamanho_grade) as grid_y
            FROM cleaned_gps_pings
            WHERE linha = :linha AND data_hora_servidor BETWEEN :data_inicio AND :data_fim
        ) as sub
        GROUP BY grid_x, grid_y
        HAVING COUNT(*) > :densidade_minima;
    """
    )
    gdf = gpd.read_postgis(
        query,
        engine,
        params={
            "linha": linha,
            "data_inicio": DATA_INICIO_ANALISE,
            "data_fim": DATA_FIM_ANALISE,
            "tamanho_grade": TAMANHO_GRADE_GRAUS,
            "densidade_minima": DENSIDADE_MINIMA_PONTOS_ESQUELETO,
        },
        geom_col="geometry",
        crs=CRS_GEOGRAFICO,
    )
    if gdf.empty:
        logging.warning(f"[{linha}] Nenhum esqueleto de rota encontrado.")
        return None
    return gdf


def identificar_corredor_principal(
    gdf_esqueleto: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame | None]:
    """
    Etapa 2: Clusteriza a grelha com DBSCAN para encontrar o 'corredor principal'.
    Retorna o GeoDataFrame original com os labels de cluster e o GeoDataFrame filtrado
    apenas com o corredor principal.
    """
    linha_nome = (
        gdf_esqueleto.name if hasattr(gdf_esqueleto, "name") else "desconhecida"
    )
    logging.info(
        f"[{linha_nome}] Identificando corredor principal de {len(gdf_esqueleto)} pontos..."
    )
    coords = np.array([p.coords[0] for p in gdf_esqueleto.geometry])
    db = DBSCAN(
        eps=DBSCAN_EPS_GRELHA, min_samples=DBSCAN_MIN_SAMPLES_GRELHA, n_jobs=-1
    ).fit(coords)
    gdf_esqueleto["cluster"] = db.labels_

    valid_clusters = gdf_esqueleto[gdf_esqueleto["cluster"] != -1]
    if valid_clusters.empty:
        logging.warning(
            f"[{linha_nome}] Nenhum corredor principal pôde ser identificado."
        )
        return gdf_esqueleto, None

    maiores_clusters_id = set(
        valid_clusters["cluster"].value_counts().nlargest(2).index
    )
    gdf_corredor = gdf_esqueleto[
        gdf_esqueleto["cluster"].isin(maiores_clusters_id)
    ].copy()
    logging.info(
        f"[{linha_nome}] Corredor principal identificado com {len(gdf_corredor)} células."
    )
    return gdf_esqueleto, gdf_corredor


def identificar_e_classificar_viagens(
    linha: str, terminais: dict, corredor_polygon: Polygon, engine: Engine
) -> gpd.GeoDataFrame | None:
    """
    Etapa 3: Identifica viagens, filtra as que estão no corredor e classifica o sentido de forma robusta.
    Esta é uma das funções mais importantes do pipeline.
    """
    logging.info(f"[{linha}] Identificando, filtrando e classificando viagens...")

    query = text(
        """
        WITH pings_com_diff AS (
            SELECT
                ordem, data_hora_servidor, geom,
                EXTRACT(EPOCH FROM (data_hora_servidor - LAG(data_hora_servidor, 1, data_hora_servidor) OVER (PARTITION BY ordem ORDER BY data_hora_servidor))) as diff_segundos
            FROM cleaned_gps_pings
            WHERE linha = :linha AND data_hora_servidor BETWEEN :data_inicio AND :data_fim
        ), pings_com_viagem_id AS (
            SELECT *, SUM(CASE WHEN diff_segundos > :max_intervalo THEN 1 ELSE 0 END) OVER (PARTITION BY ordem ORDER BY data_hora_servidor) as bloco_viagem
            FROM pings_com_diff
        )
        SELECT ordem, data_hora_servidor, geom, (ordem || '_' || bloco_viagem::text) as id_viagem
        FROM pings_com_viagem_id;
    """
    )
    gdf_pings = gpd.read_postgis(
        query,
        engine,
        params={
            "linha": linha,
            "data_inicio": DATA_INICIO_ANALISE,
            "data_fim": DATA_FIM_ANALISE,
            "max_intervalo": MAX_INTERVALO_VIAGEM_MINUTOS * 60,
        },
        geom_col="geom",
        crs=CRS_GEOGRAFICO,
    )
    if gdf_pings.empty:
        return None
    logging.info(
        f"[{linha}] Encontrados {gdf_pings['id_viagem'].nunique()} viagens brutas com {len(gdf_pings)} pings."
    )

    gdf_pings.rename_geometry("geometry", inplace=True)
    gdf_corredor_area = gpd.GeoDataFrame(
        [{"geometry": corredor_polygon}], crs=CRS_GEOGRAFICO
    )
    gdf_viagens_no_corredor = gpd.sjoin(
        gdf_pings, gdf_corredor_area, how="inner", predicate="within"
    )

    if gdf_viagens_no_corredor.empty:
        logging.warning(
            f"[{linha}] Nenhuma viagem encontrada dentro da área do corredor."
        )
        return None
    logging.info(
        f"[{linha}] {gdf_viagens_no_corredor['id_viagem'].nunique()} viagens com pings dentro do corredor ({len(gdf_viagens_no_corredor)} pings)."
    )

    viagens_classificadas = []
    gdf_viagens_metric = gdf_viagens_no_corredor.to_crs(CRS_METRICO)
    terminal_a_metric = (
        gpd.GeoSeries([terminais["A"]], crs=CRS_GEOGRAFICO).to_crs(CRS_METRICO).iloc[0]
    )
    terminal_b_metric = (
        gpd.GeoSeries([terminais["B"]], crs=CRS_GEOGRAFICO).to_crs(CRS_METRICO).iloc[0]
    )

    for id_viagem, grupo in gdf_viagens_metric.groupby("id_viagem"):
        if len(grupo) < 5:
            continue

        grupo = grupo.sort_values("data_hora_servidor").copy()
        grupo["dist_a"] = grupo.geometry.distance(terminal_a_metric)
        grupo["dist_b"] = grupo.geometry.distance(terminal_b_metric)

        idx_ponto_perto_a = grupo["dist_a"].idxmin()
        idx_ponto_perto_b = grupo["dist_b"].idxmin()

        if idx_ponto_perto_a == idx_ponto_perto_b:
            continue

        tempo_ponto_perto_a = grupo.loc[idx_ponto_perto_a, "data_hora_servidor"]
        tempo_ponto_perto_b = grupo.loc[idx_ponto_perto_b, "data_hora_servidor"]

        sentido = "desconhecido"
        viagem_segmento = None

        if tempo_ponto_perto_a < tempo_ponto_perto_b:
            sentido = "ida"
            viagem_segmento = grupo[
                (grupo["data_hora_servidor"] >= tempo_ponto_perto_a)
                & (grupo["data_hora_servidor"] <= tempo_ponto_perto_b)
            ]
        elif tempo_ponto_perto_b < tempo_ponto_perto_a:
            sentido = "volta"
            viagem_segmento = grupo[
                (grupo["data_hora_servidor"] >= tempo_ponto_perto_b)
                & (grupo["data_hora_servidor"] <= tempo_ponto_perto_a)
            ]

        if sentido != "desconhecido" and not viagem_segmento.empty:
            segmento_final = viagem_segmento.copy()
            segmento_final["sentido"] = sentido
            viagens_classificadas.append(segmento_final)

    if not viagens_classificadas:
        logging.warning(
            f"[{linha}] Nenhuma viagem pôde ser classificada como 'ida' ou 'volta'."
        )
        return None

    gdf_final = pd.concat(viagens_classificadas).to_crs(CRS_GEOGRAFICO)
    logging.info(
        f"[{linha}] {gdf_final['id_viagem'].nunique()} segmentos de viagem classificados com sucesso."
    )
    return gdf_final


def construir_grafo_do_esqueleto(
    gdf_esqueleto: gpd.GeoDataFrame, graph_name: str
) -> nx.Graph:
    """
    Constrói um grafo NetworkX a partir de um conjunto de pontos (esqueleto).
    O grafo é usado para encontrar o caminho mais curto e ordenar os pontos da rota.
    """
    G = nx.Graph()
    gdf_esqueleto_utm = gdf_esqueleto.to_crs(CRS_METRICO)
    for idx, row in gdf_esqueleto_utm.iterrows():
        G.add_node(idx, pos=(row.geometry.x, row.geometry.y))

    tree = KDTree(np.array([p.coords[0] for p in gdf_esqueleto_utm.geometry]))
    pares = tree.query_pairs(r=RAIO_CONEXAO_GRAFO_METROS)

    for i, j in pares:
        no_i_original, no_j_original = (
            gdf_esqueleto_utm.index[i],
            gdf_esqueleto_utm.index[j],
        )
        pos_i, pos_j = G.nodes[no_i_original]["pos"], G.nodes[no_j_original]["pos"]
        distancia = np.hypot(pos_i[0] - pos_j[0], pos_i[1] - pos_j[1])
        G.add_edge(no_i_original, no_j_original, weight=distancia)

    if not nx.is_connected(G):
        logging.warning(f"[{graph_name}] Grafo desconectado, conectando componentes...")
        componentes = sorted(nx.connected_components(G), key=len, reverse=True)
        componente_principal = componentes[0]
        for outro_componente in componentes[1:]:
            tree_principal = KDTree([G.nodes[n]["pos"] for n in componente_principal])
            tree_outro = KDTree([G.nodes[n]["pos"] for n in outro_componente])
            distancias, indices = tree_principal.query(tree_outro.data, k=1)
            idx_outro = np.argmin(distancias)
            idx_principal = indices[idx_outro]
            no_outro = list(outro_componente)[idx_outro]
            no_principal = list(componente_principal)[idx_principal]
            G.add_edge(no_principal, no_outro, weight=distancias.min())
            componente_principal.update(outro_componente)
    return G


def suavizar_e_simplificar_rota(
    pontos: list, janela: int, tolerancia: float
) -> LineString | None:
    """Aplica uma média móvel (suavização) e o algoritmo de Douglas-Peucker (simplificação) na rota."""
    if len(pontos) < 2:
        return None
    if len(pontos) < janela:
        return LineString(pontos)
    df = pd.DataFrame({"x": [p.x for p in pontos], "y": [p.y for p in pontos]})
    df["x_smooth"] = df["x"].rolling(window=janela, center=True, min_periods=1).mean()
    df["y_smooth"] = df["y"].rolling(window=janela, center=True, min_periods=1).mean()
    linha_suavizada = LineString(
        [Point(row.x_smooth, row.y_smooth) for _, row in df.iterrows()]
    )
    linha_simplificada = linha_suavizada.simplify(tolerancia, preserve_topology=True)
    return (
        linha_simplificada
        if isinstance(linha_simplificada, LineString)
        and not linha_simplificada.is_empty
        else None
    )


def gerar_rota_para_linha(linha: str, db_connection_url: str, queue: Queue):
    """
    Função 'trabalhadora' que executa todo o pipeline para uma única linha.
    Esta função é o alvo de cada processo no pool de paralelização.
    """
    resultado_final = {
        "linha": linha,
        "sucesso": False,
        "motivo": "Início do processo.",
        "dados_visuais": [],
        "dados_debug": {},
    }
    engine = create_engine(db_connection_url)

    try:
        gdf_esqueleto_bruto = obter_esqueleto_da_grelha(linha, engine)
        if gdf_esqueleto_bruto is None:
            resultado_final["motivo"] = "Esqueleto bruto não pôde ser gerado."
            queue.put(resultado_final)
            return
        gdf_esqueleto_bruto.name = linha
        resultado_final["dados_debug"]["esqueleto_bruto"] = gdf_esqueleto_bruto

        with engine.connect() as conn:
            term_res = conn.execute(
                text(
                    f"SELECT ST_AsText(geom_a) as a, ST_AsText(geom_b) as b FROM {TABELA_TERMINAIS_VALIDADOS} WHERE linha = :l"
                ),
                {"l": linha},
            ).first()
            if not term_res:
                resultado_final["motivo"] = "Terminais não encontrados."
                queue.put(resultado_final)
                return
            terminais = {
                "A": gpd.GeoSeries.from_wkt([term_res.a], crs=CRS_GEOGRAFICO).iloc[0],
                "B": gpd.GeoSeries.from_wkt([term_res.b], crs=CRS_GEOGRAFICO).iloc[0],
            }

        gdf_esqueleto_clusterizado, gdf_corredor = identificar_corredor_principal(
            gdf_esqueleto_bruto
        )
        resultado_final["dados_debug"][
            "esqueleto_clusterizado"
        ] = gdf_esqueleto_clusterizado
        if gdf_corredor is None:
            resultado_final["motivo"] = "Corredor principal não identificado."
            queue.put(resultado_final)
            return

        corredor_utm = gdf_corredor.to_crs(CRS_METRICO)
        if len(corredor_utm) > 1:
            dist_matrix = squareform(
                pdist(np.array([g.coords[0] for g in corredor_utm.geometry]))
            )
            i, j = np.unravel_index(np.argmax(dist_matrix), dist_matrix.shape)
            grafo_corredor = construir_grafo_do_esqueleto(gdf_corredor, linha)
            caminho_corredor = nx.shortest_path(
                grafo_corredor,
                source=corredor_utm.index[i],
                target=corredor_utm.index[j],
                weight="weight",
            )
            linha_corredor_utm = LineString(
                [corredor_utm.loc[p].geometry for p in caminho_corredor]
            )
            corredor_polygon_utm = linha_corredor_utm.buffer(BUFFER_CORREDOR_METROS)
        else:
            corredor_polygon_utm = corredor_utm.unary_union.buffer(
                BUFFER_CORREDOR_METROS
            )
        corredor_polygon_geo = (
            gpd.GeoSeries([corredor_polygon_utm], crs=CRS_METRICO)
            .to_crs(CRS_GEOGRAFICO)
            .iloc[0]
        )
        resultado_final["dados_debug"]["geofence_corredor"] = corredor_polygon_geo

        gdf_viagens = identificar_e_classificar_viagens(
            linha, terminais, corredor_polygon_geo, engine
        )
        if gdf_viagens is None:
            resultado_final["motivo"] = "Nenhuma viagem classificada encontrada."
            queue.put(resultado_final)
            return
        resultado_final["dados_debug"]["viagens_pings"] = gdf_viagens

        rotas_geradas = 0
        for sentido, (inicio_geom, fim_geom) in [
            ("ida", (terminais["A"], terminais["B"])),
            ("volta", (terminais["B"], terminais["A"])),
        ]:
            pontos_do_sentido = gdf_viagens[gdf_viagens["sentido"] == sentido].copy()
            if pontos_do_sentido.empty:
                continue

            pontos_do_sentido["grid_id"] = pontos_do_sentido.geometry.apply(
                lambda p: f"{int(p.x / TAMANHO_GRADE_GRAUS)}_{int(p.y / TAMANHO_GRADE_GRAUS)}"
            )
            esqueleto_final_sentido_df = (
                pontos_do_sentido.groupby("grid_id")["geometry"]
                .apply(lambda g: g.union_all().centroid)
                .reset_index(name="geometry")
            )
            gdf_esqueleto_final = gpd.GeoDataFrame(
                esqueleto_final_sentido_df, geometry="geometry", crs=CRS_GEOGRAFICO
            )

            if len(gdf_esqueleto_final) < 2:
                continue

            grafo_rota = construir_grafo_do_esqueleto(
                gdf_esqueleto_final, f"{linha}-{sentido}"
            )
            tree_esqueleto = KDTree([p.coords[0] for p in gdf_esqueleto_final.geometry])

            _, no_inicio_idx = tree_esqueleto.query([inicio_geom.x, inicio_geom.y])
            _, no_fim_idx = tree_esqueleto.query([fim_geom.x, fim_geom.y])

            try:
                source_node = gdf_esqueleto_final.index[no_inicio_idx]
                target_node = gdf_esqueleto_final.index[no_fim_idx]
                caminho_indices = nx.shortest_path(
                    grafo_rota, source=source_node, target=target_node, weight="weight"
                )
                pontos_ordenados = gdf_esqueleto_final.iloc[
                    caminho_indices
                ].geometry.tolist()
                rota_final = suavizar_e_simplificar_rota(
                    pontos_ordenados, JANELA_SUAVIZACAO, TOLERANCIA_SIMPLIFICACAO_GRAUS
                )
                if not rota_final:
                    continue
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                continue

            with engine.connect() as conn:
                conn.execute(
                    text(
                        "INSERT INTO rotas_canonicas (linha, sentido, trajeto) VALUES (:l, :s, ST_SetSRID(ST_GeomFromText(:wkt), 4326)) ON CONFLICT (linha, sentido) DO UPDATE SET trajeto = EXCLUDED.trajeto;"
                    ),
                    {"l": linha, "s": sentido, "wkt": rota_final.wkt},
                )
                conn.commit()

            resultado_final["dados_visuais"].append(
                (linha, sentido, gdf_esqueleto_final, rota_final)
            )
            rotas_geradas += 1

        if rotas_geradas > 0:
            resultado_final["sucesso"] = True
            resultado_final["motivo"] = f"{rotas_geradas} rota(s) gerada(s)."
        else:
            resultado_final["motivo"] = "Nenhuma rota final pôde ser construída."
        queue.put(resultado_final)

    except Exception as e:
        logging.critical(
            f"[{linha}] Erro fatal no processo trabalhador: {e}", exc_info=True
        )
        resultado_final["motivo"] = str(e)
        queue.put(resultado_final)


def adicionar_camadas_ao_mapa(resultado: dict, mapa: folium.Map):
    """Adiciona as camadas de visualização do RESULTADO FINAL para uma linha ao mapa principal."""
    if not resultado.get("sucesso"):
        return

    for linha, sentido, gdf_esqueleto, rota_final in resultado["dados_visuais"]:
        fg_rota = folium.FeatureGroup(
            name=f"Resultado: Rota {sentido} - Linha {linha}", show=True
        )
        # Amostragem para a camada de resultado para manter o mapa leve
        gdf_esqueleto_plot = gdf_esqueleto
        if len(gdf_esqueleto) > MAX_PONTOS_VISUALIZACAO:
            logging.info(
                f"[{linha}-{sentido}] Amostrando esqueleto final de {len(gdf_esqueleto)} para {MAX_PONTOS_VISUALIZACAO} pontos."
            )
            gdf_esqueleto_plot = gdf_esqueleto.sample(
                MAX_PONTOS_VISUALIZACAO, random_state=42
            )

        for _, ponto in gdf_esqueleto_plot.iterrows():
            folium.CircleMarker(
                location=[ponto["geometry"].y, ponto["geometry"].x],
                radius=2,
                color="blue",
                fill=True,
            ).add_to(fg_rota)
        folium.GeoJson(
            rota_final,
            style_function=lambda x: {
                "color": "red" if sentido == "ida" else "green",
                "weight": 4,
            },
        ).add_to(fg_rota)
        fg_rota.add_to(mapa)


def adicionar_camadas_debug_ao_mapa(resultado: dict, mapa: folium.Map):
    """Adiciona camadas de DEBUG para uma linha ao mapa principal. Estas camadas são ocultas por padrão."""
    linha = resultado["linha"]
    dados_debug = resultado.get("dados_debug", {})

    if not dados_debug:
        return

    # Camada 1: Esqueleto Clusterizado (mostra outliers)
    if "esqueleto_clusterizado" in dados_debug:
        gdf = dados_debug["esqueleto_clusterizado"]
        fg_esqueleto = folium.FeatureGroup(
            name=f"Debug: Esqueleto Clusterizado - Linha {linha}", show=False
        )

        gdf_plot = gdf
        if len(gdf) > MAX_PONTOS_VISUALIZACAO:
            logging.info(
                f"[{linha}] Amostrando esqueleto clusterizado de {len(gdf)} para {MAX_PONTOS_VISUALIZACAO} pontos."
            )
            gdf_plot = gdf.sample(n=MAX_PONTOS_VISUALIZACAO, random_state=42)

        unique_labels = sorted(gdf_plot["cluster"].unique())
        cmap = plt.get_cmap(
            "viridis", len(unique_labels) if len(unique_labels) > 0 else 1
        )
        colors = [to_hex(cmap(i)) for i in range(len(unique_labels))]
        cluster_colors = {label: colors[i] for i, label in enumerate(unique_labels)}

        for _, ponto in gdf_plot.iterrows():
            geom = ponto["geometry"]
            cor = cluster_colors.get(ponto["cluster"], "#000000")
            folium.CircleMarker(
                location=[geom.y, geom.x],
                radius=3,
                color=cor,
                fill=True,
                fill_color=cor,
                tooltip=f"Cluster: {ponto['cluster']}",
            ).add_to(fg_esqueleto)
        fg_esqueleto.add_to(mapa)

    # Camada 2: Geofence do Corredor
    if "geofence_corredor" in dados_debug:
        fg_geofence = folium.FeatureGroup(
            name=f"Debug: Geofence Corredor - Linha {linha}", show=False
        )
        folium.GeoJson(
            dados_debug["geofence_corredor"],
            style_function=lambda x: {
                "color": "orange",
                "fillColor": "orange",
                "fillOpacity": 0.2,
                "weight": 2,
            },
        ).add_to(fg_geofence)
        fg_geofence.add_to(mapa)

    # Camada 3: Pings das Viagens Classificadas
    if "viagens_pings" in dados_debug:
        gdf = dados_debug["viagens_pings"]
        fg_pings = folium.FeatureGroup(
            name=f"Debug: Pings de Viagens - Linha {linha}", show=False
        )

        gdf_plot = gdf
        if len(gdf) > MAX_PONTOS_VISUALIZACAO:
            logging.info(
                f"[{linha}] Amostrando pings de viagens de {len(gdf)} para {MAX_PONTOS_VISUALIZACAO} pontos."
            )
            gdf_plot = gdf.sample(n=MAX_PONTOS_VISUALIZACAO, random_state=42)

        cores_sentido = {"ida": "cyan", "volta": "magenta"}
        for _, ping in gdf_plot.iterrows():
            geom = ping["geometry"]
            folium.CircleMarker(
                location=[geom.y, geom.x],
                radius=1,
                color=cores_sentido.get(ping["sentido"], "gray"),
            ).add_to(fg_pings)
        fg_pings.add_to(mapa)


if __name__ == "__main__":
    start_time = time.time()
    logging.info(
        f"--- INICIANDO FASE 1: CONSTRUÇÃO DE ROTAS CANÔNICAS (ESTRATÉGIA FINAL) ---"
    )

    criar_tabela_rotas_canonicas(main_engine)
    mapa_geral = folium.Map(
        location=[-22.9068, -43.1729], zoom_start=11, tiles="cartodbpositron"
    )

    fila_de_resultados = Queue()
    resultados_finais = []

    for i in range(0, len(LINHAS_PARA_PROCESSAR), MAX_WORKERS):
        lote_linhas = LINHAS_PARA_PROCESSAR[i : i + MAX_WORKERS]
        processos_lote = []
        logging.info(f"--- Iniciando lote de processamento para: {lote_linhas} ---")
        for linha in lote_linhas:
            processo = Process(
                target=gerar_rota_para_linha,
                args=(linha, db_url, fila_de_resultados),
                name=f"Processo-{linha}",
            )
            processos_lote.append(processo)
            processo.start()

        for _ in lote_linhas:
            resultados_finais.append(fila_de_resultados.get())

        for processo in processos_lote:
            processo.join()
        logging.info(f"--- Lote de processamento concluído ---")

    logging.info("--- Processamento paralelo concluído. Gerando mapa... ---")
    for resultado in sorted(resultados_finais, key=lambda r: r.get("linha", "")):
        adicionar_camadas_ao_mapa(resultado, mapa_geral)
        adicionar_camadas_debug_ao_mapa(resultado, mapa_geral)

    folium.LayerControl().add_to(mapa_geral)
    mapa_geral.save(ARQUIVO_SAIDA_MAPA)

    end_time = time.time()
    logging.info(
        f"--- PROCESSAMENTO TOTAL FINALIZADO EM {end_time - start_time:.2f} SEGUNDOS. ---"
    )
    logging.info(f"--- MAPA SALVO EM: {ARQUIVO_SAIDA_MAPA} ---")
