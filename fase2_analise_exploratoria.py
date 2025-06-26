import pandas as pd
import numpy as np
import folium
from folium.plugins import HeatMap
import matplotlib.pyplot as plt
import seaborn as sns
from haversine import haversine, Unit
import os
from datetime import datetime
from sqlalchemy import create_engine, text, URL
from dotenv import load_dotenv
import psycopg2

# ==============================================================================
# FASE 1: CONEXÃO COM O BANCO DE DADOS E EXTRAÇÃO
# ==============================================================================


def criar_engine_conexao():
    print("\n--- Fase 1: Conectando ao Banco de Dados ---")
    try:
        load_dotenv()
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        if not all([db_user, db_password, db_host, db_port, db_name]):
            print("Erro: Variáveis de ambiente do banco de dados não definidas.")
            return None

        url_object = URL.create(
            "postgresql+psycopg2",
            username=db_user,
            password=db_password,
            host=db_host,
            database=db_name,
        )

        engine = create_engine(url_object, client_encoding="latin1")
        engine.connect()
        print("Conexão com o banco de dados estabelecida com sucesso!")
        return engine
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def obter_dados_do_banco(engine, nome_tabela, linha_desejada, dia_desejado):
    print(
        f"Buscando dados para a linha {linha_desejada} no dia {dia_desejado.strftime('%Y-%m-%d')}..."
    )
    query = text(
        f"""
        SELECT linha, ordem, data_hora_servidor, latitude, longitude, velocidade
        FROM {nome_tabela}
        WHERE linha = :linha AND DATE(data_hora_servidor) = :dia
    """
    )
    try:
        df = pd.read_sql_query(
            query, engine, params={"linha": linha_desejada, "dia": dia_desejado}
        )
        df["data_hora_servidor"] = pd.to_datetime(df["data_hora_servidor"])
        print(f"Dados carregados. Total de registros: {len(df)}")
        return df
    except Exception as e:
        print(f"Erro ao executar a consulta SQL: {e}")
        return pd.DataFrame()


# ==============================================================================
# FASE 2: ENGENHARIA DE ATRIBUTOS PARA EDA
# ==============================================================================
def calcular_metricas_eda(df):
    print("\n--- Fase 2: Calculando Métricas para EDA ---")
    df = df.sort_values(by=["ordem", "data_hora_servidor"]).reset_index(drop=True)
    df["time_delta_seconds"] = (
        df.groupby("ordem")["data_hora_servidor"].diff().dt.total_seconds()
    )
    df["prev_latitude"] = df.groupby("ordem")["latitude"].shift(1)
    df["prev_longitude"] = df.groupby("ordem")["longitude"].shift(1)
    valid_coords = df.dropna(
        subset=["latitude", "longitude", "prev_latitude", "prev_longitude"]
    )
    df["distance_km"] = valid_coords.apply(
        lambda row: haversine(
            (row["latitude"], row["longitude"]),
            (row["prev_latitude"], row["prev_longitude"]),
            unit=Unit.KILOMETERS,
        ),
        axis=1,
    )
    df["speed_kmh"] = df["distance_km"] / (df["time_delta_seconds"] / 3600)
    df["speed_kmh"].replace([np.inf, -np.inf], np.nan, inplace=True)
    df["latency_seconds"] = (
        df["data_hora_servidor"] - df["data_hora_servidor"]
    ).dt.total_seconds()
    print("Métricas calculadas com sucesso.")
    return df.drop(columns=["prev_latitude", "prev_longitude"])


# ==============================================================================
# FASE 3: ANÁLISE EXPLORATÓRIA APROFUNDADA
# ==============================================================================
def analise_exploratoria_completa(df, linha_desejada):
    print("\n--- Fase 3: Análise Exploratória de Dados Completa ---")
    if df.empty:
        print("DataFrame vazio. Nenhuma análise será realizada.")
        return

    # ----------------------------------------------------
    # 3.1 Visualização de Trajetórias com Heatmap de Lentidão
    # ----------------------------------------------------
    print("\n3.1 Gerando mapa de trajetórias com Heatmap de lentidão...")
    map_center = [df["latitude"].mean(), df["longitude"].mean()]
    bus_map = folium.Map(location=map_center, zoom_start=12, tiles="cartodbpositron")

    # Adiciona camada de Heatmap para pontos de baixa velocidade
    df_lento = df[df["speed_kmh"] < 5]
    locations_lentos = df_lento[["latitude", "longitude"]].dropna().values.tolist()
    HeatMap(locations_lentos, radius=12, blur=15).add_to(
        folium.FeatureGroup(name="Pontos Quentes de Lentidão").add_to(bus_map)
    )

    # Adiciona as trajetórias individuais
    vehicles = df["ordem"].unique()
    colors = ["blue", "red", "green", "purple", "orange", "darkred"]
    trajectories_fg = folium.FeatureGroup(name="Trajetórias dos Veículos").add_to(
        bus_map
    )
    for i, vehicle in enumerate(vehicles):
        vehicle_df = df[df["ordem"] == vehicle].sort_values("data_hora_servidor")
        locations = vehicle_df[["latitude", "longitude"]].dropna().values.tolist()
        if locations:
            folium.PolyLine(
                locations,
                color=colors[i % len(colors)],
                weight=2.5,
                opacity=0.8,
                tooltip=f"Veículo: {vehicle}",
            ).add_to(trajectories_fg)

    folium.LayerControl().add_to(bus_map)  # Adiciona controle de camadas
    map_filename = f"trajetoria_linha_{linha_desejada}.html"
    bus_map.save(map_filename)
    print(
        f"--> Mapa interativo salvo em '{map_filename}'. Abra este arquivo no navegador."
    )

    # ----------------------------------------------------
    # 3.2 Análise Estatística Básica
    # ----------------------------------------------------
    print("\n3.2 Gerando gráficos de análise estatística básica...")
    fig, axes = plt.subplots(3, 2, figsize=(15, 18))
    fig.suptitle(f"Análise Estatística Básica da Linha {linha_desejada}", fontsize=16)
    # (Código dos gráficos de velocidade, tempo e latência permanece o mesmo)
    speed_data_filtered = df["speed_kmh"].dropna()
    speed_data_filtered = speed_data_filtered[speed_data_filtered <= 120]
    sns.histplot(speed_data_filtered, bins=30, kde=True, ax=axes[0, 0]).set_title(
        "Distribuição de Velocidade (km/h)"
    )
    sns.boxplot(x=speed_data_filtered, ax=axes[0, 1]).set_title(
        "Boxplot de Velocidade (km/h)"
    )
    sns.histplot(
        df["time_delta_seconds"].dropna(), bins=30, kde=True, ax=axes[1, 0]
    ).set_title("Tempo Entre Registros (s)")
    sns.boxplot(x=df["time_delta_seconds"].dropna(), ax=axes[1, 1]).set_title(
        "Boxplot do Tempo Entre Registros (s)"
    )
    sns.histplot(
        df["latency_seconds"].dropna(), bins=30, kde=True, ax=axes[2, 0]
    ).set_title("Latência GPS vs. Servidor (s)")
    sns.boxplot(x=df["latency_seconds"].dropna(), ax=axes[2, 1]).set_title(
        "Boxplot da Latência (s)"
    )
    plt.tight_layout(rect=(0.0, 0.03, 1.0, 0.95))
    plots_filename = f"analise_estatistica_basica_{linha_desejada}.png"
    plt.savefig(plots_filename)
    plt.close()  # Fecha a figura para liberar memória
    print(f"--> Gráficos básicos salvos em '{plots_filename}'")

    # ----------------------------------------------------
    # 3.3 Análise Temporal por Hora do Dia
    # ----------------------------------------------------
    print("\n3.3 Gerando análise de performance por hora do dia...")
    df["hora"] = df["data_hora_servidor"].dt.hour
    velocidade_por_hora = df.groupby("hora")["speed_kmh"].mean().reset_index()

    plt.figure(figsize=(12, 6))
    sns.lineplot(
        x="hora", y="speed_kmh", data=velocidade_por_hora, marker="o", color="b"
    )
    plt.title(f"Velocidade Média por Hora do Dia - Linha {linha_desejada}")
    plt.xlabel("Hora do Dia")
    plt.ylabel("Velocidade Média (km/h)")
    plt.grid(True)
    plt.xticks(range(0, 24))
    plots_filename_hora = f"analise_temporal_{linha_desejada}.png"
    plt.savefig(plots_filename_hora)
    plt.close()
    print(f"--> Gráfico de análise temporal salvo em '{plots_filename_hora}'")

    # ----------------------------------------------------
    # 3.4 Análise de Performance por Veículo
    # ----------------------------------------------------
    print("\n3.4 Gerando análise de performance por veículo...")
    plt.figure(figsize=(15, 8))
    sns.boxplot(
        x="ordem", y=speed_data_filtered, data=df.loc[speed_data_filtered.index]
    )
    plt.title(
        f"Comparação da Distribuição de Velocidade por Veículo - Linha {linha_desejada}"
    )
    plt.xlabel("ID do Veículo")
    plt.ylabel("Velocidade (km/h)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plots_filename_veiculo = f"analise_por_veiculo_{linha_desejada}.png"
    plt.savefig(plots_filename_veiculo)
    plt.close()
    print(f"--> Gráfico de análise por veículo salvo em '{plots_filename_veiculo}'")


if __name__ == "__main__":
    NOME_TABELA_DADOS = "cleaned_gps_pings"
    LINHA_ALVO = "853"

    # Dia da análise
    DIA_ALVO = datetime(2024, 4, 28).date()

    # --- Execução do Pipeline ---
    db_engine = criar_engine_conexao()

    if db_engine:
        df_linha_dia = obter_dados_do_banco(
            db_engine, NOME_TABELA_DADOS, LINHA_ALVO, DIA_ALVO
        )

        if not df_linha_dia.empty:
            df_com_metricas = calcular_metricas_eda(df_linha_dia)
            analise_exploratoria_completa(df_com_metricas, LINHA_ALVO)
            print("\nAnálise Exploratória concluída com sucesso!")
        else:
            print(
                f"\nNenhum dado encontrado para a linha {LINHA_ALVO} no dia {DIA_ALVO.strftime('%Y-%m-%d')}."
            )
