import asyncio
import re
import os
from pathlib import Path

import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

# ============================================================
# Configuración
# ============================================================
data_file = "cu-lan-ho-live.log"
parquet_file = "data.parquet"

# Colas y variables globales
queue = asyncio.Queue()

# DataFrame global que guarda la evolución de los datos
agg_df = pl.DataFrame(schema={"time": pl.Utf8, "ue": pl.Utf8, "volume": pl.Int64})

# Diccionario para mapear cada UE (del plano de usuario) a su información de la Fase 2
up_ue_to_info = {}

# ============================================================
# Tarea 1: Productor asíncrono (Lectura y Parseo del Log)
# ============================================================
async def tail_file_producer(path: str, data_queue: asyncio.Queue):
    global up_ue_to_info
    
    file = Path(path)
    print(f"Esperando a que se cree el archivo: {data_file}")
    
    while not file.exists():
        await asyncio.sleep(0.5)
        
    with open(path, "r", encoding="utf-8") as f:
        print(f"Abierto el archivo: {data_file}")
        
        # Si queremos empezar a leer desde el principio del live-log, NO hacemos seek(0,2)
        # ya que el simulador escribe desde cero. Depende de si lo ejecutamos antes o después.
        # Aquí lo más seguro es leer desde el principio del archivo actual.
        
        current_sec = None
        buffer_rows = []
        
        # Expresiones regulares (Fase 1 y 2)
        re_uemng = re.compile(r'\[CU-UEMNG\] \[I\] ue=(\d+).*?plmn=(\d+).*?pci=(\d+).*?rnti=(\w+)')
        re_cpe1 = re.compile(r'\[CU-CP-E1\] \[I\] ue=(\d+) cu_cp_ue=(\d+)')
        re_upe1 = re.compile(r'\[CU-UP-E1\] \[I\] Rx PDU cu_cp_ue=(\d+)')
        re_up = re.compile(r'\[CU-UP\s*\] \[I\] ue=(\d+): UE created')
        re_sdap = re.compile(r'\[SDAP\s*\] \[D\] ue=(\d+).*?DL: TX PDU.*?pdu_len=(\d+)')
        
        # Estructuras temporales para correlar identificadores de la Fase 2
        cp_ue_info = {}
        cu_cp_ue_to_info = {}
        last_cu_cp_ue = None

        while True:
            pos = f.tell()
            line = f.readline()
            
            # Asegurarse de tener una línea completa
            if not line or not line.endswith('\n'):
                f.seek(pos)
                await asyncio.sleep(0.1)
                continue
                
            # Fase 2: Identificadores UE (CP -> UP)
            m_uemng = re_uemng.search(line)
            if m_uemng:
                cp_ue, plmn, pci, rnti = m_uemng.groups()
                cp_ue_info[cp_ue] = f"PLMN:{plmn} PCI:{pci} RNTI:{rnti}"
                continue
                
            m_cpe1 = re_cpe1.search(line)
            if m_cpe1:
                cp_ue, cu_cp_ue = m_cpe1.groups()
                if cp_ue in cp_ue_info:
                    cu_cp_ue_to_info[cu_cp_ue] = cp_ue_info[cp_ue]
                continue
                
            m_upe1 = re_upe1.search(line)
            if m_upe1:
                last_cu_cp_ue = m_upe1.group(1)
                continue
                
            m_up = re_up.search(line)
            if m_up:
                up_ue = m_up.group(1)
                if last_cu_cp_ue in cu_cp_ue_to_info:
                    up_ue_to_info[up_ue] = cu_cp_ue_to_info[last_cu_cp_ue]
                else:
                    up_ue_to_info[up_ue] = "Info no disponible"
                continue
                
            # Fase 1: Extracción de volúmenes SDAP
            m_sdap = re_sdap.search(line)
            if m_sdap:
                up_ue, pdu_len = m_sdap.groups()
                
                # Extraemos la fecha-hora (hasta el segundo, p.ej. 2026-01-19T08:57:06)
                if len(line) >= 19:
                    log_sec = line[:19]
                    
                    if current_sec is None:
                        current_sec = log_sec
                        
                    # Si cambiamos de segundo, agregamos y enviamos
                    if log_sec != current_sec:
                        if buffer_rows:
                            # 3. Guardar en un DataFrame en tiempo real
                            df_realtime = pl.DataFrame(buffer_rows, schema=["time", "ue", "volume"], orient="row")
                            
                            # 4. Agregar cada 1 segundo
                            df_agg = df_realtime.group_by("ue").agg(pl.col("volume").sum())
                            
                            # Añadimos la hora de la agregación y reordenamos columnas
                            df_agg = df_agg.with_columns(pl.lit(current_sec).alias("time"))
                            df_agg = df_agg.select(["time", "ue", "volume"])
                            await data_queue.put(df_agg)
                            
                        buffer_rows = []
                        current_sec = log_sec
                        
                buffer_rows.append((log_sec, up_ue, int(pdu_len)))

# ============================================================
# Tarea 2: Consumidor asíncrono
# ============================================================
async def consumer(data_queue: asyncio.Queue):
    global agg_df
    
    while True:
        df_agg = await data_queue.get()
        
        # Concatenar los nuevos datos agregados al DataFrame histórico
        agg_df = pl.concat([agg_df, df_agg], how="vertical")
        print(f"▶ Nuevos datos consumidos y agregados:\n{df_agg}\n")
        
        data_queue.task_done()

# ============================================================
# Tarea 3: Guardado periódico a Parquet
# ============================================================
async def parquet_saver():
    global agg_df
    while True:
        await asyncio.sleep(30)
        if len(agg_df) > 0:
            agg_df.write_parquet(parquet_file)
            print(f"[*] Guardado periódico completado: {parquet_file} ({len(agg_df)} filas)")

# ============================================================
# Tarea 4: Interfaz Gráfica con Dash
# ============================================================
app = Dash(__name__)

app.layout = html.Div([
    html.H2("Evolución de Volúmenes de Datos (SDAP) por UE"),
    dcc.Graph(id="live-graph"),
    dcc.Interval(id="interval", interval=1000, n_intervals=0),
])

@app.callback(
    Output("live-graph", "figure"),
    Input("interval", "n_intervals")
)
def update_graph(n):
    global agg_df, up_ue_to_info
    
    fig = go.Figure()
    
    if len(agg_df) > 0:
        # Obtenemos los UE únicos que han aparecido
        unique_ues = agg_df["ue"].unique().to_list()
        
        for ue in unique_ues:
            df_ue = agg_df.filter(pl.col("ue") == ue)
            
            # Añadir info de la Fase 2 a la leyenda
            info = up_ue_to_info.get(ue, "")
            label = f"UE {ue} ({info})" if info else f"UE {ue}"
            
            fig.add_trace(
                go.Scatter(
                    x=df_ue["time"].to_list(),
                    y=df_ue["volume"].to_list(),
                    mode="lines+markers",
                    name=label
                )
            )
            
    fig.update_layout(
        xaxis_title="Tiempo (Segundos)",
        yaxis_title="Volumen Agregado (Bytes)",
        title="Datos transferidos en DL",
        margin=dict(l=40, r=40, t=40, b=40)
    )
    return fig

# ============================================================
# Main Loop
# ============================================================
async def main():
    print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
    print("Práctica LPIT - Log Parser + Dash")
    print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■\n")

    # Iniciar las corrutinas
    asyncio.create_task(tail_file_producer(data_file, queue))
    asyncio.create_task(consumer(queue))
    asyncio.create_task(parquet_saver())

    # Ejecutar Dash en un hilo para no bloquear el bucle de eventos de asyncio
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=8050,
        debug=False,
    )

if __name__ == '__main__':
    asyncio.run(main())
