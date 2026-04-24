import asyncio
import time
import re
from pathlib import Path
from datetime import datetime

import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

version = "1.0-MasterTeleco"
data_file = "cu-lan-ho.log"  
parquet_file = "historico_ue_volumen.parquet"


queue = asyncio.Queue()


agg_df = pl.DataFrame(
    schema={
        "timestamp": pl.Datetime, 
        "UE_ID": pl.Utf8, 
        "DL_Bytes": pl.Float64,
        "PLMN": pl.Utf8,    
        "PCI": pl.Utf8,     
        "RNTI": pl.Utf8    
    }
)


regex_sdap = re.compile(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+).*?\[SDAP\s*\].*?ue=(\d+).*?DL: TX PDU.*?pdu_len=(\d+)', re.IGNORECASE)


regex_ue_setup = re.compile(r'\[CU-UEMNG\s*\].*?ue=(\d+).*?plmn=(\d+).*?pci=(\d+).*?rnti=(0x[0-9a-fA-F]+)', re.IGNORECASE)

async def tail_file_producer(path: str, data_queue: asyncio.Queue):
    file = Path(path)
    print(f"[*] Esperando a que el simulador cree el archivo: {data_file}")

    while not file.exists():
        await asyncio.sleep(0.5)

    with open(path, "r", encoding="utf-8") as f:
        print(f"[+] Archivo detectado. Leyendo en tiempo real (Fase 2)...")
        f.seek(0, 2)
        
        memoria_ue = {} 
        buffer_segundo = {}  
        segundo_actual_log = None 

        while True:
            line = f.readline()
            
            if not line:
                await asyncio.sleep(0.05)
                continue

            match_setup = regex_ue_setup.search(line)
            if match_setup:
                ue_id = match_setup.group(1)
                memoria_ue[ue_id] = {
                    "PLMN": match_setup.group(2),
                    "PCI": match_setup.group(3),
                    "RNTI": match_setup.group(4)
                }
                continue 

            match_sdap = regex_sdap.search(line)
            if match_sdap:
                log_dt_str = match_sdap.group(1)
                ue_id = match_sdap.group(2)
                dl_bytes = float(match_sdap.group(3))

                log_dt = datetime.fromisoformat(log_dt_str)
                log_segundo = log_dt.replace(microsecond=0)

                if segundo_actual_log is None:
                    segundo_actual_log = log_segundo

                if log_segundo > segundo_actual_log:
                    if buffer_segundo:

                        paquete_final = {}
                        for ue, bytes_totales in buffer_segundo.items():
                            info_red = memoria_ue.get(ue, {"PLMN": "N/A", "PCI": "N/A", "RNTI": "N/A"})
                            paquete_final[ue] = {
                                "DL_Bytes": bytes_totales,
                                "PLMN": info_red["PLMN"],
                                "PCI": info_red["PCI"],
                                "RNTI": info_red["RNTI"]
                            }
                            
                        await data_queue.put((segundo_actual_log, paquete_final))
                        buffer_segundo.clear()
                    
                    segundo_actual_log = log_segundo

                buffer_segundo[ue_id] = buffer_segundo.get(ue_id, 0.0) + dl_bytes

async def consumer(data_queue: asyncio.Queue):
    global agg_df
    ultimo_guardado_parquet = time.time() 

    while True:

        dt_timestamp, ue_data = await data_queue.get()
            
        nuevas_filas = [
            {
                "timestamp": dt_timestamp, 
                "UE_ID": str(ue), 
                "DL_Bytes": datos["DL_Bytes"],
                "PLMN": datos["PLMN"],
                "PCI": datos["PCI"],
                "RNTI": datos["RNTI"]
            } 
            for ue, datos in ue_data.items()
        ]
        
        if nuevas_filas:
            df_temporal = pl.DataFrame(nuevas_filas, schema=agg_df.schema)
            agg_df = pl.concat([agg_df, df_temporal], how="vertical")
            print(f"▶ Procesado 1s (Log Time: {dt_timestamp.strftime('%H:%M:%S')}): {len(nuevas_filas)} UEs.")

        tiempo_actual = time.time()
        if tiempo_actual - ultimo_guardado_parquet >= 30.0:
            if len(agg_df) > 0:
                print(f"\n[💾] Pasaron 30s: Guardando {len(agg_df)} registros en Parquet...\n")
                agg_df.write_parquet(parquet_file)
            ultimo_guardado_parquet = tiempo_actual

        data_queue.task_done()

app = Dash(__name__)

app.layout = html.Div([
    html.H2("Telemetría 5G: Volumen SDAP en Downlink (Tiempo Real)", style={'font-family': 'Arial'}),
    dcc.Graph(id="live-graph"),

    dcc.Interval(id="interval", interval=1000, n_intervals=0),
])

@app.callback(
    Output("live-graph", "figure"),
    Input("interval", "n_intervals"),
)
def update_graph(n):
    fig = go.Figure()
    
    if len(agg_df) == 0:
        return fig

    pdf = agg_df.to_pandas()
    

    for ue in pdf['UE_ID'].unique():
            datos_ue = pdf[pdf['UE_ID'] == ue]
            
            ultimo_dato = datos_ue.iloc[-1]
            etiqueta_leyenda = f"UE {ue} (RNTI: {ultimo_dato['RNTI']} | PCI: {ultimo_dato['PCI']} | PLMN: {ultimo_dato['PLMN']})"
            
            fig.add_trace(
                go.Scatter(
                    x=datos_ue['timestamp'], 
                    y=datos_ue['DL_Bytes'], 
                    mode="lines+markers",
                    name=etiqueta_leyenda
                )
            )
        
    fig.update_layout(
        xaxis_title="Tiempo",
        yaxis_title="Volumen (Bytes/s)",
        title="Tráfico Agregado por Usuario (1 seg)",
        template="plotly_dark",
        hovermode="x unified"
    )
    return fig

async def main():
    print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
    print(f"Procesador 5G (Fase 1) :: v{version}")
    print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■\n")

    asyncio.create_task(tail_file_producer(data_file, queue))
    asyncio.create_task(consumer(queue))

    print("[*] Iniciando servidor web de Dash en http://127.0.0.1:8050")
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=8050,
        debug=False, 
    )

if __name__ == '__main__':

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Cierre solicitado por el usuario. Guardando datos finales...")
        if len(agg_df) > 0:
            agg_df.write_parquet(parquet_file)
            print(f"[+] Backup finalizado: {parquet_file}")