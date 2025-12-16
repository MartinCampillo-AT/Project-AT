import pandas as pd
import os
import glob

# ---------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------
BASE_PATH = "/Users/martincampillopereda/Desktop/ProjectAT/data"
COL_NAMES = ['date', 'time', 'open', 'high', 'low', 'close', 'volume']

def cargar_datos_inteligente(base_path):
    print(f"🔍 Buscando archivos en: {base_path} ...")
    
    # 1. BÚSQUEDA RECURSIVA (EL SABUESO)
    # Buscamos cualquier CSV que empiece por "DAT_MT_EURUSD_M1_" en cualquier subcarpeta
    # ** significa "cualquier profundidad de carpetas"
    search_pattern = os.path.join(base_path, "**", "DAT_MT_EURUSD_M1_*.csv")
    
    # recursive=True permite buscar dentro de subcarpetas
    all_files = glob.glob(search_pattern, recursive=True)
    
    # Ordenamos para que cargue 2011, luego 2012, etc.
    all_files.sort()
    
    if not all_files:
        print("❌ ERROR CRÍTICO: No se ha encontrado NINGÚN archivo CSV que coincida.")
        print("   -> Verifica que la carpeta 'data' no esté vacía.")
        print("   -> Verifica que los archivos empiecen por 'DAT_MT_EURUSD_M1_'")
        return None

    print(f"✅ Se han encontrado {len(all_files)} archivos aptos.")
    
    # Mostramos los primeros y últimos para verificar
    print("   -> Primero:", os.path.basename(all_files[0]))
    print("   -> Último: ", os.path.basename(all_files[-1]))
    
    # 2. CARGA Y FUSIÓN
    all_dataframes = []
    
    for file_path in all_files:
        filename = os.path.basename(file_path)
        print(f"   Reading: {filename} ...")
        
        try:
            # HistData usa comas o punto y coma. Probamos coma primero.
            df_temp = pd.read_csv(file_path, names=COL_NAMES, header=None)
            
            # Verificación rápida de que no estamos leyendo basura
            if len(df_temp) < 10:
                print(f"   ⚠️ Warning: {filename} parece vacío o muy pequeño.")
            
            all_dataframes.append(df_temp)
            
        except Exception as e:
            print(f"   ❌ Error leyendo {filename}: {e}")

    # 3. CONCATENACIÓN FINAL
    if not all_dataframes:
        return None

    print("\nCombinando todo en un DataFrame gigante...")
    df_total = pd.concat(all_dataframes, ignore_index=True)
    
    # 4. LIMPIEZA DE FECHAS
    print("Convirtiendo fechas (esto tardará unos 30-60 segundos)...")
    
    # Truco de velocidad: Convertir solo si son strings
    df_total['timestamp'] = pd.to_datetime(
        df_total['date'] + ' ' + df_total['time'], 
        format='%Y.%m.%d %H:%M'
    )
    
    df_total.set_index('timestamp', inplace=True)
    df_total.drop(columns=['date', 'time'], inplace=True)
    df_total.sort_index(inplace=True)
    
    # Quitar duplicados
    df_total = df_total[~df_total.index.duplicated(keep='first')]
    
    print(f"✅ ¡Éxito! Total de minutos cargados: {len(df_total)}")
    return df_total

def crear_resamples(df_m1):
    print("\n--- Creando Timeframes (Resampling) ---")
    
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    print("Generando 5 Minutos...")
    df_5m = df_m1.resample('5min').agg(agg_dict).dropna()
    
    print("Generando 1 Hora...")
    df_1h = df_m1.resample('1h').agg(agg_dict).dropna()
    
    print("Generando Diario...")
    df_1d = df_m1.resample('1D').agg(agg_dict).dropna()
    
    return df_5m, df_1h, df_1d

# --- EJECUCIÓN ---
df_master_m1 = cargar_datos_inteligente(BASE_PATH)

if df_master_m1 is not None:
    df_5m, df_1h, df_1d = crear_resamples(df_master_m1)
    
    # Guardar
    print("\nGuardando copias de seguridad (.pkl)...")
    df_5m.to_pickle("EURUSD_5M.pkl")
    df_1h.to_pickle("EURUSD_1H.pkl")
    df_1d.to_pickle("EURUSD_1D.pkl")
    print("✅ Todo listo. Archivos guardados.")
    
    print("\nMuestra (1H):")
    print(df_1h.tail())