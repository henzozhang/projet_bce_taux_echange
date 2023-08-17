import requests
import pandas as pd
from io import BytesIO
import zipfile
import os
from datetime import datetime

def download_and_process_csv(url, list_currency=None, start_date=None, end_date=None, folder_path=None):
    # Télécharger le fichier ZIP depuis l'URL
    today = datetime.now().strftime('%Y%m%d')
    response = requests.get(url)
    with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
        # Extraire le fichier CSV contenu dans le ZIP
        csv_filename = zip_ref.namelist()[0]
        extraction_path = 'dossier_taux_bce'
        zip_ref.extractall(extraction_path)
        with zip_ref.open(csv_filename) as csv_file:
            # Charger le CSV en DataFrame pandas
            df = pd.read_csv(csv_file)
    df.rename(columns={'Date': 'date'}, inplace=True)
    
    df['date'] = pd.to_datetime(df['date'])
    
    df.drop("Unnamed: 42",axis=1,inplace=True)

    # Créer un nouveau DataFrame avec les colonnes de list_currency
    if list_currency:
        currency_columns = ['date'] + list_currency
        df = df[currency_columns]
    
    # Filtrer le DataFrame en fonction de start_date et end_date
    if start_date:
        if end_date:
            mask = (df['date'] >= start_date) & (df['date'] <= end_date)
            df = df.loc[mask]
        
        else:
            mask = df['date'] >= start_date
            df = df.loc[mask]

    else:
        if end_date: 
            mask = df['date'] <= end_date
            df = df.loc[mask]
   

    melted_df = df.melt(id_vars=['date'], var_name='key_change', value_name='taux').sort_values([ 'date','key_change'],ascending=False)
    melted_df['x_vers_euro'] = 1 / melted_df['taux']
    
    # Exporter le DataFrame en tant que CSV dans le dossier spécifié
    if folder_path:
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        output_filename = os.path.join(folder_path, f'taux_bce_{today}.csv')
        melted_df.to_csv(output_filename, index=False)
        print(f"Fichier CSV exporté avec succès vers {output_filename}")
    else:
        folder_path=extraction_path
        output_filename = os.path.join(folder_path, f'taux_bce_{today}.csv')
        melted_df.to_csv(output_filename, index=False)
        print(f"Fichier CSV exporté avec succès vers {output_filename}")

url ='https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip?bd049ef55462a7bf91e6643cd0735a01'
download_and_process_csv(url)