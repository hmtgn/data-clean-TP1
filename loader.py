import os
import requests
import numpy as np
import pandas as pd


# Obtenez le chemin absolu du répertoire du script Python en cours d'exécution
current_dir = os.path.dirname(os.path.abspath(__file__))

DATA_PATH_STEP1 = os.path.join(current_dir, 'data/MMM_MMM_DAE.csv')

DATA_PATH = 'data/MMM_MMM_DAE.csv'
url_mtp = "https://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_DAE.csv"

def download_data(url, force_download=False, ):
    # Utility function to donwload data if it is not in disk
    data_path = os.path.join('data', os.path.basename(url.split('?')[0]))
    if not os.path.exists(data_path) or force_download:
        # ensure data dir is created
        os.makedirs('data', exist_ok=True)
        # request data from url
        response = requests.get(url, allow_redirects=True)
        # save file
        with open(data_path, "w") as f:
            # Note the content of the response is in binary form: 
            # it needs to be decoded.
            # The response object also contains info about the encoding format
            # which we use as argument for the decoding
            f.write(response.content.decode(response.apparent_encoding))

    return data_path


def load_formatted_data(data_fname:str) -> pd.DataFrame:
    """ One function to read csv into a dataframe with appropriate types/formats.
        Note: read only pertinent columns, ignore the others.
    """
    kept_columns = [
        'nom', 'adr_num', 'adr_voie',
        'com_cp', 'com_nom',
        'tel1',
        'freq_mnt', 'dermnt',
        'lat_coor1', 'long_coor1']

    df = pd.read_csv(data_fname)
    df = pd.DataFrame(df, columns=kept_columns)
    print(df)
    return df


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """One function to do all sanitizing"""
        # sanitizing nom
    df['nom'] = df['nom'].str.replace('�', 'é')

    # sanitizing com_nom
    df.loc[pd.to_numeric(df['com_nom'], errors='coerce').notna(), 'com_nom'] = 'Montpellier'
    df['com_nom'] = df['com_nom'].replace(' ', 'Montpellier')

    # sanitizing adr_voie
    df['adr_voie'] = df['adr_voie'].str.replace('�', 'é')

    print(df)

    return df


# Define a framing function
def frame_data(df:pd.DataFrame) -> pd.DataFrame:
    # Fusionner 'adr_num' et 'adr_voie' en une seule colonne 'adresse'
    df['adresse'] = df['adr_num'] + ' ' + df['adr_voie']
    # Supprimer les colonnes 'adr_num' et 'adr_voie'
    df.drop(columns=['adr_num', 'adr_voie'], inplace=True)
    # Déplacer la colonne 'adresse' en deuxième position
    cols = df.columns.tolist()
    cols.insert(1, cols.pop(cols.index('adresse')))
    df = df.reindex(columns=cols)

    # Fusionner 'lat_coor1' et 'long_coor1' en une seule colonne 'coordonnees'
    df['coordonnees'] = df['lat_coor1'].astype(str) + ', ' + df['long_coor1'].astype(str)
    # Supprimer les colonnes 'lat_coor1' et 'long_coor1'
    df.drop(columns=['lat_coor1', 'long_coor1'], inplace=True)


    df.rename(columns={'dermnt': 'der_mnt', 'tel1': 'tel_num1'}, inplace=True)
    print(df)
    return df


# once they are all done, call them in the general clean loading function
def load_clean_data(df:pd.DataFrame)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (df.pipe(load_formatted_data)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    return df



df=load_formatted_data(DATA_PATH_STEP1)
print("------------------------------------------------------------------------------------------------------------------------------------")
df=sanitize_data(df)
print("------------------------------------------------------------------------------------------------------------------------------------")
df=frame_data(df)
print("------------------------------------------------------------------------------------------------------------------------------------")

# if the module is called, run the main loading function

if __name__ == '__main__':
    df = load_clean_data(download_data(url_mtp))




