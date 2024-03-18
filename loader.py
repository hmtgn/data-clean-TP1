import os
import requests
import numpy as np
import pandas as pd


# Obtenez le chemin absolu du répertoire du script Python en cours d'exécution
current_dir = os.path.dirname(os.path.abspath(__file__))

DATA_PATH_STEP1 = os.path.join(current_dir, 'data/MMM_MMM_DAE.csv')


def sample_formatted(data_csv):
    # still need to choose which columns we have to keep
    kept_columns = [
        'nom', 'adr_num', 'adr_voie',
        'com_cp', 'com_nom',
        'tel1',
        'freq_mnt', 'dermnt',
        'lat_coor1', 'long_coor1']
    df_local = pd.read_csv(data_csv)
    df_local = pd.DataFrame(df_local, columns=kept_columns)
    print(df_local)
    return df_local


def sample_sanitized(df_local):
    # sanitizing nom
    df['nom'] = df['nom'].str.replace('�', 'é')
    print(df_local['nom'])

    # sanitizing com_nom
    df.loc[pd.to_numeric(df['com_nom'], errors='coerce').notna(), 'com_nom'] = 'Montpellier'
    df['com_nom'] = df['com_nom'].replace(' ', 'Montpellier')
    print(df_local['com_nom'])

    # sanitizing adr_voie
    df['adr_voie'] = df['adr_voie'].str.replace('�', 'é')
    print(df_local['adr_voie'])

    return df_local


def sample_framed(df_local):
    # Fusionner 'adr_num' et 'adr_voie' en une seule colonne 'adresse'
    df_local['adresse'] = df_local['adr_num'] + ' ' + df_local['adr_voie']
    # Supprimer les colonnes 'adr_num' et 'adr_voie'
    df_local.drop(columns=['adr_num', 'adr_voie'], inplace=True)
    # Déplacer la colonne 'adresse' en deuxième position
    cols = df_local.columns.tolist()
    cols.insert(1, cols.pop(cols.index('adresse')))
    df_local = df_local.reindex(columns=cols)

    # Fusionner 'lat_coor1' et 'long_coor1' en une seule colonne 'coordonnees'
    df_local['coordonnees'] = df_local['lat_coor1'].astype(str) + ', ' + df_local['long_coor1'].astype(str)
    # Supprimer les colonnes 'lat_coor1' et 'long_coor1'
    df_local.drop(columns=['lat_coor1', 'long_coor1'], inplace=True)


    df_local.rename(columns={'dermnt': 'der_mnt', 'tel1': 'tel_num1'}, inplace=True)
    print(df_local)
    return df_local


df=sample_formatted(DATA_PATH_STEP1)
print("------------------------------------------------------------------------------------------------------------------------------------")
df=sample_sanitized(df)
print("------------------------------------------------------------------------------------------------------------------------------------")
df=sample_framed(df)
print("------------------------------------------------------------------------------------------------------------------------------------")



DATA_PATH = 'data/MMM_MMM_DAE.csv'

# def sample_formatted(data_csv):
#     kept_columns = ['nom,lat_coor1,x,long_coor1,y,adr_num,adr_voie,com_cp,com_insee,com_nom,acc,acc_lib,acc_pcsec,acc_acc,acc_etg,acc_complt,photo1,photo2,disp_j,disp_h,disp_compl,tel1,tel2,site_email,date_insta,etat_fonct,fab_siren,fab_rais,mnt_siren,mnt_rais,modele,num_serie,id_euro,lc_ped,dtpr_lcped,dtpr_lcad,dtpr_bat,freq_mnt,dispsurv,dermnt,expt_siren,expt_rais,expt_tel1,expt_tel2,expt_email,ref,id,appartenan,dae_mobile,date1insta,chgtpiles']
#     pd.read_csv(data_csv)
#     print(data_csv)

# sample_formatted('sample_dirty.csv')



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
    df = pd.read_csv(
        data_fname,
        ...
        )
    return df


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """One function to do all sanitizing"""
    ... 
    return df


# Define a framing function
def frame_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function all framing (column renaming, column merge)"""
    df.rename(...)
    ...
    return df


# once they are all done, call them in the general clean loading function
def load_clean_data(df:pd.DataFrame)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (df.pipe(load_formatted_data)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    return df


# if the module is called, run the main loading function
if __name__ == '__main__':
    load_clean_data(download_data())