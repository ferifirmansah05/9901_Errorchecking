import streamlit as st
import pandas as pd
import zipfile
import io
import os  
from glob import glob
import csv
import requests
import pickle
import os
import openpyxl
import numpy as np
import time
import datetime as dt
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import tempfile


st.title('Automate Error Checking (99.01)')
st.markdown('### Upload file *Zip')
uploaded_file = st.file_uploader("Pilih file",type="xlsx", accept_multiple_files=True)

if uploaded_file is not None:
    st.write('File berhasil diupload')
    # Baca konten zip file

    if st.button('Process'):
        with tempfile.TemporaryDirectory() as tmpdirname:

            dfs =[]
            for file in uploaded_file:
                    dfs.append(pd.read_excel(file))
                    
                    
            #df_prov = pd.read_csv(f'{tmpdirname}/_bahan/database provinsi.csv', encoding='latin1')
            #df_prov = df_prov.loc[:,['Kode Cabang','Provinsi Gudang','Kota/Kabupaten']].rename(columns={'Kode Cabang':'Nama Cabang'})
            df_9901 =   pd.concat(dfs,ignore_index=True).fillna('')

            # Filter kolom-kolom yang berawalan "Unnamed:"
            kolom_unnamed = df_9901.filter(regex='^Unnamed:').columns
            # Hapus kolom-kolom tersebut
            df_9901.drop(columns=kolom_unnamed, inplace=True)

            # Filter the codes that start with '1' or '2'
            df_9901 = df_9901[df_9901['Kode #'].astype(str).str.startswith(('1', '2', '4', '9'))] #'1', '2', '4', '9'

            # Make it same format'
            df_9901['Provinsi Gudang'] = df_9901['Provinsi Gudang'].str.title()

            ## Assuming df is your DataFrame and these are the columns you mentioned
            columns_to_clean = ['#Purch.Qty', '#Purch.@Price', '#Purch.Discount', '#Purch.Total', '#Prime.Ratio', '#Prime.Qty', '#Prime.NetPrice']

            # Remove commas from values in specified columns
            #for col in columns_to_clean:
            #    df_9901[col] = df_9901[col].str.replace(',', '')

            # Convert values in specified columns to numeric type
            for col in columns_to_clean:
                df_9901[col] = pd.to_numeric(df_9901[col])

            #SAVE
            #df_9901x = df_9901.to_csv(f'{tmpdirname}/_olah/9901_{saveas}.csv', index=False)

            df_salah_cg = df_9901[(~ df_9901.apply(lambda row: re.findall(r'(\w+)$',row['Nama Cabang'])[0] in row['Nama Gudang'], axis=1)) 
                    & (df_9901['Nama Gudang']!='')]

            df_9901_kode = df_9901[['Nama Cabang', 'Nama Gudang','Nomor #']]
            df_9901_kode['Kode'] = df_9901_kode['Nama Cabang']+df_9901_kode['Nama Gudang']+df_9901_kode['Nomor #']
            df_9901_kode['Count Kode'] = df_9901_kode.groupby('Kode')['Kode'].transform('count')
            df_9901_kode = df_9901_kode.sort_values(by='Count Kode', ascending=False).drop_duplicates('Nomor #').reset_index(drop=True)

            df_9901_kode[(~ df_9901_kode.apply(lambda row: re.findall(r'(\w+)$',row['Nama Cabang'])[0] in row['Nama Gudang'], axis=1)) 
                    | (df_9901_kode['Nama Gudang']=='')]

            df_9901_kode.loc[(~ df_9901_kode.apply(lambda row: re.findall(r'(\w+)$',row['Nama Cabang'])[0] in row['Nama Gudang'], axis=1)) 
                    | (df_9901_kode['Nama Gudang']==''), ['Nama Cabang', 'Nama Gudang']] = ''

            df_9901_kode = df_9901_kode.rename(columns={'Nama Cabang':'New-Nama Cabang',
                                                        'Nama Gudang':'New-Nama Gudang'}).drop(columns=['Count Kode','Kode'])

            df_salah_cg = pd.merge(df_salah_cg, df_9901_kode, how='left', on='Nomor #').fillna('')
            #df_salah_cg.to_csv(f'{tmpdirname}/_final/Salah Cabang_Gudang.csv', index=False)

            df_9901['Keterangan'] = ''  # Create 'Keterangan' column if not already present

            # Fill 'Keterangan' column with "Free Item" where '#Prime.NetPrice' is 0
            df_9901.loc[df_9901['#Prime.NetPrice'] == 0., 'Keterangan'] = 'Free Item'

            df_9901_FI  =   df_9901[df_9901['Keterangan']       ==      'Free Item']

            #df_9901_FI.to_csv(f'{tmpdirname}/_final/Free Item.csv', index=False)

            def download_file_from_github(url, save_path):
                response = requests.get(url)
                if response.status_code == 200:
                    with open(save_path, 'wb') as file:
                        file.write(response.content)
                    print(f"File downloaded successfully and saved to {save_path}")
                else:
                    print(f"Failed to download file. Status code: {response.status_code}")
            url = 'https://raw.githubusercontent.com/ferifirmansah05/9901_Errorchecking/main/database barang.csv'

            # Path untuk menyimpan file yang diunduh
            save_path = 'database barang.csv'
            
            # Unduh file dari GitHub
            download_file_from_github(url, save_path)
            
            # Muat model dari file yang diunduh
            if os.path.exists(save_path):
                df_prov = load_excel(save_path)
                print("Model loaded successfully")
            else:
                print("Model file does not exist")
                
            df_database_barang = pd.read_csv(f'database barang.csv').fillna('')
            
            df_database_barang = df_database_barang.drop_duplicates().reset_index(drop=True)

            df_9901_cek                   = df_9901.loc[:,['Kode #','Nama Barang']]
            df_database_barang_cek        = df_database_barang.loc[:,['Kode #']].drop_duplicates()
            df_database_barang_cek['Cek'] = 'Cek'

            df_9901_cek['Kode #']               = df_9901_cek['Kode #'].astype('object')
            df_database_barang_cek['Kode #']    = df_database_barang_cek['Kode #'].astype('object')

            df_database_barang_cek        = pd.merge(df_9901_cek, df_database_barang_cek, how='left', on='Kode #').fillna('')
            df_database_barang_cek        = df_database_barang_cek[df_database_barang_cek['Cek']  ==  "Cek"]

            df_database_barang_cek        = df_database_barang_cek.loc[:,['Kode #','Nama Barang']]
            df_database_barang_cek

            df_database_barang = df_database_barang.rename(columns={'Nama Barang':'New-Nama Barang','Kode #Nama Barang':'Kode Nama Barang'})
            df_kode_namabarang = df_database_barang.loc[:,['Kode Nama Barang']].drop_duplicates()

            df_9901con = [
                df_9901['Nama Cabang'].str.startswith('H'),
                df_9901['Nama Cabang'].str.startswith('5'),
                df_9901['Nama Cabang'].str.startswith('2')
            ]

            # Buat nilai yang sesuai untuk kondisi tersebut
            pilih = ['HO','WH/DC','WH/DC']

            # Update kolom 'Kode Cabang' dengan nilai yang sesuai
            df_9901['Kode Cabang'] = np.select(df_9901con, pilih, default= "RESTO")

            df_salah_b1 = pd.merge(df_9901[df_9901['Kode #'].astype('str').str.startswith(('1','2','4'))], df_database_barang[['Kode #','New-Nama Barang']], on='Kode #')
            df_salah_b1 = df_salah_b1[df_salah_b1.apply(lambda row:row['Nama Barang']!=row['New-Nama Barang'],axis=1)]

            #df_salah_b1.to_csv(f'{tmpdirname}/_final/Salah Nama Barang.csv', index=False)

            df_satuan = df_9901.drop(columns=['Kode Cabang'])
            df_satuan = df_satuan[df_satuan['Keterangan'] != "Free Item"]

            df_satuan = df_satuan.merge(df_satuan.groupby(['Nama Cabang','Nama Barang']).agg({'#Prime.NetPrice':'mean','Kategori Barang':'count'}).reset_index().rename(columns={'Kategori Barang':'Jumlah xTransaksi','#Prime.NetPrice':'weight_avg_#Prime.NetPrice'}),
                                        on =['Nama Cabang','Nama Barang'], how='left')
            df_satuan['percentage_#Prime.NetPrice'] = df_satuan['#Prime.NetPrice']/df_satuan['weight_avg_#Prime.NetPrice']

            df_satuan1  =   df_satuan[(df_satuan['Kode #'].astype(str).str.startswith('1')) &
                            (((df_satuan['percentage_#Prime.NetPrice']>1.5) | (df_satuan['percentage_#Prime.NetPrice']<0.5)))
                            ].sort_values('percentage_#Prime.NetPrice',ascending=False)
            df_satuan2  =   df_satuan[(df_satuan['Kode #'].astype(str).str.startswith('2')) &
                            (((df_satuan['percentage_#Prime.NetPrice']>1.5) | (df_satuan['percentage_#Prime.NetPrice']<0.5)))
                            ].sort_values('percentage_#Prime.NetPrice',ascending=False)
            df_satuan4  =   df_satuan[(df_satuan['Kode #'].astype(str).str.startswith('4')) &
                            (((df_satuan['percentage_#Prime.NetPrice']>1.5) | (df_satuan['percentage_#Prime.NetPrice']<0.5)))
                            ].sort_values('percentage_#Prime.NetPrice',ascending=False)
            df_satuan9  =   df_satuan[(df_satuan['Kode #'].astype(str).str.startswith('9')) &
                            (((df_satuan['percentage_#Prime.NetPrice']>1.5) | (df_satuan['percentage_#Prime.NetPrice']<0.5)))
                            ].sort_values('percentage_#Prime.NetPrice',ascending=False)

            st.markdown('### Output')
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                zip_file.writestr(f'Salah Cabang.csv', df_salah_cg.to_csv(index=False))
                zip_file.writestr(f'Free Item.csv', df_9901_FI.to_csv(index=False))
                zip_file.writestr(f'Salah Nama Barang.csv', df_salah_b1.to_csv(index=False))
                zip_file.writestr(f'Salah Kuantitas_Satuan_Harga (Code 1).csv', df_satuan1.to_csv(index=False))
                zip_file.writestr(f'Salah Kuantitas_Satuan_Harga (Code 2).csv', df_satuan2.to_csv(index=False))
                zip_file.writestr(f'Salah Kuantitas_Satuan_Harga (Code 4).csv', df_satuan4.to_csv(index=False))
                zip_file.writestr(f'Salah Kuantitas_Satuan_Harga (Code 9).csv', df_satuan9.to_csv(index=False))
            
            # Pastikan buffer ZIP berada di awal
            zip_buffer.seek(0)
            
            # Tombol untuk mengunduh file ZIP
            st.download_button(
                label="Download all Files",
                data=zip_buffer,
                file_name=f'AEC-9901 Error.zip',
                mime='application/zip',
            )  
