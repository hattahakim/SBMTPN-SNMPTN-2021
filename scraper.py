import os
import sqlite3
from requests_html import HTMLSession
import pandas as pd

class Data:
    def __init__(self,jalur):
        self.jalur = jalur.upper()
        self.namaUniv = []
        self.namaJurusan = []
        self.jenjangJurusan = []
        self.daya_tampung = []
        self.peminat = []
        if jalur.lower() == 'sbmptn':
            self.link = 'https://sidata-ptn.ltmpt.ac.id/ptn_sb.php'
        elif jalur.lower() == 'snmptn':
            self.link = 'https://sidata-ptn.ltmpt.ac.id/ptn_sn.php'

    def scrape(self):

        daftar_univ = HTMLSession()
        url = daftar_univ.get(self.link)

        html = url.html

        univ_univ = html.find('tr')[1:]

        for univ in univ_univ:

            nama_univ = univ.find('td')[2].text.split('\n')[0]

            link_jurusan = os.path.join(self.link,univ.find('td')[3].find('a')[0].attrs['href'])

            daftar_jurusan = HTMLSession()

            url = daftar_jurusan.get(link_jurusan)

            html = url.html

            jurusan_jurusan = html.find('tr')[2:]

            for jurusan in jurusan_jurusan:

                try:
                    info_jurusan = jurusan.find('td')
                    nama_jur = info_jurusan[2].text.lower()
                    jenjang = info_jurusan[3].text
                    daya_tampung_jur = info_jurusan[4].text
                    peminat_jur = info_jurusan[5].text

                    self.namaUniv.append(nama_univ)
                    self.namaJurusan.append(nama_jur.upper())
                    self.jenjangJurusan.append(jenjang)
                    self.daya_tampung.append(daya_tampung_jur)
                    self.peminat.append(peminat_jur)

                    print(nama_univ,nama_jur.upper(),jenjang,daya_tampung_jur,peminat_jur)
                except:
                    pass
    def olah(self):

        # BUAT DATAFRAME
        d = {'Universitas':self.namaUniv,'Jurusan':self.namaJurusan,'Jenjang':self.jenjangJurusan,
             'Daya Tampung 2021':self.daya_tampung,'Peminat 2020':self.peminat}

        df = pd.DataFrame(data=d)
        df['Daya Tampung 2021'] = df['Daya Tampung 2021'].astype(int)

        # PENGOLAHAN DATAFRAME
        for i in range(len(df)):
            try:
                df.loc[i,'Peminat 2020'] = int(df.loc[i,'Peminat 2020'])
            except:
                df.loc[i,'Peminat 2020'] = int(df.loc[i,'Peminat 2020'][0] + df.loc[i,'Peminat 2020'][2:])

        for i in range(len(df)):
            if df.loc[i,'Daya Tampung 2021'] <= df.loc[i,'Peminat 2020']:
                df.loc[i,'Persentase'] = round((df.loc[i,'Daya Tampung 2021']/df.loc[i,'Peminat 2020'])*100,2)
            elif df.loc[i,'Daya Tampung 2021'] > df.loc[i,'Peminat 2020']:
                df.loc[i,'Persentase'] = 100

        df = df.sort_values('Universitas',ascending=False)

        # EXPORT KE CSV DAN EXCEL
        try:
            os.makedirs(f'{self.jalur}')
        except:
            pass
        df.to_csv(f'{self.jalur}/{self.jalur.lower()}2021.csv',index=False)
        df.to_excel(f'{self.jalur}/{self.jalur.lower()}2021.xlsx',index=False)

        # EXPORT KE DATABASE
        con = sqlite3.connect(f'{self.jalur}/{self.jalur.lower()}2021.db')
        df.to_sql(f'tabel{self.jalur[0:3]}',con,index=False,if_exists='replace')

        # BUAT FILE TIAP UNIV
        try:
            os.makedirs('SNMPTN/Universitas')
        except:
            pass

        for univ in df['Universitas'].unique():
            condition = (df['Universitas'] == univ)
            univ_df = df.loc[condition].sort_values('Jurusan')
            univ_df.to_csv(f'{self.jalur}/Universitas/{univ}.csv',index=False)

        # BUAT FILE TIAP UNIV
        try:
            os.mkdir('SNMPTN/Jurusan')
        except:
            pass

        for jurusan in sorted(df['Jurusan'].unique()):
            if '/' in jurusan:
                jurusan = jurusan.replace('/','-')

            condition = (df['Jurusan'] == jurusan)
            jurusan_df = df.loc[condition].sort_values('Universitas')
            jurusan_df.to_csv(f'{self.jalur}/Jurusan/{jurusan}.csv',index=False)

    def mulai(self):
        self.scrape()
        self.olah()
