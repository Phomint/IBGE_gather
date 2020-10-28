import requests
import unidecode
import pandas as pd
from sqlalchemy import create_engine
import credentials as c

def get_id(cidade):
    print('[Master[ Realizando consulta do id de '+str(cidade))
    for c in response.json():
        if unidecode.unidecode(c['nome'].lower()) == (unidecode.unidecode(str(cidade).lower())):
            print(c['nome']+' ? '+str(cidade))
            return str(c['id'])[:-1]

def get_pop(cod):
    print('[IBGE API] Coletando população de '+str(cod) if cod != None else '')
    if cod == None:
        return None
    else:
        response = requests.get('https://servicodados.ibge.gov.br/api/v1/pesquisas/indicadores/29171/resultados/'+cod)
        return response.json()[0]['res'][0]['res']['2020']

if __name__ == '__main__':
    print('[Master] Conectando ao banco de dados')
    eng = create_engine('mysql+pymysql://'+c.user+':'+c.password+'@'+c.host+'/'+c.database)
    print('[MySQL | MariaDB] Conectado com sucesso')
    query = '''SELECT DISTINCT city FROM contracts c
                JOIN people p ON c.client_id = p.id'''

    print('[MySQL | MariaDB] Realizando consulta')
    df = pd.read_sql(query, eng)
    print('[IBGE API] Coletando todos municipios')
    response = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/municipios')
    df['id_ibge'] = df['city'].apply(get_id)
    df['population'] = df['id_ibge'].apply(get_pop)
    print('[Master] Salvando arquivo')
    df.to_csv('../Upload_GSheets/Files/ibge_populacao.tsv', sep='\t')
    print('[Master] Concluído!')
