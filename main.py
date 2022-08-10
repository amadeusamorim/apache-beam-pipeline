import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None) # Opcoes de pipeline
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue =[
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']

def lista_para_dicionario(elemento, colunas):
    """
    Recebe 2 listas
    Retorna 1 dicionário
    """
    return dict(zip(colunas, elemento))

def texto_para_lista(elemento, delimitador='|'): # Delimitador padrao é o pipe
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador) # Transformando a string em lista, pelo delimitador

def trata_datas(elemento):
    """
    Recebe um dicionário e cria um novo campo com ANO-MÊS
    Retorna o mesmo dicionario com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2]) # O join junta atraves do parametro, no caso '-'
    return elemento
    
def chave_uf(elemento):
    """
    Receber um dicionário
    Retornar uma tupla com o Estado(UF) e o elemento (UF, dicionario)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{}, {}])
    Retorna uma tupla ('RS-2014-12', 8.0)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])): # Se retornar verdadeira, faço o Yeld. Se retornar False, é porque o dado 'casos' estava vazio
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos'])) # Vai retornar todos os valores do for, transformo em float para que possa ser somado
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0) # Se tiver vazio, retorna zero

def chave_uf_ano_mes_de_lista(elemento):
    """
    Receber uma lista de elementos
    Retornar uma tupla contendo uma chave e valor de chuva em mm
    ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = elemento # Cada variavel, é um pedaço da lista
    ano_mes = '-'.join(data.split('-')[:2]) # Splitei a minha data e peguei os dois primeiros elementos e juntei novamente com ano e mes
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0 # Trata erro do dataset que constam valores negativos, transformando em 0.0
    else:
        mm = float(mm) # Transforma os mm em float para realizar operacoes
    return chave, mm # Retorna a chave formatada e a chuva em mm

def arredonda(elemento):
    """
    Recebe uma tupla ('MT-2019-12', 6674.399999999898)
    Retorna uma tupla com o valor arredondado ('MT-2019-12', 6674.4)
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def filtra_campos_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    Recebe uma tupla ('CE-2015-10', {'chuvas': [0.0], 'dengue': []})
    Retorna uma tupla sem campos vazios
    """
    chave, dados = elemento # A tupla vai para variavel dados
    if all([
        dados['chuvas'],
        dados['dengue']
        ]):
        return True
    return False

def descompactar_elementos(elemento):
    """
    Receber uma tupla ('CE-2015-08', {'chuvas': [0.0], 'dengue': [169.0]})
    Retornar uma tupla ('CE',2015,08, 0.0,-169.0)
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0] # Pegando o unico elemento da lista para nao retornar lista na func
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, int(ano), int(mes), chuva, dengue

# Variavel que recebe processos se chama pcollection
# Cada processo é uma pipeline

# Var dengue mostra a quantidade de casos
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('sample_casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista) # Passo o metodo que retorna o elemento
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_datas)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue) # Para Yield usa-se o FlatMap
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum) # Pegam o segundo elemento e somam de acordo com as chaves iguais
    # | "Mostrar resultados" >> beam.Map(print)
) # Nome do processo e metodo, skippando uma linha do header, retorna lista e aplica um print

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> 
        ReadFromText('sample_chuvas.csv', skip_header_lines=1) # ReadFromText ajuda no processamento em ambientes clusterizados, melhora desempenho
    | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista, delimitador=',') # Nao posso ter duas pipelines com o mesmo nome, tem parecida na var dengue
    | "Criando a chave UF-ANO-MES" >> beam.Map(chave_uf_ano_mes_de_lista) # Pipeline de criacao de chave e valor
    | "Soma do total de chuvas pela chave" >> beam.CombinePerKey(sum) # Como no pcollection anterior, pega a mesma chave e agrupa os valores com o parâmetro (sum)
    | "Arredondar resultados de chuvas" >> beam.Map(arredonda) # Arredonda a soma para uma casa decimal
    # | "Mostrar resultados de chuvas" >> beam.Map(print)
)

# pcollection que junta as duas anteriores
resultado = (
    # (chuvas, dengue) # Passo como parametro as duas pcollections anteriores
    # | "Empilha as pcols" >> beam.Flatten() # Une as pcollections do parametro (empilha)
    # | "Agrupa as pcols" >> beam.GroupByKey()
    ({'chuvas':chuvas, 'dengue': dengue})
    | 'Mesclar pcols' >> beam.CoGroupByKey() # Faz o agrupamento pela chave
    | 'Filtrar dados vazios' >> beam.Filter(filtra_campos_vazios) # O Filter espera um retorno de True or False para saber quem deixa na npcollections
    | 'Descompactar elementos' >> beam.Map(descompactar_elementos)
    | "Mostrar resultados da uniao" >> beam.Map(print) 
)

pipeline.run()