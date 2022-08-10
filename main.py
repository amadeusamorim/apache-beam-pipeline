import apache_beam as beam
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
        yield (f"{uf}-{registro['ano_mes']}", registro['casos']) # Vai retornar todos os valores do for

# Variavel que recebe processos se chama pcollection
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista) # Passo o metodo que retorna o elemento
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_datas)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue) # Para Yield usa-se o FlatMap
    | "Mostrar resultados" >> beam.Map(print)
) # Nome do processo e metodo, skippando uma linha do header, retorna lista e aplica um print

pipeline.run()