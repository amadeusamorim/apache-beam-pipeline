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

# Variavel que recebe processos se chama pcollection
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista) # Passo o metodo que retorna o elemento
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Mostrar resultados" >> beam.Map(print)
) # Nome do processo e metodo, skippando uma linha do header, retorna lista e aplica um print

pipeline.run()