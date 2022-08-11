# Pipelines no Apache Beam
### Chuvas e dengue no Brasil (2015 - 2019)

O projeto tem por base a análise da quantidade de chuvas e casos de dengue em estados brasileiros do ano de 2015 à 2019.

O projeto inicia com a **ingestão de dados** utilizando a SDK do Apache Beam, utilizando **código Python**, trazendo uma série de classes, métodos, módulos necessários para realizar todos os processos, desde a ingestão de dados até a persistência deles.

1. **Ingestão de dados**: foi utilizado o ``ReadFromText``, onde foi feita a leitura, trazendo para dentro do processo o dado bruto em .txt dos casos de dengue, contendo várias informações sobre a quantidade de casos de dengue por cidades e estados no Brasil; Como também os dados de quantidade de chuva (precipitação meteorológica) em .csv, com várias estações meteorológicas também no arquivo bruto.

2. **Transformação de dados**: Após a realização da leitura dos arquivos, trouxe a informação para dentro do processo, no qual foram aplicadas várias transformações, que refletiram as regras de negócio definidas na fáse de análise. 
Foi utilizado o método ``Map`` para realizar algumas transformações, como também o ``FlatMap``, agrupando pela chave, passando um método de soma. No geral, foram aplicadas diversas transformações para ir trabalhando o dado passo a passo dentro das pipelines. Na etapa de transformação foram utilizados também outros métodos, tais como: ``CoGroupByKey``, ``Flatten``, ``CombinePerKey``, etc.

3. **Persistência dos dados**: Foi persistido os dados no final em .csv para que fosse mais fácil de realizar uma análise. Foi utilizado o método ``WriteToText``, para consolidar todos esses tratamentos em um arquivo.

4. **Analisar os dados**: Foi realizado um rápido insight com os dados oriundos do tratamento com o Jupyter Notebook e com a biblioteca Pandas.
