# Trabalho PySpark
🎬 Análise e Recomendação de Filmes com PySpark
Um sistema de análise e recomendação de filmes construído usando PySpark e o dataset MovieLens 25M.



📋 Visão Geral
Este projeto demonstra como construir um sistema de recomendação de filmes utilizando técnicas de filtragem colaborativa através do algoritmo ALS (Alternating Least Squares) do Apache Spark. O sistema analisa o dataset MovieLens 25M, que contém 25 milhões de avaliações de 62.000 filmes feitas por 162.000 usuários.




🌟 Funcionalidades

Análise Exploratória de Dados:

Filmes mais populares (com maior número de avaliações)
Filmes com melhores médias de avaliação
Distribuição de avaliações (ratings)
Análise por gênero de filme


Sistema de Recomendação:

Modelo de filtragem colaborativa usando ALS
Treinamento e avaliação do modelo
Recomendações personalizadas para usuários
Visualização das recomendações



🛠 Tecnologias Utilizadas

PySpark: Framework de processamento distribuído para análise de grandes volumes de dados
Spark MLlib: Biblioteca de Machine Learning do Apache Spark
Python: Linguagem de programação principal
Matplotlib: Biblioteca para visualização de dados
Kaggle API: Para download do dataset MovieLens 25M

📊 Dataset
O projeto utiliza o famoso dataset MovieLens 25M, que inclui:

25 milhões de avaliações de filmes
62.000 filmes
162.000 usuários
Dados coletados entre janeiro de 1995 e novembro de 2019

🚀 Como Executar
Pré-requisitos

Python 3.x
Apache Spark
Bibliotecas Python listadas em requirements.txt

Instalação

Clone este repositório:
git clone https://github.com/seu_usuario/analise-recomendacao-filmes-pyspark.git
cd analise-recomendacao-filmes-pyspark

Instale as dependências:
pip install -r requirements.txt

Execute o notebook Jupyter ou o script Python:
jupyter notebook "Análise e Recomendação de Filmes com PySpark_25M.ipynb"
ou
python análise_e_recomendação_de_filmes_com_pyspark_25m.py


📈 Resultados
O sistema é capaz de:

Identificar tendências nos dados de avaliação de filmes
Gerar recomendações personalizadas com base no histórico de avaliações dos usuários
Avaliar a qualidade das recomendações usando métricas como RMSE (Root Mean Square Error)

🔍 Estrutura do Código
O código segue as seguintes etapas:

Configuração do ambiente: Instalação e importação das bibliotecas necessárias
Carregamento dos dados: Leitura dos arquivos CSV do MovieLens 25M
Análise exploratória: Geração de insights sobre os dados
Pré-processamento: Preparação dos dados para o modelo de recomendação
Modelagem: Treinamento do algoritmo ALS para recomendações
Avaliação: Teste do modelo e cálculo de métricas de desempenho
Visualização: Representação gráfica dos resultados


👥 Autores

Eduardo Arruda Remião

📚 Referências

MovieLens 25M Dataset
Apache Spark Documentation
Collaborative Filtering with Spark
