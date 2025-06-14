# Spark Movie Analytics com Grafana
🎬 Sistema Avançado de Análise e Recomendação de Filmes com Monitoramento em Tempo Real

Um sistema completo de análise de dados e recomendação de filmes construído com **PySpark**, **Prometheus** e **Grafana**, utilizando o dataset MovieLens para demonstrar técnicas avançadas de Data Engineering e MLOps.

## 📋 Visão Geral

Este projeto demonstra como construir um pipeline completo de Data Engineering com:
- **Análise de Big Data** usando PySpark e o dataset MovieLens 20M
- **Sistema de Recomendação** com algoritmo ALS (Alternating Least Squares)
- **Monitoramento em Tempo Real** através de métricas Prometheus
- **Dashboards Interativos** no Grafana para visualização avançada
- **Data Quality Monitoring** com scores de qualidade automatizados

O sistema processa **25 milhões de avaliações** de **62.000 filmes** feitas por **162.000 usuários**, com monitoramento completo de performance e qualidade dos dados.

## 🚀 Arquitetura do Sistema

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PySpark       │    │   Prometheus    │    │    Grafana      │
│   Analytics     │───▶│   Metrics       │───▶│   Dashboards    │
│   Engine        │    │   Server        │    │   Visualization │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MovieLens     │    │   Performance   │    │   Real-time     │
│   20M Dataset   │    │   Monitoring    │    │   Monitoring    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🌟 Funcionalidades Avançadas

### 📊 Análise de Dados
- **Análise Exploratória Completa**: Filmes mais populares, melhores avaliações, distribuição de ratings
- **Análise de Gêneros**: Distribuição detalhada por categorias de filmes
- **Data Quality Monitoring**: Validação automática de qualidade dos dados
- **Performance Tracking**: Monitoramento em tempo real de todas as operações

### 🤖 Machine Learning
- **Sistema de Recomendação**: Modelo ALS com filtragem colaborativa
- **Avaliação de Modelo**: Métricas RMSE e validação cruzada
- **Tratamento de Cold Start**: Estratégias para novos usuários/filmes

### 📈 Monitoramento e Observabilidade
- **Métricas Prometheus**: Exposição de métricas customizadas
- **Dashboards Grafana**: Visualização em tempo real
- **Alertas Automatizados**: Monitoramento de performance e qualidade
- **Logging Estruturado**: Rastreamento completo de operações

## 🛠 Stack Tecnológica

### Core Technologies
- **PySpark 3.5.0**: Framework de processamento distribuído
- **Spark MLlib**: Biblioteca de Machine Learning
- **Python 3.x**: Linguagem principal

### Monitoramento
- **Prometheus**: Coleta e armazenamento de métricas
- **Grafana**: Visualização e dashboards
- **prometheus-client**: Integração Python-Prometheus

### Processamento de Dados
- **Pandas**: Manipulação de dados complementar
- **NumPy**: Operações numéricas
- **PyArrow**: Otimização de I/O

### Visualização
- **Matplotlib**: Gráficos estáticos
- **Seaborn**: Visualizações estatísticas

## 📊 Dataset

**MovieLens 20M Dataset**:
- 📈 **20 milhões** de avaliações
- 🎬 **62.000** filmes únicos
- 👥 **162.000** usuários
- 📅 Dados coletados entre **janeiro de 1995** e **novembro de 2019**
- 🎭 **20 gêneros** diferentes de filmes

## 🚀 Guia de Instalação

### Pré-requisitos

1. **Python 3.8+** instalado
2. **Java 8+** (para Spark)
3. **Apache Spark** configurado
4. **Prometheus** rodando na porta 9090
5. **Grafana** rodando na porta 3000

### Instalação Rápida

```bash
# 1. Clone o repositório
git clone https://github.com/seu_usuario/spark-movie-analytics-grafana.git
cd spark-movie-analytics-grafana

# 2. Instale as dependências
pip install -r requirements.txt

# 3. Baixe o dataset MovieLens 20M
# Coloque os arquivos rating.csv e movie.csv na pasta datasets/

# 4. Execute o sistema
python pyspark_20m.py
```

### Configuração dos Serviços

#### Prometheus (prometheus.yml)
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'spark-analytics'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 5s
```

#### Grafana
- Acesse: http://localhost:3000
- Login: admin/admin
- O dashboard será criado automaticamente

## 📈 Métricas Monitoradas

### Métricas de Performance
- **spark_job_duration_seconds**: Duração das operações Spark
- **spark_records_processed_total**: Total de registros processados
- **spark_active_executors**: Número de executors ativos
- **spark_memory_usage_mb**: Uso de memória por componente

### Métricas de Qualidade
- **spark_data_quality_score**: Score de qualidade dos dados (0-1)
- **spark_ml_model_rmse**: RMSE do modelo de recomendação

### Métricas de Negócio
- **movies_popular_genres_count**: Distribuição por gêneros
- **movies_rating_distribution**: Distribuição de avaliações

## 🎯 Dashboards Disponíveis

Após a execução, você terá acesso a:

| Serviço | URL | Descrição |
|---------|-----|-----------|
| **Spark UI** | http://localhost:4040 | Interface do Spark |
| **Prometheus** | http://localhost:9090 | Métricas e queries |
| **Grafana Dashboard** | http://localhost:3000 | Visualização interativa |
| **Metrics Endpoint** | http://localhost:8000/metrics | Métricas Prometheus |

## 📊 Exemplo de Resultados

### Top 10 Filmes Mais Populares
```
🔥 TOP 10 FILMES MAIS POPULARES:
1. Pulp Fiction (1994) - 67,310 avaliações
2. Forrest Gump (1994) - 66,172 avaliações
3. The Shawshank Redemption (1994) - 63,366 avaliações
...
```

### Performance do Modelo
```
🎯 PERFORMANCE DO MODELO:
RMSE: 0.8234
Predições válidas: 4,782,456
Score de qualidade: 0.998
```

## 🔧 Configurações Avançadas

### Otimizações Spark
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.executor.memory", "4g")
```

### Personalização de Métricas
```python
# Adicionar novas métricas
custom_metric = Gauge('custom_business_metric', 'Descrição')
custom_metric.set(valor)
```

## 📝 Estrutura do Projeto

```
spark-movie-analytics-grafana/
├── README.md                   # Este arquivo
├── requirements.txt            # Dependências Python
├── pyspark_20m.py             # Script principal
├── datasets/                   # Dados do MovieLens
│   ├── rating.csv
│   └── movie.csv
├── logs/                       # Logs de execução
│   └── spark_analytics.log
└── dashboards/                 # Dashboards Grafana
    └── spark-dashboard.json
```

## 🔍 Monitoramento de Qualidade

O sistema implementa validação automática de qualidade dos dados:

- **Detecção de Nulos**: Identifica registros com valores ausentes
- **Detecção de Duplicatas**: Remove registros duplicados
- **Score de Qualidade**: Métrica consolidada (0-1) da qualidade dos dados
- **Alertas Automáticos**: Notificações quando a qualidade cai abaixo do threshold

## 🎓 Casos de Uso

### Data Engineering
- Pipeline de processamento de Big Data
- Monitoramento de qualidade de dados
- Otimização de performance em larga escala

### MLOps
- Treinamento e avaliação de modelos
- Monitoramento de performance de ML
- Deployment de modelos em produção

### Business Intelligence
- Análise de comportamento de usuários
- Insights de mercado cinematográfico
- Dashboards executivos

## 👥 Contribuição

Contribuições são bem-vindas! Para contribuir:

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

## 👨‍💻 Autor

**Eduardo Arruda Remião**
- LinkedIn: [Eduardo Remião](https://www.linkedin.com/in/eduardoremiao/)
- GitHub: [@eduardoremiao](https://github.com/duduremiao/)

## 📚 Referências e Recursos

- [MovieLens 20M Dataset](https://www.kaggle.com/datasets/grouplens/movielens-20m-dataset)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Prometheus Monitoring](https://prometheus.io/docs/)
- [Grafana Dashboards](https://grafana.com/docs/)
- [Collaborative Filtering with ALS](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html)


---

⭐ **Se este projeto foi útil, não se esqueça de dar uma estrela no GitHub!**
