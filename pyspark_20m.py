# -*- coding: utf-8 -*-
"""
Spark Data Engineering - Movie Analytics com Grafana Integration
"""

import findspark
findspark.init()

import os
import time
import logging
import json
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, desc, asc
from pyspark.sql.functions import split, explode, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, LongType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import pandas as pd
from prometheus_client import start_http_server, Gauge, Counter, Histogram, CollectorRegistry
import threading

# ==============================================================================
# CONFIGURAÇÃO DE LOGGING
# ==============================================================================

def setup_logging():
    """Configura logging para monitoramento de operações"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('spark_analytics.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ==============================================================================
# CLASSE DE MÉTRICAS PERSONALIZADAS PARA GRAFANA (SEM PUSHGATEWAY)
# ==============================================================================

class PrometheusMetricsServer:
    """Servidor HTTP para expor métricas diretamente ao Prometheus"""
    
    def __init__(self, port=8000):
        self.port = port
        self.registry = CollectorRegistry()
        
        # Métricas do Spark Job
        self.job_duration = Histogram(
            'spark_job_duration_seconds',
            'Duração das operações Spark',
            ['operation'],
            registry=self.registry
        )
        
        self.records_processed = Gauge(
            'spark_records_processed_total',
            'Total de registros processados',
            ['dataset'],
            registry=self.registry
        )
        
        self.data_quality_score = Gauge(
            'spark_data_quality_score',
            'Score de qualidade dos dados (0-1)',
            ['dataset'],
            registry=self.registry
        )
        
        self.model_rmse = Gauge(
            'spark_ml_model_rmse',
            'RMSE do modelo de recomendação',
            registry=self.registry
        )
        
        self.active_executors = Gauge(
            'spark_active_executors',
            'Número de executors ativos',
            registry=self.registry
        )
        
        self.memory_usage = Gauge(
            'spark_memory_usage_mb',
            'Uso de memória em MB',
            ['type'],
            registry=self.registry
        )
        
        # Métricas de negócio
        self.popular_genres = Gauge(
            'movies_popular_genres_count',
            'Contagem por gênero popular',
            ['genre'],
            registry=self.registry
        )
        
        self.rating_distribution = Gauge(
            'movies_rating_distribution',
            'Distribuição de ratings',
            ['rating'],
            registry=self.registry
        )
        
        # Iniciar servidor HTTP
        self.start_server()
    
    def start_server(self):
        """Inicia servidor HTTP para métricas"""
        try:
            start_http_server(self.port, registry=self.registry)
            logger.info(f"🚀 Servidor de métricas iniciado na porta {self.port}")
            logger.info(f"📊 Métricas disponíveis em: http://localhost:{self.port}/metrics")
        except Exception as e:
            logger.error(f"❌ Erro ao iniciar servidor de métricas: {e}")
    
    def record_operation_time(self, operation, duration):
        """Registra tempo de operação"""
        self.job_duration.labels(operation=operation).observe(duration)
    
    def set_records_processed(self, dataset, count):
        """Define contagem de registros processados"""
        self.records_processed.labels(dataset=dataset).set(count)
    
    def set_data_quality_score(self, dataset, score):
        """Define score de qualidade dos dados"""
        self.data_quality_score.labels(dataset=dataset).set(score)
    
    def set_model_rmse(self, rmse):
        """Define RMSE do modelo"""
        self.model_rmse.set(rmse)
    
    def set_cluster_metrics(self, executors, driver_memory, executor_memory):
        """Define métricas do cluster"""
        self.active_executors.set(executors)
        self.memory_usage.labels(type="driver").set(driver_memory)
        self.memory_usage.labels(type="executor").set(executor_memory)
    
    def set_business_metrics(self, genre_counts, rating_dist):
        """Define métricas de negócio"""
        # Top 10 gêneros
        for genre, count in genre_counts[:10]:
            self.popular_genres.labels(genre=genre).set(count)
        
        # Distribuição de ratings
        for rating, count in rating_dist:
            self.rating_distribution.labels(rating=str(rating)).set(count)

# ==============================================================================
# CONFIGURADOR DO GRAFANA
# ==============================================================================

class GrafanaSetup:
    """Configura dashboards automaticamente no Grafana"""
    
    def __init__(self, grafana_url="http://localhost:3000", username="admin", password="admin"):
        self.grafana_url = grafana_url
        self.auth = (username, password)
        self.headers = {'Content-Type': 'application/json'}
    
    def create_datasource(self):
        """Cria datasource Prometheus no Grafana"""
        datasource_config = {
            "name": "Prometheus-Spark",
            "type": "prometheus",
            "url": "http://localhost:9090",
            "access": "proxy",
            "isDefault": True
        }
        
        try:
            response = requests.post(
                f"{self.grafana_url}/api/datasources",
                json=datasource_config,
                auth=self.auth,
                headers=self.headers
            )
            if response.status_code in [200, 409]:  # 409 = already exists
                logger.info("✅ Datasource Prometheus configurado no Grafana")
            else:
                logger.warning(f"⚠️ Erro ao criar datasource: {response.text}")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao conectar com Grafana: {e}")
    
    def create_spark_dashboard(self):
        """Cria dashboard personalizado para Spark Analytics"""
        dashboard_json = {
            "dashboard": {
                "title": "Spark Movie Analytics Dashboard",
                "tags": ["spark", "analytics", "movies"],
                "timezone": "browser",
                "panels": [
                    {
                        "id": 1,
                        "title": "Duração das Operações",
                        "type": "graph",
                        "targets": [{
                            "expr": "spark_job_duration_seconds",
                            "legendFormat": "{{operation}}"
                        }],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
                    },
                    {
                        "id": 2,
                        "title": "Registros Processados",
                        "type": "stat",
                        "targets": [{
                            "expr": "spark_records_processed_total",
                            "legendFormat": "{{dataset}}"
                        }],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
                    },
                    {
                        "id": 3,
                        "title": "Qualidade dos Dados",
                        "type": "gauge",
                        "targets": [{
                            "expr": "spark_data_quality_score",
                            "legendFormat": "{{dataset}}"
                        }],
                        "gridPos": {"h": 8, "w": 6, "x": 0, "y": 8}
                    },
                    {
                        "id": 4,
                        "title": "RMSE do Modelo",
                        "type": "stat",
                        "targets": [{
                            "expr": "spark_ml_model_rmse"
                        }],
                        "gridPos": {"h": 8, "w": 6, "x": 6, "y": 8}
                    },
                    {
                        "id": 5,
                        "title": "Executors Ativos",
                        "type": "stat",
                        "targets": [{
                            "expr": "spark_active_executors"
                        }],
                        "gridPos": {"h": 8, "w": 6, "x": 12, "y": 8}
                    },
                    {
                        "id": 6,
                        "title": "Uso de Memória",
                        "type": "graph",
                        "targets": [{
                            "expr": "spark_memory_usage_mb",
                            "legendFormat": "{{type}}"
                        }],
                        "gridPos": {"h": 8, "w": 6, "x": 18, "y": 8}
                    },
                    {
                        "id": 7,
                        "title": "Top Gêneros",
                        "type": "bargauge",
                        "targets": [{
                            "expr": "movies_popular_genres_count",
                            "legendFormat": "{{genre}}"
                        }],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16}
                    },
                    {
                        "id": 8,
                        "title": "Distribuição de Ratings",
                        "type": "piechart",
                        "targets": [{
                            "expr": "movies_rating_distribution",
                            "legendFormat": "Rating {{rating}}"
                        }],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16}
                    }
                ],
                "time": {"from": "now-1h", "to": "now"},
                "refresh": "30s"
            }
        }
        
        try:
            response = requests.post(
                f"{self.grafana_url}/api/dashboards/db",
                json=dashboard_json,
                auth=self.auth,
                headers=self.headers
            )
            if response.status_code == 200:
                result = response.json()
                dashboard_url = f"{self.grafana_url}/d/{result['uid']}"
                logger.info(f"✅ Dashboard criado: {dashboard_url}")
                return dashboard_url
            else:
                logger.warning(f"⚠️ Erro ao criar dashboard: {response.text}")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao criar dashboard: {e}")
        
        return None

# ==============================================================================
# SPARK SESSION OTIMIZADA COM MÉTRICAS
# ==============================================================================

def create_spark_session():
    """Cria Spark Session com configurações otimizadas e métricas"""
    logger.info("Criando Spark Session...")

    spark = SparkSession.builder \
        .appName("MovieAnalytics-DataEngineering-Grafana") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"✅ Spark Session criada - Versão: {spark.version}")
    logger.info(f"🔗 Spark UI: {spark.sparkContext.uiWebUrl}")
    
    return spark

# ==============================================================================
# CLASSE PRINCIPAL COM INTEGRAÇÃO GRAFANA 
# ==============================================================================

def track_performance_with_metrics(operation_name):
    """Decorator para monitorar performance"""
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            logger.info(f"🚀 Iniciando: {operation_name}")

            try:
                result = func(self, *args, **kwargs)
                execution_time = time.time() - start_time

                self.performance_metrics[operation_name] = {
                    'time': execution_time,
                    'status': 'SUCCESS'
                }
                
                # Enviar para servidor de métricas
                self.metrics_server.record_operation_time(operation_name, execution_time)

                logger.info(f"✅ {operation_name} concluído em {execution_time:.2f}s")
                return result

            except Exception as e:
                execution_time = time.time() - start_time
                self.performance_metrics[operation_name] = {
                    'time': execution_time,
                    'status': 'ERROR',
                    'error': str(e)
                }
                logger.error(f"❌ Erro em {operation_name}: {str(e)}")
                raise

        return wrapper
    return decorator

class MovieAnalyticsWithGrafana:
    """Classe principal com integração completa ao Grafana (sem Pushgateway)"""
    
    def __init__(self):
        self.spark = create_spark_session()
        self.ratings_df = None
        self.movies_df = None
        self.performance_metrics = {}
        
        # Inicializar servidor de métricas e Grafana
        self.metrics_server = PrometheusMetricsServer(port=8000)
        self.grafana_setup = GrafanaSetup()
        
        # Configurar Grafana automaticamente
        self.setup_grafana()
        
    def setup_grafana(self):
        """Configura Grafana automaticamente"""
        logger.info("🔧 Configurando Grafana...")
        
        # Configurar datasource
        self.grafana_setup.create_datasource()
        
        # Criar dashboard
        dashboard_url = self.grafana_setup.create_spark_dashboard()
        if dashboard_url:
            self.dashboard_url = dashboard_url
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Mostrar informações finais
        print("\n" + "="*60)
        print("📊 ANÁLISE FINALIZADA - DASHBOARDS DISPONÍVEIS:")
        print("="*60)
        print(f"🔗 Spark UI: http://localhost:4040/")
        print(f"📊 Grafana Dashboard: {getattr(self, 'dashboard_url', 'http://localhost:3000')}")
        print(f"📈 Prometheus Metrics: http://localhost:8000/metrics")
        print(f"📈 Prometheus UI: http://localhost:9090")
        print("="*60)
        
        print("🔥 Dashboards rodando. Pressione ENTER para encerrar...")
        input()
        
        if self.spark:
            self.spark.stop()
            logger.info("🔄 Spark Session finalizada")

    @track_performance_with_metrics("load_data")
    def load_data(self, ratings_path, movies_path):
        """Carrega dados e atualiza métricas"""
        
        # Schema definitions
        ratings_schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("timestamp", LongType(), True)
        ])
        
        movies_schema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("genres", StringType(), True)
        ])
        
        # Load data
        self.ratings_df = self.spark.read.csv(
            ratings_path, schema=ratings_schema, header=True
        ).cache()
        
        self.movies_df = self.spark.read.csv(
            movies_path, schema=movies_schema, header=True
        ).cache()
        
        # Get counts and send to metrics server
        ratings_count = self.ratings_df.count()
        movies_count = self.movies_df.count()
        
        self.metrics_server.set_records_processed("ratings", ratings_count)
        self.metrics_server.set_records_processed("movies", movies_count)
        
        # Data quality validation
        quality_score = self.validate_data_quality()
        
        logger.info(f"📊 Ratings carregados: {ratings_count:,} registros")
        logger.info(f"🎬 Filmes carregados: {movies_count:,} registros")

    def validate_data_quality(self):
        """Valida qualidade e envia score para servidor de métricas"""
        logger.info("🔍 Validando qualidade dos dados...")
        
        # Check nulls
        total_ratings = self.ratings_df.count()
        null_ratings = self.ratings_df.filter(
            col("userId").isNull() | col("movieId").isNull() | col("rating").isNull()
        ).count()
        
        # Check duplicates
        unique_ratings = self.ratings_df.dropDuplicates().count()
        duplicates = total_ratings - unique_ratings
        
        # Calculate quality score (0-1)
        null_rate = null_ratings / total_ratings if total_ratings > 0 else 0
        duplicate_rate = duplicates / total_ratings if total_ratings > 0 else 0
        quality_score = max(0, 1 - null_rate - duplicate_rate)
        
        # Send to metrics server
        self.metrics_server.set_data_quality_score("ratings", quality_score)
        
        logger.info(f"📈 Score de qualidade: {quality_score:.3f}")
        logger.info(f"📈 Registros nulos: {null_ratings}")
        logger.info(f"📈 Duplicatas: {duplicates}")
        
        return quality_score

    @track_performance_with_metrics("basic_analysis")
    def perform_basic_analysis(self):
        """Análise básica com métricas"""
        logger.info("📊 Executando análise exploratória...")
        
        # Popular movies
        popular_movies = self.ratings_df.groupBy("movieId") \
            .count() \
            .join(self.movies_df, "movieId") \
            .orderBy(desc("count")) \
            .select("title", "count") \
            .limit(10)
        
        print("\n🔥 TOP 10 FILMES MAIS POPULARES:")
        print("-" * 50)
        popular_movies.show(truncate=False)
        
        # Best rated movies
        best_rated = self.ratings_df.groupBy("movieId") \
            .agg(count("rating").alias("num_reviews"), avg("rating").alias("avg_rating")) \
            .filter(col("num_reviews") >= 1000) \
            .join(self.movies_df, "movieId") \
            .orderBy(desc("avg_rating")) \
            .select("title", "avg_rating", "num_reviews") \
            .limit(10)
        
        print("\n⭐ TOP 10 FILMES MELHOR AVALIADOS (min. 1000 reviews):")
        print("-" * 60)
        best_rated.show(truncate=False)
        
        # Rating distribution
        rating_distribution = self.ratings_df.groupBy("rating") \
            .count() \
            .orderBy("rating") \
            .collect()
        
        rating_dist_list = [(row.rating, row['count']) for row in rating_distribution]

        print("\n📊 DISTRIBUIÇÃO DE RATINGS:")
        print("-" * 30)
        for rating, cnt in rating_dist_list:
            print(f"Rating {rating}: {cnt:,}")
        
        return rating_dist_list

    @track_performance_with_metrics("genre_analysis")
    def analyze_genres(self):
        """Análise de gêneros com métricas"""
        logger.info("🎭 Analisando distribuição por gêneros...")
        
        # Expand genres
        genres_expanded = self.movies_df.withColumn(
            "genre", explode(split(col("genres"), "\\|"))
        ).filter(col("genre") != "(no genres listed)")
        
        # Count by genre
        genre_counts = genres_expanded.groupBy("genre") \
            .count() \
            .orderBy(desc("count")) \
            .collect()
        
        genre_counts_list = [(row['genre'], row['count']) for row in genre_counts]
        
        print("\n🎭 DISTRIBUIÇÃO POR GÊNEROS:")
        print("-" * 40)
        for genre, cnt in genre_counts_list:
            print(f"{genre}: {cnt:,}")
        
        return genre_counts_list

    @track_performance_with_metrics("train_recommendation_model")
    def train_recommendation_model(self):
        """Treina modelo e envia RMSE para servidor de métricas"""
        logger.info("🤖 Treinando modelo de recomendação...")
        
        # Split data
        train_data, test_data = self.ratings_df.randomSplit([0.8, 0.2], seed=42)
        
        # Configure ALS
        als = ALS(
            maxIter=10, regParam=0.1, rank=10,
            userCol="userId", itemCol="movieId", ratingCol="rating",
            coldStartStrategy="drop"
        )
        
        # Train model
        model = als.fit(train_data)
        
        # Evaluate model
        predictions = model.transform(test_data)
        predictions_clean = predictions.filter(~isnan(col("prediction")))
        
        evaluator = RegressionEvaluator(
            metricName="rmse", labelCol="rating", predictionCol="prediction"
        )
        
        rmse = evaluator.evaluate(predictions_clean)
        
        # Send RMSE to metrics server
        self.metrics_server.set_model_rmse(rmse)
        
        print(f"\n🎯 PERFORMANCE DO MODELO:")
        print("-" * 30)
        print(f"RMSE: {rmse:.4f}")
        print(f"Predições válidas: {predictions_clean.count():,}")
        
        return model, rmse

    def update_cluster_metrics(self):
        """Atualiza métricas do cluster"""
        sc = self.spark.sparkContext
        
        # Get cluster info
        default_parallelism = sc.defaultParallelism
        driver_memory = int(sc.getConf().get('spark.driver.memory', '1g').replace('g', '')) * 1024
        executor_memory = int(sc.getConf().get('spark.executor.memory', '1g').replace('g', '')) * 1024
        
        # Send to metrics server
        self.metrics_server.set_cluster_metrics(
            executors=default_parallelism,
            driver_memory=driver_memory,
            executor_memory=executor_memory
        )

    def run_complete_analysis_with_grafana(self, ratings_path, movies_path):
        """Execução completa com integração Grafana"""
        logger.info("🚀 Iniciando análise completa com Grafana...")
        
        try:
            # 1. Load data
            self.load_data(ratings_path, movies_path)
            
            # 2. Update cluster metrics
            self.update_cluster_metrics()
            
            # 3. Basic analysis
            rating_dist = self.perform_basic_analysis()
            
            # 4. Genre analysis
            genre_counts = self.analyze_genres()
            
            # 5. Train model
            model, rmse = self.train_recommendation_model()
            
            # 6. Send business metrics to server
            self.metrics_server.set_business_metrics(genre_counts, rating_dist)
            
            print("\n🎉 ANÁLISE COMPLETA FINALIZADA COM GRAFANA!")
            print(f"🎯 RMSE do modelo: {rmse:.4f}")
            print(f"📊 Métricas disponíveis no servidor local")
            
        except Exception as e:
            logger.error(f"❌ Erro na análise: {str(e)}")
            raise

# ==============================================================================
# FUNÇÃO PRINCIPAL
# ==============================================================================

def main():
    """Função principal com integração Grafana"""
    
    print("🎬 Spark Data Engineering - Movie Analytics + Grafana")
    print("="*60)
    print("📋 PRÉ-REQUISITOS:")
    print("- Prometheus rodando na porta 9090")
    print("- Grafana rodando na porta 3000 (admin/admin)")
    print("- Servidor de métricas será iniciado na porta 8000")
    print("="*60)
    
    # Data paths
    ratings_path = "datasets/rating.csv"
    movies_path = "datasets/movie.csv"
    
    try:
        with MovieAnalyticsWithGrafana() as analytics:
            analytics.run_complete_analysis_with_grafana(ratings_path, movies_path)
            
    except FileNotFoundError:
        print("❌ Arquivos de dados não encontrados!")
        print("📝 Baixe o dataset MovieLens e coloque na pasta 'datasets/'")
        
    except Exception as e:
        logger.error(f"❌ Erro na execução: {str(e)}")
        raise

if __name__ == "__main__":
    main()
