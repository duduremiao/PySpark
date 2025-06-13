# -*- coding: utf-8 -*-
"""
Spark Data Engineering - Movie Analytics
Demonstração de competências para Engenharia de Dados com foco em:
- Distributed Computing
- Spark Optimization
- Performance Monitoring
- Data Quality
- Basic ML Pipeline

Autor: Eduardo Arruda Remião
Versão: 1.0 - Production Ready
"""

import findspark
findspark.init()

import os
import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, desc, asc
from pyspark.sql.functions import split, explode, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, LongType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import pandas as pd

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
# SPARK SESSION OTIMIZADA
# ==============================================================================

def create_spark_session():
    """Cria Spark Session com configurações otimizadas para produção e monitoramento"""
    logger.info("Criando Spark Session...")

    metrics_agent_path = "./metrics/jmx_prometheus_javaagent-0.20.0.jar"
    metrics_config_path = "./metrics/spark_metrics.yaml"
    metrics_port = "7071"

    spark = SparkSession.builder \
        .appName("MovieAnalytics-DataEngineering") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.extraJavaOptions", 
                f"-javaagent:{metrics_agent_path}={metrics_port}:{metrics_config_path}") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"✅ Spark Session criada - Versão: {spark.version}")
    logger.info(f"🔗 Spark UI: {spark.sparkContext.uiWebUrl}")
    logger.info(f"📡 Métricas expostas para Prometheus em http://localhost:{metrics_port}/metrics")

    return spark

# ==============================================================================
# CLASSE PRINCIPAL DE ANÁLISE
# ==============================================================================

import time

def track_performance(operation_name):
    """Decorator para monitorar performance de operações"""
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

class MovieAnalytics:
    """Classe principal para análise de dados com Spark"""
    
    def __init__(self):
        self.spark = create_spark_session()
        self.ratings_df = None
        self.movies_df = None
        self.performance_metrics = {}
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        print("🔥 Spark rodando. Pressione ENTER para encerrar...")
        
        input()  # Aguarda a entrada do usuário
        
        if self.spark:
            self.spark.stop()
            logger.info("🔄 Spark Session finalizada")



    @track_performance("load_data")
    def load_data(self, ratings_path, movies_path):
        """Carrega dados com schema otimizado e validação"""
        
        # Definir schemas para melhor performance
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
        
        # Carregar com schema definido
        self.ratings_df = self.spark.read.csv(
            ratings_path, 
            schema=ratings_schema, 
            header=True
        ).cache()  # Cache para reutilização
        
        self.movies_df = self.spark.read.csv(
            movies_path, 
            schema=movies_schema, 
            header=True
        ).cache()
        
        # Validação básica de qualidade
        self.validate_data_quality()
        
        logger.info(f"📊 Ratings carregados: {self.ratings_df.count():,} registros")
        logger.info(f"🎬 Filmes carregados: {self.movies_df.count():,} registros")

    def validate_data_quality(self):
        """Valida qualidade dos dados carregados"""
        logger.info("🔍 Validando qualidade dos dados...")
        
        # Verificar valores nulos em ratings
        null_ratings = self.ratings_df.filter(
            col("userId").isNull() | 
            col("movieId").isNull() | 
            col("rating").isNull()
        ).count()
        
        # Verificar duplicatas
        total_ratings = self.ratings_df.count()
        unique_ratings = self.ratings_df.dropDuplicates().count()
        
        # Verificar range de ratings
        rating_stats = self.ratings_df.select("rating").describe().collect()
        
        logger.info(f"📈 Registros nulos: {null_ratings}")
        logger.info(f"📈 Duplicatas: {total_ratings - unique_ratings}")
        logger.info(f"📈 Range de ratings: {rating_stats}")

    @track_performance("basic_analysis")
    def perform_basic_analysis(self):
        """Análise exploratória básica com otimizações"""
        logger.info("📊 Executando análise exploratória...")
        
        # 1. Filmes mais populares (mais avaliados)
        popular_movies = self.ratings_df.groupBy("movieId") \
            .count() \
            .join(self.movies_df, "movieId") \
            .orderBy(desc("count")) \
            .select("title", "count") \
            .limit(10)
        
        print("\n🔥 TOP 10 FILMES MAIS POPULARES:")
        print("-" * 50)
        popular_movies.show(truncate=False)
        
        # 2. Filmes com melhores avaliações (min 1000 reviews)
        best_rated = self.ratings_df.groupBy("movieId") \
            .agg(
                count("rating").alias("num_reviews"),
                avg("rating").alias("avg_rating")
            ) \
            .filter(col("num_reviews") >= 1000) \
            .join(self.movies_df, "movieId") \
            .orderBy(desc("avg_rating")) \
            .select("title", "avg_rating", "num_reviews") \
            .limit(10)
        
        print("\n⭐ TOP 10 FILMES MELHOR AVALIADOS (min. 1000 reviews):")
        print("-" * 60)
        best_rated.show(truncate=False)
        
        # 3. Distribuição de ratings
        rating_distribution = self.ratings_df.groupBy("rating") \
            .count() \
            .orderBy("rating")
        
        print("\n📊 DISTRIBUIÇÃO DE RATINGS:")
        print("-" * 30)
        rating_distribution.show()
        
        return {
            'popular_movies': popular_movies,
            'best_rated': best_rated,
            'rating_distribution': rating_distribution
        }

    @track_performance("genre_analysis")
    def analyze_genres(self):
        """Análise por gêneros usando transformações Spark"""
        logger.info("🎭 Analisando distribuição por gêneros...")
        
        # Expandir gêneros (cada filme pode ter múltiplos gêneros)
        genres_expanded = self.movies_df.withColumn(
            "genre", explode(split(col("genres"), "\\|"))
        ).filter(col("genre") != "(no genres listed)")
        
        # Contar filmes por gênero
        genre_counts = genres_expanded.groupBy("genre") \
            .count() \
            .orderBy(desc("count"))
        
        print("\n🎭 DISTRIBUIÇÃO POR GÊNEROS:")
        print("-" * 40)
        genre_counts.show(20, truncate=False)
        
        # Gêneros mais bem avaliados
        genre_ratings = genres_expanded.join(self.ratings_df, "movieId") \
            .groupBy("genre") \
            .agg(
                count("rating").alias("total_ratings"),
                avg("rating").alias("avg_rating")
            ) \
            .filter(col("total_ratings") >= 10000) \
            .orderBy(desc("avg_rating"))
        
        print("\n🌟 GÊNEROS MELHOR AVALIADOS (min. 10k ratings):")
        print("-" * 50)
        genre_ratings.show(truncate=False)
        
        return genre_counts

    @track_performance("train_recommendation_model")
    def train_recommendation_model(self):
        """Treina modelo de recomendação com ALS"""
        logger.info("🤖 Treinando modelo de recomendação...")
        
        # Dividir dados em treino e teste
        train_data, test_data = self.ratings_df.randomSplit([0.8, 0.2], seed=42)
        
        # Configurar ALS (Alternating Least Squares)
        als = ALS(
            maxIter=10,
            regParam=0.1,
            rank=10,
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            coldStartStrategy="drop"  # Lidar com usuários/filmes não vistos
        )
        
        # Treinar modelo
        model = als.fit(train_data)
        
        # Avaliar modelo
        predictions = model.transform(test_data)
        predictions_clean = predictions.filter(~isnan(col("prediction")))
        
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        
        rmse = evaluator.evaluate(predictions_clean)
        
        print(f"\n🎯 PERFORMANCE DO MODELO:")
        print("-" * 30)
        print(f"RMSE: {rmse:.4f}")
        print(f"Predições válidas: {predictions_clean.count():,}")
        
        # Gerar recomendações para usuário exemplo
        user_recommendations = model.recommendForAllUsers(10) \
            .filter(col("userId") == 1)
        
        if user_recommendations.count() > 0:
            # Expandir recomendações
            recs_expanded = user_recommendations.select(
                "userId",
                explode("recommendations").alias("recommendation")
            ).select(
                "userId",
                col("recommendation.movieId").alias("movieId"),
                col("recommendation.rating").alias("predicted_rating")
            )
            
            # Adicionar títulos
            recs_with_titles = recs_expanded.join(self.movies_df, "movieId") \
                .select("title", "predicted_rating", "genres") \
                .orderBy(desc("predicted_rating"))
            
            print("\n🎬 RECOMENDAÇÕES PARA USUÁRIO 1:")
            print("-" * 50)
            recs_with_titles.show(10, truncate=False)
        
        return model, rmse

    def create_performance_dashboard(self):
        """Cria dashboard de performance das operações"""
        logger.info("📈 Criando dashboard de performance...")
        
        # Dados de performance
        operations = list(self.performance_metrics.keys())
        times = [self.performance_metrics[op]['time'] for op in operations]
        statuses = [self.performance_metrics[op]['status'] for op in operations]
        
        # Criar gráfico
        plt.figure(figsize=(12, 6))
        
        # Subplot 1: Tempos de execução
        plt.subplot(1, 2, 1)
        colors = ['green' if s == 'SUCCESS' else 'red' for s in statuses]
        bars = plt.bar(range(len(operations)), times, color=colors, alpha=0.7)
        plt.title('Tempo de Execução por Operação')
        plt.xlabel('Operações')
        plt.ylabel('Tempo (segundos)')
        plt.xticks(range(len(operations)), operations, rotation=45)
        
        # Adicionar valores nas barras
        for bar, time_val in zip(bars, times):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f'{time_val:.2f}s', ha='center', va='bottom')
    
        # Subplot 2: Resumo de status
        plt.subplot(1, 2, 2)
        status_counts = {'SUCCESS': statuses.count('SUCCESS'), 
                        'ERROR': statuses.count('ERROR')}
        plt.pie(status_counts.values(), labels=status_counts.keys(), 
                autopct='%1.1f%%', colors=['green', 'red'])
        plt.title('Status das Operações')
        
        plt.tight_layout()
        plt.savefig('performance_dashboard.png', dpi=150, bbox_inches='tight')    

    def get_cluster_info(self):
        """Obtém informações do cluster Spark"""
        sc = self.spark.sparkContext
        
        cluster_info = {
            'app_name': sc.appName,
            'spark_version': self.spark.version,
            'master': sc.master,
            'executor_memory': sc.getConf().get('spark.executor.memory', 'default'),
            'driver_memory': sc.getConf().get('spark.driver.memory', 'default'),
            'total_cores': sc.defaultParallelism
        }
        
        print("\n🖥️  INFORMAÇÕES DO CLUSTER:")
        print("-" * 40)
        for key, value in cluster_info.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        
        return cluster_info

    def print_performance_summary(self):
        """Imprime resumo final de performance"""
        print("\n" + "="*60)
        print("📊 RESUMO DE PERFORMANCE")
        print("="*60)
        
        total_time = sum(m['time'] for m in self.performance_metrics.values())
        successful_ops = sum(1 for m in self.performance_metrics.values() if m['status'] == 'SUCCESS')
        
        for operation, metrics in self.performance_metrics.items():
            status_icon = "✅" if metrics['status'] == 'SUCCESS' else "❌"
            print(f"{status_icon} {operation}: {metrics['time']:.2f}s")
        
        print(f"\n⏱️  Tempo total: {total_time:.2f}s")
        print(f"✅ Operações bem-sucedidas: {successful_ops}/{len(self.performance_metrics)}")
        print("="*60)

    def run_complete_analysis(self, ratings_path, movies_path):
        """Executa análise completa com monitoramento"""
        logger.info("🚀 Iniciando análise completa...")
        
        try:
            # 1. Carregar dados
            self.load_data(ratings_path, movies_path)
            
            # 2. Informações do cluster
            self.get_cluster_info()
            
            # 3. Análise básica
            analysis_results = self.perform_basic_analysis()
            
            # 4. Análise de gêneros
            self.analyze_genres()
            
            # 5. Modelo de recomendação
            model, rmse = self.train_recommendation_model()
            
            # 6. Dashboard de performance
            self.create_performance_dashboard()
            
            # 7. Resumo final
            self.print_performance_summary()
            
            print("\n🎉 ANÁLISE COMPLETA FINALIZADA!")
            print(f"🎯 RMSE do modelo: {rmse:.4f}")
            
        except Exception as e:
            logger.error(f"❌ Erro na análise: {str(e)}")
            raise

# ==============================================================================
# FUNÇÃO PRINCIPAL E EXEMPLO DE USO
# ==============================================================================

def main():
    """Função principal demonstrando uso da classe"""
    
    print("🎬 Spark Data Engineering - Movie Analytics")
    print("="*50)
    
    # Caminhos dos dados (ajustar conforme necessário)
    ratings_path = "datasets/rating.csv"  # ou sua pasta de dados
    movies_path = "datasets/movie.csv"
    
    try:
        # Usar context manager para garantir limpeza de recursos
        with MovieAnalytics() as analytics:
            analytics.run_complete_analysis(ratings_path, movies_path)
            
    except FileNotFoundError:
        print("❌ Arquivos de dados não encontrados!")
        print("📝 Instruções:")
        print("1. Baixe o dataset MovieLens do Kaggle")
        print("2. Extraia os arquivos rating.csv e movie.csv")
        print("3. Coloque na pasta 'datasets/'")
        print("4. Execute novamente")
        
    except Exception as e:
        logger.error(f"❌ Erro na execução: {str(e)}")
        raise

if __name__ == "__main__":
    main()

# ==============================================================================
# GUIA RÁPIDO DE TROUBLESHOOTING
# ==============================================================================

def troubleshooting_tips():
    """Dicas de troubleshooting para problemas comuns"""
    
    tips = """
    🔧 TROUBLESHOOTING GUIDE
    
    PROBLEMA: OutOfMemoryError
    ✅ SOLUÇÃO: Aumentar driver/executor memory ou reduzir partições
    
    PROBLEMA: Slow Performance  
    ✅ SOLUÇÃO: Usar cache(), verificar shuffle operations, otimizar joins
    
    PROBLEMA: Skewed Partitions
    ✅ SOLUÇÃO: Habilitar adaptive query execution, usar repartition()
    
    PROBLEMA: Cold Start no ALS
    ✅ SOLUÇÃO: Usar coldStartStrategy="drop"
    
    📋 MONITORAMENTO:
    - Spark UI: http://localhost:4040
    - Logs: spark_analytics.log
    - Métricas: Performance dashboard
    """
    
    print(tips)

# Executar dicas se chamado diretamente
if __name__ == "__main__" and len(os.sys.argv) > 1 and os.sys.argv[1] == "--help":
    troubleshooting_tips()