"""
AI Keywords and Tech Stack Keywords for Pipeline 2
app/pipelines/keywords.py
"""

from __future__ import annotations

# AI-related keywords for job posting classification
AI_KEYWORDS = frozenset([
    # Core AI/ML terms
    "artificial intelligence",
    "machine learning",
    "deep learning",
    "neural network",
    "nlp",
    "natural language processing",
    "computer vision",
    "reinforcement learning",
    "generative ai",
    "large language model",
    "llm",
    "gpt",
    "transformer",

    # ML frameworks and tools
    "tensorflow",
    "pytorch",
    "keras",
    "scikit-learn",
    "sklearn",
    "hugging face",
    "langchain",
    "openai",
    "anthropic",

    # Data science
    "data science",
    "data scientist",
    "ml engineer",
    "machine learning engineer",
    "ai engineer",
    "mlops",
    "feature engineering",
    "model training",
    "model deployment",

    # Specific techniques
    "supervised learning",
    "unsupervised learning",
    "convolutional neural network",
    "cnn",
    "rnn",
    "lstm",
    "random forest",
    "gradient boosting",
    "xgboost",
    "recommendation system",
    "predictive analytics",
    "sentiment analysis",
    "text classification",
    "object detection",
    "image recognition",

    # AI roles
    "ai researcher",
    "ml researcher",
    "ai/ml",
    "prompt engineer",
    "ai specialist",
    "data architect",
])

# Tech stack keywords for digital presence scoring
AI_TECHSTACK_KEYWORDS = frozenset([
    # Cloud AI platforms
    "aws sagemaker",
    "azure ml",
    "google vertex ai",
    "databricks",
    "snowflake",
    "bigquery",
    "redshift",

    # ML infrastructure
    "kubernetes",
    "docker",
    "mlflow",
    "kubeflow",
    "airflow",
    "prefect",
    "dagster",

    # Data tools
    "spark",
    "pyspark",
    "hadoop",
    "kafka",
    "flink",
    "dbt",
    "fivetran",
    "airbyte",

    # Programming languages
    "python",
    "r programming",
    "julia",
    "scala",

    # Databases
    "postgresql",
    "mongodb",
    "elasticsearch",
    "redis",
    "neo4j",
    "pinecone",
    "weaviate",
    "milvus",

    # Visualization
    "tableau",
    "power bi",
    "looker",
    "metabase",

    # Version control / MLOps
    "git",
    "github",
    "gitlab",
    "dvc",
    "wandb",
    "neptune",

    # AI-specific infra
    "gpu cluster",
    "nvidia",
    "cuda",
    "ray",
    "dask",
])

# Leadership background keywords for DEF 14A parsing
AI_LEADERSHIP_KEYWORDS = frozenset([
    # Titles
    "chief data officer",
    "cdo",
    "chief analytics officer",
    "chief ai officer",
    "caio",
    "chief technology officer",
    "cto",
    "vp of data",
    "vp of ai",
    "vp of engineering",
    "head of data science",
    "head of ai",
    "head of ml",

    # Background indicators
    "data science background",
    "ai experience",
    "machine learning",
    "phd in computer science",
    "stanford ai",
    "mit ai",
    "google ai",
    "meta ai",
    "deepmind",
    "openai",
    "anthropic",

    # Education
    "computer science degree",
    "statistics degree",
    "mathematics degree",
    "engineering degree",
])

# Patent classification keywords for PatentsView
PATENT_AI_KEYWORDS = frozenset([
    "artificial intelligence",
    "machine learning",
    "neural network",
    "deep learning",
    "natural language processing",
    "computer vision",
    "pattern recognition",
    "automated decision",
    "predictive model",
    "classification algorithm",
    "clustering algorithm",
    "recommendation engine",
    "speech recognition",
    "image processing",
    "data mining",
    "knowledge graph",
])

# Top AI tools for bonus scoring in tech stack
TOP_AI_TOOLS = frozenset([
    "tensorflow",
    "pytorch",
    "kubernetes",
    "spark",
    "databricks",
    "aws sagemaker",
    "mlflow",
    "hugging face",
])
