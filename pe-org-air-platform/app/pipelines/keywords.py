"""
AI Keywords and Tech Stack Keywords for Pipeline 2
app/pipelines/keywords.py
"""

from __future__ import annotations

# AI-related keywords for job posting classification
AI_KEYWORDS = frozenset([
    # Core AI / ML terms
    "artificial intelligence",
    "machine learning",
    "deep learning",
    "neural network",
    "neural networks",
    "nlp",
    "natural language processing",
    "computer vision",
    "reinforcement learning",
    "generative ai",
    "gen ai",
    "foundation model",
    "foundation models",
    "multimodal",
    "self supervised learning",
    "self-supervised learning",

    # Models & architectures
    "large language model",
    "large language models",
    "llm",
    "llms",
    "gpt",
    "bert",
    "t5",
    "llama",
    "mistral",
    "claude",
    "transformer",
    "attention mechanism",
    "diffusion model",
    "stable diffusion",
    "gan",
    "generative adversarial network",
    "convolutional neural network",
    "cnn",
    "rnn",
    "lstm",

    # ML techniques
    "supervised learning",
    "unsupervised learning",
    "semi supervised learning",
    "transfer learning",
    "fine tuning",
    "fine-tuning",
    "hyperparameter tuning",
    "feature engineering",
    "model training",
    "model evaluation",
    "model deployment",
    "model serving",
    "model monitoring",
    "experiment tracking",

    # Libraries & frameworks
    "tensorflow",
    "pytorch",
    "keras",
    "scikit-learn",
    "sklearn",
    "hugging face",
    "transformers",
    "langchain",
    "llamaindex",
    "ray",
    "mlflow",
    "onnx",
    "fastai",

    # LLM / GenAI tooling
    "prompt engineering",
    "prompt engineer",
    "rag",
    "retrieval augmented generation",
    "vector database",
    "vector search",
    "embeddings",
    "semantic search",
    "faiss",
    "pinecone",
    "weaviate",
    "chroma",

    # Providers & platforms
    "openai",
    "anthropic",
    "cohere",
    "azure openai",
    "aws sagemaker",
    "google vertex ai",
    "bedrock",

    # Data science & analytics
    "data science",
    "data scientist",
    "predictive analytics",
    "statistical modeling",
    "time series forecasting",
    "anomaly detection",
    "recommendation system",
    "recommendation engine",

    # NLP / Vision tasks
    "sentiment analysis",
    "text classification",
    "named entity recognition",
    "ner",
    "topic modeling",
    "speech recognition",
    "speech to text",
    "object detection",
    "image recognition",
    "image classification",

    # ML roles & job titles
    "ml engineer",
    "machine learning engineer",
    "ai engineer",
    "mlops",
    "ml ops",
    "ai researcher",
    "ml researcher",
    "ai specialist",
    "applied scientist",
    "research scientist",
    "computer vision engineer",
    "nlp engineer",
    "data architect",

    # Ops & infra
    "ml pipeline",
    "training pipeline",
    "inference pipeline",
    "model registry",
    "feature store",
    "distributed training",
    "gpu",
    "cuda",

    # Shorthand / hybrid terms
    "ai",
    "ai/ml",
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
