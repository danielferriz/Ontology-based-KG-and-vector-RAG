# Ontology-based KG and vector RAG


## Additional Dependencies

- [Python 3.10](https://www.python.org/downloads/release/python-3100/) Version 3.10.0
- [LM Studio](https://lmstudio.ai/download) Version 0.3.13 
- [Memgraph](https://memgraph.com/docs/getting-started/install-memgraph) Version 3.0.1
- [PostgreSQL](https://www.postgresql.org/download/) Version 16.7
- [pgvector extension for PostgreSQL](https://github.com/pgvector/pgvector) Version 0.8.0
- [Damage Ontology Topology](https://alhakam.github.io/dot/) Version 0.8

## Installation instructions

1. Read the README.md file
2. Install Python dependencies using the requirements.txt file
3. Install and configure [LM Studio](https://lmstudio.ai/download) 
    1. Follow the [installation guide] (https://lmstudio.ai/docs/app)
    2. Download the text embedding model [granite-embedding-278m-multilingual-GGUF/granite-embedding-278m-multilingual-Q4_K_M.gguf with Quant Q4_K_M](https://huggingface.co/lmstudio-community/granite-embedding-278m-multilingual-GGUF)
    3. Download the LLM [deepseek-coder-v2-lite-instruct-mlx with Quant 4bit](https://huggingface.co/mlx-community/DeepSeek-Coder-V2-Lite-Instruct-4bit-mlx) 
    4. Set the context length for LLM to be 8020 in order to support up to 163840 tokens. **Note:** If this value is changed, then the config.yaml file should be updated to the selected context length
4. Install [Memgraph](https://memgraph.com/docs/getting-started/install-memgraph)
    * Within the installation directory of Memgraph, validate that the file /etc/memgraph/memgraph.conf has the option ```--schema-info-enabled``` set to ```True```. If the option ```--schema-info-enabled``` is set to ```False```, or if it doesn't exist in the configuration file, please adjust it and restart the Memgraph service. For more information, please visit the [Memgraph documentation site](https://memgraph.com/docs/database-management/configuration)
5. Install [PostgreSQL](https://www.postgresql.org/download/)
    * Install [pgvector extension for PostgreSQL](https://github.com/pgvector/pgvector)
6. Download the [Damage Ontology Topology](https://alhakam.github.io/dot/) xml file
7. Modify the content of the ```config.yaml``` file to match the files locations and the settings of the additional dependencies
8. Run ```python main.py -h``` 

## Pydoc
This project is fully compatible with Python's ```pydoc``` documentation system, enabling clear and structured access to the underlying codebase. The detailed, low-level implementation of the proposed solution can be explored directly through the generated documentation



