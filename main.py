import argparse
import yaml
from pathlib import Path
from re import sub
from math import ceil

from base_logger import logger
from firstread import graph_from_pdf_directory, read_ontology
from interactions import chat_loop_vector_questions, chat_loop_graph_questions, chat_loop
from tools import clean_node_metadata, remove_special_chars_in_llm_output, get_local_name
from rdf_interface import search_rdf_classes_objects
from postgresql import  create_connection, create_insert_prompt_tables

from langchain.text_splitter import RecursiveCharacterTextSplitter


import os
import sys


from langchain_community.graphs import MemgraphGraph


def local_parser():
    """
    Program display
    """
    parser = argparse.ArgumentParser(
        description='RAG application using LM Studio for LLM management and Memgraph for KG management'
        )
    parser.add_argument("-b", "--build-rag", action='store_true', help="Uses connection to Memgraph defined in .yaml file to build rag")
    parser.add_argument("-u", "--update-table", action='store_true', help="Update values of PostgreSQL database using files declared in .yaml file")
    parser.add_argument("-v", "--vector-chat", action='store_true', help="Chat with the vector dataset through LLM")
    parser.add_argument("-g", "--graph-chat", action='store_true', help="Chat with the Knowledge Graph through LLM. Compatible with --ontology")
    parser.add_argument("-o", "--ontology", action='store_true', help="Incorporates ontolgy when creating knowledge graph")
    parser.add_argument("-c", "--chat", action='store_true', help="*Experimental* Chat with both Knowledge Graph and Vector Dataset through LLM. Compatible with --ontology")
    return parser


def load_config():
    """
    Reads content of yaml file

    Returns:
        dict (config): Options set in config.yaml file
    """
    try:
        config_filepath = Path(__file__).absolute().resolve().parent / "config.yaml"
        with config_filepath.open() as f:
            config_dict = yaml.load(f, Loader=yaml.FullLoader)
        config = argparse.Namespace()
        for key, value in config_dict.items():
            setattr(config, key, value)
        return config
    except Exception as ex:
        print( f"An error occurred: {ex}")
        return None

def check_file_existance(filename:str, fileTypes:list[str]) -> tuple[int, str]:
    """
    Validates if file exists and is of valid type

    Params:
        str (filename): Filename
        list (fileTypes): Valid file types

    Returns:
        tuple (t): Tuple that shows success or failure
    """
    if not os.path.exists(filename):
        return 1, "File does not exist"

    for fileType in fileTypes:
        if filename.lower().endswith(fileType.lower()):
            return 0, "Success"

    return 2, f"File is not a valid type: {fileTypes}"


# Validates configuration. 
# [1, 100] for warnings
# >100 if there are crtitical flaws
def validate_config(config):
    """
    Validation of local config.yaml file

    Params:
        dict (config): Configuration dictionary using values from .yaml file

    Returns:
        bool (shouldTerminate): Determines if the process should terminate due to errors in config file
    """
    validations={}
    # ERRORS
    if config is None:
        validations[101]='Configuration file wasn\'t initilized properly. '
        return validations
    if not isinstance(config, argparse.Namespace):
        validations[102]='Configuration was initilized as type: '+str(type(config))
        return validations

    if not hasattr(config, 'chunk_overlap_ratio'):
        validations[103]='Parameter "chunk_overlap_ratio" not found. '
    elif not isinstance(config.chunk_overlap_ratio, float) or config.chunk_overlap_ratio<=0 or config.chunk_overlap_ratio>=1:
        validations[104]='Parameter "chunk_overlap_ratio" can only be an FLOAT in range [ 1, 99 ] '
    if not hasattr(config, 'llm_max_tokens'):
        validations[105]='Parameter "llm_max_tokens" not found. '
    elif not isinstance(config.llm_max_tokens, int) or config.llm_max_tokens<=0:
        validations[106]='Parameter "llm_max_tokens" can only be an INTEGER greater than zero. '
    if not hasattr(config, 'llm_tokens_per_100_characters'):
        validations[107]='Parameter "llm_tokens_per_100_characters" not found. '
    elif not isinstance(config.llm_tokens_per_100_characters, int) or config.llm_tokens_per_100_characters<=0 or config.llm_tokens_per_100_characters>100:
        validations[108]='Parameter "llm_tokens_per_100_characters" can only be an INTEGER in range [ 1, 100 ] '
    if not hasattr(config, 'llm_len_prompt_engineering'):
        validations[109]='Parameter "llm_len_prompt_engineering" not found. '
    elif not isinstance(config.llm_len_prompt_engineering, int) or config.llm_len_prompt_engineering<=0:
        validations[110]='Parameter "llm_len_prompt_engineering" can only be an INTEGER greater than zero '
    check_consistency_in_llm_variables=True
    for i in range(105, 111):
        if i in validations.keys():
            check_consistency_in_llm_variables=False
    if check_consistency_in_llm_variables:
        tokens_in_prompt = ceil(config.llm_len_prompt_engineering * config.llm_tokens_per_100_characters / 100)
        if tokens_in_prompt > config.llm_max_tokens:
            validations[111]=f'Parameter "llm_len_prompt_engineering" uses {tokens_in_prompt} tokens, however {config.llm_max_tokens} is configured to be the maximum'
    if not hasattr(config, 'k_most_similar'):
        validations[112]='Parameter "k_most_similar" not found. '
    elif not isinstance(config.k_most_similar, int) or config.k_most_similar<=0:
        validations[113]='Parameter "k_most_similar" can only be an INTEGER greater than zero. '
    check_consistency_in_llm_variables=True
    for i in range(105, 114):
        if i in validations.keys():
            check_consistency_in_llm_variables=False
    if check_consistency_in_llm_variables:
        tokens_in_prompt = ceil(config.llm_len_prompt_engineering * config.llm_tokens_per_100_characters / 100)
        tokens_per_k=int(  (config.llm_max_tokens - tokens_in_prompt )/ config.k_most_similar )
        chars_per_k= int(  tokens_per_k * 100 / config.llm_tokens_per_100_characters  )
        if chars_per_k < 100:
            validations[114]='Parameter "k_most_similar" has an extremely high value, which will not allow LLM interactions. '
        elif chars_per_k < 500:
            validations[5]='Parameter "k_most_similar" has a high value, which will negatively affect LLM interactions. '
    if not hasattr(config, 'memgraph_socket'):
        validations[115]='Parameter "memgraph_socket" not found. '
    elif not isinstance(config.memgraph_socket, str) or ':' not in config.memgraph_socket:
        validations[116]='Parameter "memgraph_socket" MUST be a string with a string with a ":" character. '
    if not hasattr(config, 'memgraph_user'):
        validations[117]='Parameter "memgraph_user" not found. '
    if not hasattr(config, 'memgraph_password'):
        validations[118]='Parameter "memgraph_password" not found. '
    if not hasattr(config, 'pdf_folder_path'):
        validations[119]='Parameter "pdf_folder_path" not found. '
    elif not isinstance(config.pdf_folder_path, str) or not config.pdf_folder_path:
        validations[120]='Parameter "pdf_folder_path" MUST be a non-empty string. '
    if not hasattr(config, 'llm_embedding_model'):
        validations[121]='Parameter "llm_embedding_model" not found. '
    elif not isinstance(config.llm_embedding_model, str) or not config.llm_embedding_model:
        validations[122]='Parameter "llm_embedding_model" MUST be a non-empty string. '
    if not hasattr(config, 'llm_embedding_url'):
        validations[123]='Parameter "llm_embedding_url" not found. '
    elif not isinstance(config.llm_embedding_url, str) or not config.llm_embedding_url:
        validations[124]='Parameter "llm_embedding_url" MUST be a non-empty string. '
    if not hasattr(config, 'llm_embedding_vector_len'):
        validations[125]='Parameter "llm_embedding_vector_len" not found. '
    elif not isinstance(config.llm_embedding_vector_len, int) or config.llm_embedding_vector_len<=0:
        validations[126]='Parameter "llm_embedding_vector_len" can only be an INTEGER greater than zero. '
    if not hasattr(config, 'llm_embedding_context_len'):
        validations[127]='Parameter "llm_embedding_context_len" not found. '
    elif not isinstance(config.llm_embedding_context_len, int):
        validations[128]='Parameter "llm_embedding_context_len" can only be an INTEGER'
    if not hasattr(config, 'llm_chat_model'):
        validations[129]='Parameter "llm_chat_model" not found. '
    elif not isinstance(config.llm_chat_model, str) or not config.llm_chat_model:
        validations[130]='Parameter "llm_chat_model" MUST be a non-empty string. '
    if not hasattr(config, 'llm_chat_url'):
        validations[131]='Parameter "llm_chat_url" not found. '
    elif not isinstance(config.llm_chat_url, str) or not config.llm_chat_url:
        validations[132]='Parameter "llm_chat_url" MUST be a non-empty string. '
    if not hasattr(config, 'db_name'):
        validations[133]='Parameter "db_name" not found. '
    elif not isinstance(config.db_name, str) or not config.db_name:
        validations[134]='Parameter "db_name" MUST be a non-empty string. '
    if not hasattr(config, 'db_user'):
        validations[135]='Parameter "db_user" not found. '
    elif not isinstance(config.db_user, str) or not config.db_user:
        validations[136]='Parameter "db_user" MUST be a non-empty string. '
    if not hasattr(config, 'db_password'):
        validations[137]='Parameter "db_password" not found. '
    elif not isinstance(config.db_password, str) or not config.db_password:
        validations[138]='Parameter "db_password" MUST be a non-empty string. '
    if not hasattr(config, 'db_host'):
        validations[139]='Parameter "db_host" not found. '
    elif not isinstance(config.db_host, str) or not config.db_host:
        validations[140]='Parameter "db_host" MUST be a non-empty string. '
    if not hasattr(config, 'db_port'):
        validations[141]='Parameter "db_port" not found. '
    elif not isinstance(config.db_port, str) or not config.db_port:
        validations[142]='Parameter "db_port" MUST be a non-empty string. '
    if not hasattr(config, 'prompts_xlsx'):
        validations[143]='Parameter "prompts_xlsx" not found. '
    elif not isinstance(config.prompts_xlsx, str) or not config.prompts_xlsx:
        validations[144]='Parameter "prompts_xlsx" MUST be a non-empty string. '
    else:
        errnum, errmes = check_file_existance(config.prompts_xlsx, [".xls", ".xlsx"])
        if errnum > 0:
            validations[144+errnum]=errmes
    if not hasattr(config, 'examples_xlsx'):
        validations[147]='Parameter "examples_xlsx" not found. '
    elif not isinstance(config.examples_xlsx, str) or not config.examples_xlsx:
        validations[148]='Parameter "examples_xlsx" MUST be a non-empty string. '
    else:
        errnum, errmes = check_file_existance(config.examples_xlsx, [".xls", ".xlsx"])
        if errnum > 0:
            validations[148+errnum]=errmes
    if not hasattr(config, 'createTables_sql'):
        validations[151]='Parameter "createTables_sql" not found. '
    elif not isinstance(config.createTables_sql, str) or not config.createTables_sql:
        validations[152]='Parameter "createTables_sql" MUST be a non-empty string. '
    else:
        errnum, errmes = check_file_existance(config.createTables_sql, [".sql"])
        if errnum > 0:
            validations[152+errnum]=errmes
    if not hasattr(config, 'language'):
        validations[155]='Parameter "language" not found. '
    elif not isinstance(config.language, str) or not config.language:
        validations[156]='Parameter "language" MUST be a non-empty string. '
    else:
        config.language=config.language.replace("'",'').replace('"','').lower()[:3]
        if config.language=='eng':
            config.language='en'
        if config.language=='fra' or config.language=='fre':
            config.language='fr'
        if config.language not in ['en','fr']:
            validations[157]='Parameter "language" can ONLY by either "en" or "fr". '
    # WARNINGS
    if not hasattr(config, 'logging_level'):
        validations[1]='Parameter "logging_level" not found. Defaulting logging level to INFO'
    elif config.logging_level not in [ "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL" ]:
        validations[2]='Logging level not found in list [ DEBUG, INFO, WARNING, ERROR, CRITICAL ]. Defaulting logging level to INFO'
    if not hasattr(config, 'log_file'):
        validations[3]='Parameter "log_file" not found. Defaulting name to "newLog"'
    elif hasattr(config, 'log_file') and not sub(r'[^a-zA-Z0-9]', '', config.log_file):
        validations[4]='Parameter "log_file" is empty string and/or use special characters. Defaulting name to "newLog"'

    validations=dict(sorted(validations.items()))
    shouldTerminate=False
    for k in validations.keys():
        if k > 100:
            logger.critical(f"{k} - {validations[k]}")
            shouldTerminate=True
        else:
            logger.warning(f"{k} - {validations[k]}")
    return shouldTerminate 

def additional_variables_setup(config):
    """
    Add variables to configuration

    Params:
        dict (config): Configuration dictionary using values from .yaml file

    Returns:
        dict (config): Updated config file
    """
    config.chunk_size = int((config.llm_max_tokens - config.llm_len_prompt_engineering) / config.k_most_similar)
    config.chunk_size = int(config.chunk_size * 100 / config.llm_tokens_per_100_characters)
    if config.chunk_size > config.llm_embedding_context_len:
        config.chunk_size = config.llm_embedding_context_len
    # Closest to 50, to have a little room to avoid truncation
    config.chunk_size = config.chunk_size - (  config.chunk_size%50  )
    config.chunk_overlap = int(config.chunk_overlap_ratio * config.chunk_size)
    # Closest to 10, to have a little room to avoid truncation
    config.chunk_overlap = config.chunk_overlap - (  config.chunk_overlap%10  )
    config.chunk_size_graph = config.llm_max_tokens - config.llm_len_prompt_engineering
    # Closest to 50, to have a little room to avoid truncation
    config.chunk_size_graph = config.chunk_size_graph - (  config.chunk_size_graph%50  )
    config.chunk_overlap_graph = int(config.chunk_overlap_ratio * config.chunk_size_graph)
    # Closest to 10, to have a little room to avoid truncation
    config.chunk_overlap_graph = config.chunk_overlap_graph - (  config.chunk_overlap_graph%10  )
    return config


def initialize_graph(config):
    """
    Initiliazes KG in Memgraph. Failure during this step will return in termination

    Params:
        dict (config): Configuration dictionary using values from .yaml file

    Returns:
        langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
    """
    try:
        url = os.environ.get("MEMGRAPH_URI", "bolt://"+config.memgraph_socket)
        username = os.environ.get("MEMGRAPH_USERNAME", config.memgraph_user)
        password = os.environ.get("MEMGRAPH_PASSWORD", config.memgraph_password)

        graph = MemgraphGraph(
            url=url, username=username, password=password, refresh_schema=False
        )
        return graph

    except Exception as ex:
        logger.error( f"An error occurred: {ex}")
        return None

def create_rdf_instructions(use_ontology, config):
    """
    Creates a list of definitions that already exist in ontology and format them as an additional instruction to provide to LLMs

    Params:
        bool (use_ontology): Validate if ontologies will be used
        dict (config): Configuration dictionary using values from .yaml file

    Returns:
        string (additional_instructions): Ontology definitions to provide to LLM
    """
    if not use_ontology:
        return ""
    rdf_graph=read_ontology(use_ontology, config)
    if rdf_graph is None:
        return ""
    additional_instructions=""
    try:

        results = search_rdf_classes_objects(rdf_graph)

        node_definitions = {}
        edge_definitions = {}
        for row in results:
            subject = clean_node_metadata(remove_special_chars_in_llm_output(get_local_name(str(row.subject))))
            entity_type = str(row.type).split("#")[-1]  # Get local name
            comment = str(row.comment)
            if entity_type == "Class":
                node_definitions[subject]=comment

            if entity_type == "ObjectProperty":
                edge_definitions[subject]=comment

        if node_definitions:
            additional_instructions+="Use the following definitions to help you determine what node labels you need to select\n"
            for k in node_definitions:
                additional_instructions+="{"+f"{k}, {node_definitions[k]}"+"}\n"
        if edge_definitions:
            additional_instructions+="Use the following definitions to help you determine what relation labels you need to select\n"
            for k in edge_definitions:
                additional_instructions+="{"+f"{k}, {edge_definitions[k]}"+"}\n"




    except Exception as ex:
        logger.error(f"Something unexpected happened: {ex}.  Working with node_labels={node_labels}, rel_types={rel_types}, relationship_type={relationship_type}, additional_instructions={additional_instructions}")


    return additional_instructions


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    if len(sys.argv)==1:
        local_parser().print_help()
        sys.exit(1)

    args = local_parser().parse_args() 
    config = load_config()

    if validate_config(config):
        sys.exit(1)

    # Create a connection to the database
    postgresql_connection = create_connection(config)
    if postgresql_connection is None:
        logger.critical("Could not be possible to connect to PostgreSQL. Due to this error, the program will exit")
        sys.exit(1)


    graph=initialize_graph(config)
    if graph is None:
        logger.critical("Graph couldn't be initialized. Due to this error, the program will exit")
        postgresql_connection.close()
        sys.exit(1)

    config = additional_variables_setup(config)

    if args.build_rag:
        errnum, errmsg=graph_from_pdf_directory(config, postgresql_connection, graph, args.ontology)
        if "000000" not in errnum:
            logger.critical("Due to this error, the program will exit")
            postgresql_connection.close()
            sys.exit(1)

    if args.update_table:
        create_insert_prompt_tables(config, postgresql_connection)

    if args.vector_chat:
        chat_loop_vector_questions(config, postgresql_connection)

    if args.graph_chat:
        rdf_additional_data=create_rdf_instructions(args.ontology, config)
        chat_loop_graph_questions(config, postgresql_connection, graph, rdf_additional_data)

    if args.chat:
        rdf_additional_data=create_rdf_instructions(args.ontology, config)
        chat_loop(config, postgresql_connection, graph, rdf_additional_data)

    postgresql_connection.close()
