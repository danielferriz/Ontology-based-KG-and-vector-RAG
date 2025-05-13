from base_logger import logger
from tools import get_absolute_path, get_parent_folder
from tools import handle_logs, clean_node_metadata, remove_special_chars_in_llm_output, get_local_name
from tools import cleanWords
from memgraph_interface import initialize_graph_with_chunk, create_fileNode, linkActiveNodesToFile
from memgraph_interface import merge_new_graph_chunk_node, create_fileNode
from lmstudio import get_embedding
from interactions import create_knowledge_graph_with_llm
from rdf_interface import search_rdf_classes_objects, get_class_hierarchy
from postgresql import insert_chunks_with_vectors, initialize_vector_table, select_prompt

import os
import math
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFLoader
from tqdm import tqdm
from rdflib import Graph, URIRef, Literal, RDF


def graph_from_pdf_directory(config, postgresql_connection, graph, use_ontology=False):
	"""
	Splits text and then use each text chunk to create initial graph and vector store. Error Interval: [101,150]

	Params:
		dict (config): Configuration dictionary using values from .yaml file
		psycopg2.connection (postgresql_connection): Database connnection
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		bool (use_ontology): Boolean value that indicates if user has chosen to use an ontology for the KG creation

	Returns:
		Message Code, and Message Text. 
	"""
	rdf_graph=read_ontology(use_ontology, config)
	if use_ontology and rdf_graph is None:
		return handle_logs(101,"Error while reading ontology",logger.CRITICAL)
	full_path=get_absolute_path(config.pdf_folder_path)
	if full_path is None:
		return handle_logs(102,"Path of PDF files does NOT exist",logger.CRITICAL)

	text_splitter=None
	try:
		text_splitter = RecursiveCharacterTextSplitter(
		chunk_size = config.chunk_size,
		chunk_overlap  = config.chunk_overlap,
		length_function = len,
		is_separator_regex = False,
		)
	except Exception as ex:
		handle_logs( 103, f"An error occurred: {ex}", logger.ERROR)

	if text_splitter is None:
		return handle_logs(104,"Text Splitter wasn't propperly initialized",logger.CRITICAL)


	text_splitter_kg=None
	try:
		text_splitter_kg = RecursiveCharacterTextSplitter(
		chunk_size = config.chunk_size_graph,
		chunk_overlap  = config.chunk_overlap_graph,
		length_function = len,
		is_separator_regex = False,
		)
	except Exception as ex:
		handle_logs( 105, f"An error occurred: {ex}", logger.ERROR)

	if text_splitter_kg is None:
		return handle_logs(106,"Text Splitter wasn't propperly initialized",logger.CRITICAL)

	logger.debug(f"config.chunk_size: {config.chunk_size}")
	logger.debug(f"config.chunk_overlap: {config.chunk_overlap}")

	errnum, errmsg=initialize_vector_table(postgresql_connection, config)
	if "000000" not in errnum:
		return errnum, errmsg

	initialize_graph_with_chunk(graph)


	results=search_rdf_classes_objects(rdf_graph)
	node_labels, rel_types, relationship_type, additional_instructions=create_variables_for_up_with_rdf(results)
	system_prompt, human_prompt_string=create_unstructured_prompt(node_labels, rel_types, relationship_type, additional_instructions, postgresql_connection, config)
	logger.debug(f"------\nsystem_prompt:\n{system_prompt}\n-----\n human_prompt_string\n{human_prompt_string}")
	rdf_nodes, rdf_edges, local2uri=get_rdf_nodes_edges(results)
	hierarchy=get_class_hierarchy(rdf_graph)
	rel_hierarchy=get_class_hierarchy(rdf_graph,'property')

	vector_node_count=0
	file_counter=0
	for file in tqdm(os.listdir(full_path)):
		if file.endswith('.pdf'):
			pdf_path = os.path.join(full_path, file)
			logger.info(f"Processing file: {pdf_path}")
			loader = PyPDFLoader(pdf_path)
			full_pdf_text=""
			for doc in loader.load():
				full_pdf_text += doc.page_content +"\n" # grab the text of the item
			# Vector Side
			item_text_chunks = text_splitter.split_text(full_pdf_text) # split the text into chunks
			logger.debug(f'item_text_chunks size for vectors: {len(item_text_chunks)}')
			chunk_seq_id = 0
			file_seq_id= f"{file_counter:06x}"
			for chunk in item_text_chunks: 
				chunk_with_metadata=create_chunk_with_metadata_and_vector(config, pdf_path, chunk, chunk_seq_id, file_seq_id)
				errnum, errmsg=insert_chunks_with_vectors(postgresql_connection, chunk_with_metadata)
				if "000000" not in errnum:
					return errnum, errmsg
				chunk_seq_id += 1
			# Graph Side
			item_text_chunks = text_splitter.split_text(full_pdf_text) # split the text into chunks
			chunk_seq_id = 0
			logger.debug(f'item_text_chunks size for knowledge graph: {len(item_text_chunks)}')
			for chunk in tqdm(item_text_chunks, leave=False, desc="Adding chunks to knowledge graph"): 
				query=human_prompt_string+chunk
				chunk_with_metadata=create_chunk_with_metadata_no_vector(pdf_path, chunk, chunk_seq_id)
				create_knowledge_graph_with_llm(postgresql_connection, config, graph, chunk_with_metadata, rdf_graph, rdf_nodes, rdf_edges, local2uri, hierarchy, rel_hierarchy,system_prompt, query)
				"""
				errnum, errmsg=create_knowledge_graph_with_llm(config, graph, chunk, rdf_graph)
				if "000000" not in errnum:
					return errnum, errmsg
				"""
				chunk_seq_id += 1

			create_fileNode(graph, pdf_path, file_seq_id)
			linkActiveNodesToFile(graph,  file_seq_id)
			file_counter+=1

	
	return handle_logs()


def create_chunk_with_metadata_and_vector(config, pdf_path, chunk, chunk_seq_id, file_seq_id):
	"""
	From a given text chunk, creates a dictionary containing the text itself, alongside its metadata including the vectorial representation (embedding)

	Params:
		dict (config): Configuration dictionary using values from .yaml file
		str (pdf_path): Filepath 
		str (chunk): Text chunk
		int (chunk_seq_id): Text chunk ID
		str (file_seq_id): File ID

	Returns:
		Dictionary containing text chunk with metadata (including embedding)
	"""
	form_id = pdf_path[pdf_path.rindex('/') + 1:pdf_path.rindex('.')] # extract form id from file name
	text = cleanWords(chunk)
	vector = get_embedding(config, text)
	chunk_with_metadata={
		'text': chunk, 
		'filename': pdf_path,
		# constructed metadata...
		'embedding': f'{vector}',
		'chunkId': f'file-{file_seq_id}_form-{form_id}_chunk-{chunk_seq_id:09d}',
		}
	return chunk_with_metadata

def create_chunk_with_metadata_no_vector(pdf_path, chunk, chunk_seq_id):
	"""
	From a given text chunk, creates a dictionary containing the text itself

	Params:
		str (pdf_path): Filepath 
		str (chunk): Text chunk
		int (chunk_seq_id): Text chunk ID

	Returns:
		Dictionary containing text chunk with metadata
	"""
	form_id = pdf_path[pdf_path.rindex('/') + 1:pdf_path.rindex('.')] # extract form id from file name
	base_directory = get_parent_folder(pdf_path)
	# finally, construct a record with metadata and the chunk text
	chunk_with_metadata={
		'text': chunk, 
		'chunkSeqId': chunk_seq_id,
		# constructed metadata...
		'directory': f'{base_directory}',
		'formId': f'{form_id}', # pulled from the filename
		'chunkId': f'{form_id}-chunk{chunk_seq_id:06d}',
		}
	return chunk_with_metadata

# ERRORS [151,200]
def read_ontology(use_ontology, config):
	"""
	RdfLib implementation to connect with ontology. Error Interval: [151,200]

	Params:
		bool (use_ontology): Boolean value that indicates if an attempt to connect to the ontology should be used
		dict (config): Configuration dictionary using values from .yaml file

	Returns:
		None if validations fail, rdflib graph otherwise
	"""
	if not use_ontology:
		return None
	if not config.rdf_filepath:
		handle_logs( 151, "Configuration file is missing the path for the ontology definition", logger.ERROR)
		return None
	if not os.path.isfile(config.rdf_filepath):
		handle_logs( 151, f"Could not retrieve file {config.rdf_filepath}", logger.ERROR)
	try:
		graph = Graph()
		graph.parse(config.rdf_filepath, format='xml')
		return graph
	except Exception as ex:
		handle_logs( 153, f"An error occurred: {ex}", logger.ERROR)
		return None
		

def get_rdf_nodes_edges(results):
	"""
	Creates a list of possible nodes and edges of a given ontology, as well as an associaton between their local name and the ontology URI

	Params:
		dict (results): Results after querying the rdflib connection

	Returns:
		list (rdf_nodes): List of possible nodes
		list (rdf_edges): List of possible edges
		dict (local2uri): Relation between local name and URI
	"""
	if results is None:
		return [], [], {}

	rdf_nodes=[]
	rdf_edges=[]
	local2uri={}

	for row in results:
		subject = get_local_name(str(row.subject))
		entity_type = str(row.type).split("#")[-1]  # Get local name

		subject=clean_node_metadata(remove_special_chars_in_llm_output(subject))

		if entity_type == "Class":
			rdf_nodes.append(subject)

		if entity_type == "ObjectProperty":
			rdf_edges.append(subject)

		if entity_type == "ObjectProperty" or entity_type == "Class":
			if subject in local2uri:
				logger.warning(f"Value {subject} was being used to represent RDF URI {local2uri[subject]}, now it will represent RDF URI {str(row.subject)} which might lead to unreferenced URIs")
			local2uri[subject]= str(row.subject)

	return rdf_nodes, rdf_edges, local2uri


def create_variables_for_up_with_rdf(results):
	"""
	Creates a set of variables that are compatible with langchain.experimental libraries

	Params:
		dict (results): Results after querying the rdflib connection

	Returns:
		list (node_labels): String names containing node labels (ontology classes)
		list (rel_types):String names containing relation labels (ontology object properties)
		string (relationship_type): String containing empty value or value "tuple"
		string (additional_instructions): Additional instruction to parse onto LLM
	"""
	if results is None:
		return None, None, None, None#
	node_labels=None
	rel_types=None
	relationship_type=None
	additional_instructions=None

	try:
		node_definitions = {}
		edge_definitions = {}
		for row in results:
			subject = get_local_name(str(row.subject))
			entity_type = str(row.type).split("#")[-1]  # Get local name
			comment = str(row.comment)
			if entity_type == "Class":
				if node_labels is None:
					node_labels=[]
				node_labels.append(subject)
				node_definitions[subject]=comment

			if entity_type == "ObjectProperty":
				if rel_types is None:
					rel_types=[]
				rel_types.append(subject)
				edge_definitions[subject]=comment

		relationship_type=""
		# logger.debug(f"node_labels: {node_labels}")
		# logger.debug(f"rel_types: {rel_types}")
		# logger.debug(f"node_definitions: {node_definitions}")
		# logger.debug(f"edge_definitions: {edge_definitions}")
		if node_definitions or edge_definitions:
			additional_instructions=""
			if node_definitions:
				additional_instructions+="Use the following definitions to help you determine what types of head, and tail you need to select\n"
				for k in node_definitions:
					additional_instructions+="{"+f"{k}, {node_definitions[k]}"+"}\n"
			if edge_definitions:
				additional_instructions+="Use the following definitions to help you determine what types of relation you need to select\n"
				for k in edge_definitions:
					additional_instructions+="{"+f"{k}, {edge_definitions[k]}"+"}\n"



	except Exception as ex:
		logger.error(f"Something unexpected happened: {ex}.  Working with node_labels={node_labels}, rel_types={rel_types}, relationship_type={relationship_type}, additional_instructions={additional_instructions}")


	return node_labels, rel_types, relationship_type, additional_instructions



def create_unstructured_prompt(node_labels=None, rel_types=None, relationship_type=None, additional_instructions=None, postgresql_connection=None, config=None):
	"""
	Creates an LLM prompt for querying the creation of initial KG using ontology definitions

	Params:
		list (node_labels): String names containing node labels (ontology classes)
		list (rel_types):String names containing relation labels (ontology object properties)
		string (relationship_type): String containing empty value or value "tuple"
		string (additional_instructions): Additional instruction to parse onto LLM
		psycopg2.connection (postgresql_connection): Database connnection
		dict (config): Configuration dictionary using values from .yaml file

	Returns:
		string (system_prompt): Behavior to be adopted by LLM to create KG
		string (human_prompt_string): LLM request to create KG
	"""
	node_labels_str=''
	if node_labels:
		node_labels_str=str(node_labels)
	rel_types_str=''
	if rel_types:
		if relationship_type == "tuple":
			rel_types_str = str(list({item[1] for item in rel_types}))
		else:
			rel_types_str = str(rel_types)
	if not additional_instructions:
		additional_instructions=''
	prompt_variables = {'node_labels_str':node_labels_str, 'rel_types_str':rel_types_str, 'rel_types':rel_types,'additional_instructions':additional_instructions,'node_labels':node_labels}
	system_prompt=select_prompt(postgresql_connection, config, 1, variables=prompt_variables)
	if additional_instructions:
		system_prompt+=".\n"+additional_instructions

	human_prompt_string=select_prompt(postgresql_connection, config, 2, variables=prompt_variables)
	
	return system_prompt, human_prompt_string


