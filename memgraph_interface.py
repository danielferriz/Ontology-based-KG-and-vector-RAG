from base_logger import logger
from tools import handle_logs, clean_node_metadata, remove_special_chars_in_llm_output, get_local_name
from rdf_interface import validate_relation, get_subclass_uri
import ast

def truncate_graph(graph):
	"""
	Truncates KG

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
	"""

	# graph.query("STORAGE MODE IN_MEMORY_ANALYTICAL")
	# graph.query("DROP GRAPH")
	# graph.query("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
	graph.query("MATCH (n) DETACH DELETE n;")

merge_chunk_node_query = """
MERGE(mergedChunk:Chunk {chunkId: $chunkParam.chunkId})
	ON CREATE SET 
		mergedChunk.directory = $chunkParam.directory,
		mergedChunk.formId = $chunkParam.formId, 
		mergedChunk.chunkSeqId = $chunkParam.chunkSeqId, 
		mergedChunk.text = $chunkParam.text
RETURN mergedChunk
"""

# ERRORS [201,250]
def initialize_graph_with_chunk(graph):
	"""
	Initializes KG in Memgraph. Error interval: [201,250]

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph

	Returns:
		Message Code, and Message Text.
	"""
	truncate_graph(graph)
	return handle_logs(logging_level=logger.DEBUG)

def merge_new_graph_chunk_node(graph, chunk):
	"""
	Creates/Merge new KG node in Memgraph

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		dict (chunk): Chunk with metadata

	Returns:
		Message Code, and Message Text.
	"""
	logger.debug(f"Creating `:Chunk` node for chunk ID {chunk['chunkId']}")
	graph.query(merge_chunk_node_query, 
		params={
			'chunkParam': chunk
		})
	return handle_logs(logging_level=logger.DEBUG)

def return_graph_labels(graph):
	"""
	Provides a list of current used labels both in nodes, and edges

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph

	Returns:
		list (node_labels): Node labels
		list (edge_labels): Edge labels
	"""
	node_labels=[]
	edge_labels=[]
	query="MATCH (n) RETURN DISTINCT labels(n);"
	results=graph.query(query)
	for r in results:
		for i in r['labels(n)']:
			if i not in node_labels:
				node_labels.append(i)
	query="MATCH ()-[r]-() RETURN DISTINCT type(r);"
	results=graph.query(query)
	for r in results:
		i=r['type(r)']
		if i not in edge_labels:
			edge_labels.append(i)
	return node_labels, edge_labels

def validate_graph_element(llm_str, approved_list):
	"""
	Validates if a string, which might represent either a node label or an edge label, exists within a list of approved labels

	Params:
		string (llm_str): Label to be analyzed
		list (approved_list): Valid names list

	Returns:
		string: Approved name from list, or empty string if no match was found
	"""
	if not approved_list or not llm_str:
		return llm_str
	for e in approved_list:
		# We commented the following lines since there's synergy with how the list is created
		# cleaned_list_member=clean_node_metadata(remove_special_chars_in_llm_output(e))
		# if not cleaned_list_member:
		# 	logger.warning(f"RDF class or property '{e}' is empty string after cleaning process")
		if e.lower() == llm_str.lower():
			return e
	return "" 

def hierarchy2nodeLabels(label, local2uri, hierarchy):
	"""
	Creates a string list of superclasses an ontology class might have

	Params:
		string (label): Label to be analyzed
		dict (local2uri): Relation between local name and URI
		dict (hierarchy): Dictionary that lists, per ontology class, the set of superclass related to that class

	Returns:
		list (superclasses): Superclasses names list, or None if there was an error during process
	"""
	if label not in local2uri.keys():
		logger.error(f"Unreferenced URI error caused by label: {label}")
	elif local2uri[label] not in hierarchy.keys():
		logger.debug(f"URI does not have superclasses: {local2uri[label]}")
	elif hierarchy[ local2uri[label] ]:
		superclasses_array=[]
		for sc in hierarchy[ local2uri[label] ]:
			superclasses_array.append(  clean_node_metadata(remove_special_chars_in_llm_output(get_local_name(sc))) )
		superclasses=':'.join(  superclasses_array  )
		return superclasses
	return None


def add_superclasses(graph, node, label, local2uri, hierarchy):
	"""
	Creates/Merges a new node in KG

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		string (node): Value of 'name' property in new node
		string (label): Name of node label
		dict (local2uri): Relation between local name and URI
		dict (hierarchy): Dictionary that lists, per ontology class, the set of superclass related to that class
	"""
	superclasses=hierarchy2nodeLabels(label, local2uri, hierarchy)
	if superclasses is not None:
		superclass_query=f"""
		MATCH (m:{label} {{name: '{node}'}})
		SET m:{superclasses}
		RETURN labels(m);
		"""
		logger.info(f"Adding superclasses with query:\n{superclass_query}")
		graph.query(superclass_query)

def insert_knowledge_graph_nodes_relations(graph, conn, chunk, rdf_graph, rdf_nodes, rdf_edges, local2uri, hierarchy, rel_hierarchy):
	"""
	Insert new node from LLM response

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		dict (conn): LLM-detected relation between 2 nodes
		dict (chunk): Chunk with metadata
		rdflib.Graph (rdf_graph): Ontology graph
		list (rdf_nodes): List of possible nodes
		list (rdf_edges): List of possible edges
		dict (local2uri): Relation between local name and URI
		dict (hierarchy): Dictionary that lists, per ontology class, the set of superclass related to that class
		dict (rel_hierarchy): Dictionary that lists, per ontology relation, the set of superclass related to that relation
	"""
	for k in conn.keys():
		conn[k]=remove_special_chars_in_llm_output(conn[k])
	head=conn['head']
	tail=conn['tail']
	head_type=validate_graph_element(clean_node_metadata(conn['head_type']), rdf_nodes )
	relation=validate_graph_element(clean_node_metadata(conn['relation']), rdf_edges )
	tail_type=validate_graph_element(clean_node_metadata(conn['tail_type']), rdf_nodes)

	if head and head_type:

		merge_node_query = f"""
		MERGE (m:{head_type} {{name: '{head}'}})
			ON CREATE SET 
			m.onProgress = 'Y',
			m.progressId = '{conn['prefix_id']+'A'}',
			m.originalType = '{head_type}'
		RETURN m;
		"""
		graph.query(merge_node_query)
		
		if rdf_graph is not None:
			add_superclasses(graph, head, head_type, local2uri, hierarchy)

	if tail and tail_type:

		merge_node_query = f"""
		MERGE (n:{tail_type} {{name: '{tail}'}})
			ON CREATE SET 
			n.onProgress = 'Y',
			n.progressId = '{conn['prefix_id']+'B'}',
			n.originalType = '{tail_type}'
		RETURN n;
		"""
		graph.query(merge_node_query)

		if rdf_graph is not None:
			add_superclasses(graph, tail, tail_type, local2uri, hierarchy)


	if head and tail and head_type and relation and tail_type:

		shouldIncludeRelation=True
		if local2uri:
			if head_type not in local2uri.keys() or tail_type not in local2uri.keys() or relation not in local2uri.keys():
				logger.error(f"""Couldn't retrieve LLM detected labels and RDF labels.
									head_type: {head_type}, tail_type: {tail_type}, relation: {relation}
									RDF labels:{local2uri.keys()}
								""")
			else:
				shouldIncludeRelation=validate_relation(rdf_graph, local2uri[head_type], local2uri[relation], local2uri[tail_type])
				if not shouldIncludeRelation:
					shouldIncludeRelation=validate_relation(rdf_graph, local2uri[tail_type], local2uri[relation], local2uri[head_type])
					if shouldIncludeRelation:
						war_msg=f"""LLM created a relation with an inverse direction. 
						LLM Detected Relation:
						<<head: {head}, head_type: {head_type}, relation: {relation}, tail: {tail}, tail_type: {tail_type}>>
						Changing to:
						<<head: {tail}, head_type: {tail_type}, relation: {relation}, tail: {head}, tail_type: {head_type}>>
						
						Manually adjusting..."""
						logger.warning(war_msg)
						aux=head_type
						head_type=tail_type
						tail_type=aux
						aux=head
						head=tail
						tail=aux
					else:
						info_msg = f""" Relation '{relation}' cannot exist between classes '{head_type}' and '{tail_type}'
						Skipping relation..."""
						logger.info(info_msg)

		if shouldIncludeRelation:		
			merge_node_query = f"""
			MATCH (m:{head_type} {{name: '{head}'}}), (n:{tail_type} {{name: '{tail}'}})
			MERGE (m)-[r:{relation}]->(n)
			RETURN m;
			"""
			graph.query(merge_node_query)

			if rdf_graph is not None:
				edge_hierarchy = hierarchy2nodeLabels(relation, local2uri, rel_hierarchy)
				if edge_hierarchy is not None:
					edgeSuperClasses=edge_hierarchy.split(':')
					for edgeSuperClass in edgeSuperClasses:
						merge_node_query = f"""
						MATCH (m:{head_type} {{name: '{head}'}}), (n:{tail_type} {{name: '{tail}'}})
						MERGE (m)-[r:{edgeSuperClass}]->(n)
						RETURN m;
						"""
						graph.query(merge_node_query)


def return_onProcess_nodes(graph):
	"""
	Returns a list of nodes that relate to current analyzed text chunk, i.e., has the 'onProgress' temporary label

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph

	Returns:
		list: Nodes that relate to current analyzed text chunk
	"""
	query="""
	MATCH (n)  
	WHERE n.onProgress IS NOT NULL
	RETURN n.name AS nodes, n.progressId AS progressId, n.originalType AS originalType;
	"""
	return graph.query(query)

def remove_onProcess_status(graph):
	"""
	Per each active node, i.e., nodes with the 'onProgress' temporary label, assigns 
	an id after any merge/delete KG operations, to the active nodes. Then, it removes
	any temporary label from the KG

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
	"""
	query="""
	MATCH (n)  
	WHERE n.onProgress IS NULL
	RETURN count(n) AS total_past_nodes;
	"""
	results = graph.query(query)
	counter=0
	if results and 'total_past_nodes' in results[0].keys():
		counter=results[0]['total_past_nodes']
	results=return_onProcess_nodes(graph)
	for row in results:
		counter+=1
		query=f"""
		MATCH (n)
		WHERE n.progressId = '{row['progressId']}'
		SET n.id = '{counter:09x}'
		RETURN n
		"""
		graph.query(query)
	query="""
	MATCH (n)
	WHERE n.onProgress IS NOT NULL
	SET n.onProgress = NULL, n.progressId=NULL, n.persistent=NULL, n.originalType=NULL
	RETURN n;
	"""
	graph.query(query)

def recover_label_list_of_subgroup(graph, progressId ):
	"""
	Returns a list of labels a node has

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		string (progressId): Node ID

	Returns:
		list (result): Label nodes
	"""
	query=f"""
	MATCH (n)
	WHERE n.progressId = '{progressId}'
	RETURN labels(n) as nodeLabels
	"""
	logger.debug(f"Querying Cypher: {query}")
	result = graph.query(query)
	if len(result) != 1:
		logger.error(f"Cypher expected 1 value, instead it got {len(result)} ")
		logger.info(str(result))
	return result


def combine_similar_group_nodes(graph, rdf_graph, local2uri, hierarchy, similar_groups):
	"""
	Combine nodes that represent same subject

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		rdflib.Graph (rdf_graph): Ontology graph
		dict (local2uri): Relation between local name and URI
		dict (hierarchy): Dictionary that lists, per ontology class, the set of superclass related to that class
		dict (similar_groups): Optimal group assignment per node
	"""
	try:
		logger.debug(f"similar_groups: {similar_groups}")
		for originalTypeGroup in similar_groups.keys():
			hierarchyOrginalType = hierarchy2nodeLabels(originalTypeGroup, local2uri, hierarchy)
			labelSet=''
			if hierarchyOrginalType is not None and hierarchyOrginalType:
				labelSet=f", n:{hierarchyOrginalType}"
			head_progressId=similar_groups[originalTypeGroup][0]
			lats_known_id=head_progressId
			query=f"""
			MATCH (n {{progressId: '{head_progressId}'}})
			SET n.persistent = 'Y' {labelSet}
			RETURN n.name AS name
			"""
			logger.debug(f"Querying Cypher: {query}")
			alias=[graph.query(query)[0]['name']]
			for i in range(1,len(similar_groups[originalTypeGroup])):
				logger.debug("Getting data from next similar node")
				tail_progressId = similar_groups[originalTypeGroup][i]
				query=f"""
				MATCH (old {{progressId: '{tail_progressId}'}}),
				      (new {{progressId: '{head_progressId}'}})

				// Merge properties (overwritting head data)
				SET new += old
				RETURN old.name AS name;
				"""
				# Keep overwritting
				head_progressId=tail_progressId
				logger.debug(f"Querying Cypher: {query}")

				tail_name=graph.query(query)[0]['name']
				alias.append(tail_name)
				# Combining Relations
				query=f"""
				MATCH (old {{progressId: '{tail_progressId}'}})-[r]-(o)
				RETURN type(r) AS target_relations_type, startNode(r) AS starting_node, endNode(r) AS ending_node;
				"""
				logger.debug(f"Querying Cypher: {query}")
				kg_results=graph.query(query)
				for row in kg_results:
					logger.debug(f"Validanting row {row}")
					senderMerge = f"(new)-[:{row['target_relations_type']}]->(t);"
					if row['starting_node']['properties']['progressId'] != tail_progressId:
						senderMerge = f"(new)<-[:{row['target_relations_type']}]-(t);"
					query=f"""
					MATCH (new {{progressId: '{tail_progressId}'}}), (t {{progressId: '{row['ending_node']['progressId']}'}})
					MERGE {senderMerge}
					"""
					logger.debug(f"Querying Cypher: {query}")
					graph.query(query)

				# Deleting the <old> node
				query=f"""
				MATCH (n {{progressId: '{tail_progressId}'}})
				WHERE n.persistent IS NULL
				DETACH DELETE n;
				"""
				logger.debug(f"Querying Cypher: {query}")
				graph.query(query)
				lats_known_id=tail_progressId
			if len(alias)>1: 
				query=f"""
				MATCH (n {{progressId: '{tail_progressId}'}})
				SET n.alias='{';'.join(alias[:-1])}'
				"""
				logger.debug(f"Querying Cypher: {query}")
				graph.query(query)

	except Exception as ex:
		logger.error(f"Error during execution: {ex} in line {ex.__traceback__.tb_lineno}")

def create_new_relations(graph, same_relations):
	"""
	Creates relations that were not originally detected by LLM

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		dict (same_relations): Set of similar relations found by LLM
	"""
	try:
		#logger.info(f"same_relations: {same_relations}")
		for relation in same_relations:
			for tupleRel in same_relations[relation]:
				leftNode=tupleRel[0]
				rightNode=tupleRel[1]
				query = f"""
				MATCH (m {{progressId: '{leftNode}'}}), (n {{progressId: '{rightNode}'}})
				MERGE (m)-[r:{relation}]->(n);
				"""
				graph.query(query)


	except Exception as ex:
		logger.error(f"Error during execution: {ex}")


def counts_connections_from_a_to_b(graph, headId, relationType, tailId):
	"""
	Counts the number of connections that exists between 2 nodes of an specific edge label

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		string (headId): Head node ID
		string (relationType): Edge label
		string (tailId): Tail node ID

	Returns:
		int: Number of connections that exists between 2 nodes of an specific edge label
	"""
	try:
		query=f"""
		MATCH (n {{progressId:'{headId}'}}) -[r:{relationType}]->(m {{progressId:'{tailId}'}})
		RETURN COUNT(r) AS numberOfRelations
		"""
		numberOfRelations=graph.query(query)
		return numberOfRelations[0]['numberOfRelations']
	except Exception as ex:
		logger.error(f"Error during execution: {ex}")
		return 0

def create_fileNode(graph, filePath, fileId):
	"""
	Creates a node that represents the original file from where the current KG was generated from 

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		string (filePath): Absolute path of analyzed file
		string (fileId): File ID
	"""
	try:
		query=f"""
		MERGE (m:PdfFile {{fileId: '{fileId}'}})
			ON CREATE SET 
			m.filePath = '{filePath}'
		RETURN m;
		"""
		numberOfRelations=graph.query(query)
	except Exception as ex:
		logger.error(f"Error during execution: {ex}")

def linkActiveNodesToFile(graph,  fileId):
	"""
	Links active text chunk nodes to the working file node

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph
		string (fileId): File ID
	"""
	try:
		query=f"""
		MATCH (m), (n:PdfFile {{fileId: '{fileId}'}})
		WHERE m.onProgress IS NOT NULL
		MERGE (m)-[r:DefinedInFile]->(n)
		"""
		graph.query(query)

		remove_onProcess_status(graph)
		
	except Exception as ex:
		logger.error(f"Error during execution: {ex}")

def return_schema(graph):
	"""
	Returns KG schema

	Params:
		langchain_community.graphs.memgraph_graph.MemgraphGraph (graph): Memgraph knowledge graph

	Returns:
		string: KG schema, or None if schema retrieval operation failed
	"""
	query="SHOW SCHEMA INFO;"
	try:
		results=graph.query(query)
		if not isinstance(results, list): 
			raise ValueError("Couldn't retrieve result list from Memgraph.")
		if not results:
			raise ValueError("Empty set.")
		if 'schema' not in results[0]:
			raise ValueError("Set didn't include 'schema' value")
		if not isinstance(results[0]['schema'], str) or not results[0]['schema']:
			raise ValueError("Empty schema")

		return results[0]['schema']
		
	except Exception as ex:
		err_msg = f"""Error during execution of: '{query}'
		ERROR: {ex}
		This could be caused by missing configuration options in Memgraph.
		Please, validate that the file /etc/memgraph/memgraph.conf 
		has the option --schema-info-enabled set to True. 
		If the option --schema-info-enabled is set to False, or if it
		doesn't exist in the configuration file, please adjust it and
		restart the Memgraph service. 

		For more information on Memgraph configuration, please visit:
		https://memgraph.com/docs/database-management/configuration

		For information regarding the operation {query}, visit:
		https://memgraph.com/docs/querying/schema
		"""
		logger.critical(err_msg)
		return None





