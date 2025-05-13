from rdflib import Graph, RDFS, RDF, Namespace, URIRef
from base_logger import logger


def search_rdf_classes_objects(rdf_graph):
	if rdf_graph is None:
		return None
	try:
		# Define namespaces
		OWL = Namespace("http://www.w3.org/2002/07/owl#")

		# SPARQL query with type filtering
		query = """
		PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

		SELECT ?subject ?type ?comment
		WHERE {
		  ?subject rdfs:comment ?comment .
		  ?subject rdf:type ?type .
		  FILTER (?type IN (owl:Class, owl:ObjectProperty))
		}
		"""
		results = rdf_graph.query(query)
		return results

	except Exception as ex:
		logger.error(f"Something unexpected happened: {ex}.")
		return None



def get_namespace_schema(rdf_graph):
	schema_ns = rdf_graph.namespace_manager.store.namespace("schema")
	possible_namespaces=[str(schema_ns)]
	for prefix, ns in rdf_graph.namespace_manager.namespaces():
		if prefix.startswith("schema") and str(ns) not in possible_namespaces:
			possible_namespaces.append(str(ns))
	if len(possible_namespaces)==1:
		return possible_namespaces[0]
	if len(possible_namespaces)>2:
		logger.warning("Multiple Schemas detected. This might lead to an error. Please verify XML file")
	return possible_namespaces[1]

def get_property_end2end(rdf_graph, property_uri, typeAttribute='domain'):
	SCHEMA = Namespace(get_namespace_schema(rdf_graph))
	if typeAttribute.lower() not in ["domain","range"]:
		logger.warning("Invalid option. Setting to [domain]")
		typeAttribute = "domain"
	includesType = SCHEMA.domainIncludes
	rdfsType="domain"
	if typeAttribute.lower() == "range":
		includesType = SCHEMA.rangeIncludes
		rdfsType="range"
	query = f"""
	SELECT ?var WHERE {{
	  <{property_uri}> <{includesType}> ?var .
	}}
	"""
	results = rdf_graph.query(query)
	result_list = [str(row.var) for row in results]
	query=f"""
	SELECT ?var WHERE {{
      <{property_uri}> rdfs:{rdfsType} ?var .
	}}
	"""
	results = rdf_graph.query(query)
	return result_list + [str(row.var) for row in results]



def validate_relation(rdf_graph, domain_class, relation, range_class):
	# SPARQL Query to check if the relation exists between subject_class and object_class
	query = f"""
	ASK WHERE {{
      <{relation}> rdfs:domain ?dom .
      <{relation}> rdfs:range  ?ran .
      ?dom rdfs:subClassOf* <{domain_class}> .
      ?ran rdfs:subClassOf* <{range_class}> .
	}}
	"""
	# Run the query
	result = rdf_graph.query(query)
	if bool(result.askAnswer):
		return True
	
	range_list = get_property_end2end(rdf_graph, relation, typeAttribute='range')
	domain_list = get_property_end2end(rdf_graph, relation, typeAttribute='domain')
	
	subclass = ""
	i=0
	while i<len(range_list) and subclass != range_class:
		subclass = get_subclass_uri(rdf_graph, range_class, range_list[i])
		i+=1
	if subclass != range_class: 
		return False

	subclass = ""
	i=0
	while i<len(domain_list) and subclass != domain_class:
		subclass = get_subclass_uri(rdf_graph, domain_class, domain_list[i])
		i+=1
	if subclass != domain_class: 
		return False

	return True


def get_class_hierarchy(rdf_graph, typeOfRelation='object'):
	if rdf_graph is None:
		return {}
	if typeOfRelation.lower() not in ['object','property']:
		logger.warning("Invalid type of hierarchy. Setting to [object]")
		typeOfRelation = 'object'

	relation=RDFS.subClassOf
	if typeOfRelation.lower()=='property':
		relation=RDFS.subPropertyOf
	# Dictionary to store class hierarchy
	hierarchy = {}

	# Get all classes (subjects of rdfs:subClassOf statements)
	all_classes = set(rdf_graph.subjects(relation, None)) | set(rdf_graph.objects(None, relation))

	# Function to recursively find all superclasses
	def get_superclasses(cls):
		superclasses = set()
		for superclass in rdf_graph.objects(cls, relation):
			superclasses.add(superclass)
			superclasses.update(get_superclasses(superclass))  # Recursively find superclasses
		return superclasses

	# Build the hierarchy dictionary
	for cls in all_classes:
		hierarchy[str(cls)] = [str(superclass) for superclass in get_superclasses(cls)]

	return hierarchy


def get_subclass_uri(rdf_graph, subclass_uri, superclass_uri):
	# Define a SPARQL query.
	# We use a BIND to set ?subclass to the given subclass URI and then check if
	# it is indeed a subclass of the given superclass.
	if subclass_uri == superclass_uri:
		return superclass_uri

	query = f"""
	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
	SELECT ?subclass WHERE {{
	  BIND(<{subclass_uri}> AS ?subclass) .
	  ?subclass rdfs:subClassOf <{superclass_uri}> .
	}}
	"""

	# Execute the query.
	results = rdf_graph.query(query)
	# If a result is found, return the URI of the subclass
	for row in results:
		return str(row.subclass)


	# If the result is not found, validates if the inverse order is true; 
	# otherwise, return an empty string. 
	query = f"""
	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
	SELECT ?subclass WHERE {{
	  BIND(<{superclass_uri}> AS ?subclass) .
	  ?subclass rdfs:subClassOf <{subclass_uri}> .
	}}
	"""

	# Execute the query.
	results = rdf_graph.query(query)
	# If a result is found, return the URI of the subclass; otherwise, return an empty string.
	for row in results:
		return str(row.subclass)
	
	return ""


def provide_relation_comment(rdf_graph, relation):
	try:
		subject = URIRef(relation)
		comment = rdf_graph.value(subject=subject, predicate=RDFS.comment)
		# logger.debug("comment: "+str(comment))
		if comment:
			return str(comment)
		return ""
	except Exception as ex:
		logger.error(f"Error while reading comments. {ex}")
		return ""