import requests
import json
from base_logger import logger

def get_embedding(config, text):
	"""
	Gets embedding of text

	Params:
		dict (config): Configuration dictionary using values from .yaml file
		string (text): Text to embed

	Returns:
		list (embedding): Text embedding
	"""
	try:
		model_name=config.llm_embedding_model
		if not text:
			logger.warning("Text to embed is epmpty string")
		url = config.llm_embedding_url
		headers = {"Content-Type": "application/json"}
		data = {
			"model": model_name,
			"input": text
		}
		response = requests.post(url, headers=headers, data=json.dumps(data))
		response.raise_for_status()
		if "data" not in response.json().keys(): 
			raise ValueError("Response didn't include the block 'data'")
		if not isinstance(response.json()['data'], list):
			raise ValueError("Response didn't include message list")
		if not response.json()['data']:
			raise ValueError("Response replied with empty message list")
		if "embedding" not in response.json()['data'][0].keys(): 
			raise ValueError("Response didn't include the block 'embedding'")
		return response.json()['data'][0]['embedding']
	except requests.exceptions.RequestException as e:
		logger.error( f"Request error: {e}")
		return None
	except ValueError as ve:
		logger.error(f"Validation error: {ve}")
		return None
	except KeyError:
		logger.error( "Unexpected response format")
		return None
	except Exception as ex:
		logger.error( f"An error occurred: {ex}")
		return None

def get_chat_completion(config, messages=[]):
	"""
	Manager of LLM prompts

	Params:
		dict (config): Configuration dictionary using values from .yaml file
		list (messages): Set of messages sent to LLM, where there's at least one 'role' and one 'content' message

	Returns:
		list (embedding): LLM response
	"""
	try:
		model_name=config.llm_chat_model
		if not isinstance(messages, list) or not all(isinstance(msg, dict) and "role" in msg and "content" in msg for msg in messages):
			raise ValueError("messages must be a list of dictionaries with 'role' and 'content' keys")
		
		if not any(msg["role"] == "system" for msg in messages):
			raise ValueError("messages must contain at least one dictionary with role 'system'")
		
		if not any(msg["role"] == "user" for msg in messages):
			raise ValueError("messages must contain at least one dictionary with role 'user'")

		query_length = 0
		for msg in messages:
			if "content" not in msg.keys():
				raise ValueError("messages must contain variable 'content'")
			query_length+=len(msg["content"])

		# We add 50 tokens since we are working with approximations
		query_tokens = int(query_length * config.llm_tokens_per_100_characters / 100) + 50
		if query_tokens  > config.llm_max_tokens:
			error_msg=f"""Cannot process since the number of tokens surpasses the
			limit stablished in the config.yaml file. 

			requesting message: {messages}

			Query Tokens: {query_tokens}

			LLM Token Limit: {config.llm_max_tokens}

			"""
			raise ValueError(error_msg)
		
		url = config.llm_chat_url
		headers = {"Content-Type": "application/json"}
		data = {
			"model": model_name,
			"messages": messages,
			"temperature": 0,
			"max_tokens": -1,
			"stream": False
		}
		#logger.debug(messages)
		#logger.debug(f"Query Tokens: {query_tokens}")
		#logger.debug(f"query_length: {query_length}")
		response = requests.post(url, headers=headers, data=json.dumps(data))
		response.raise_for_status()
		if "choices" not in response.json().keys(): 
			raise ValueError("Response didn't include the block 'choices'")
		if not isinstance(response.json()['choices'], list):
			raise ValueError("Response didn't include message list")
		if not response.json()['choices']:
			raise ValueError("Response replied with empty message list")
		if "message" not in response.json()['choices'][0].keys(): 
			raise ValueError("Response didn't include the block 'message'")
		if "content" not in response.json()['choices'][0]['message'].keys(): 
			raise ValueError("Response didn't include the block 'content'")
		return response.json()['choices'][0]['message']['content']
	
	except requests.exceptions.RequestException as e:
		logger.error( f"Request error: {e}")
		return None
	except ValueError as ve:
		logger.error(f"Validation error: {ve}")
		return None
	except KeyError:
		logger.error( "Unexpected response format")
		return None
	except Exception as ex:
		logger.error( f"An error occurred: {ex}")
		return None
