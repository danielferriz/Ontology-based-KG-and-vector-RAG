import os
from base_logger import logger
from argparse import Namespace

from nltk import word_tokenize
from nltk.probability import FreqDist
from nltk.stem.snowball import stopwords
from nltk.stem import PorterStemmer
from nltk import download

from re import sub

def get_absolute_path(path: str):
    if os.path.exists(path):
        return os.path.abspath(path).rstrip(os.sep) + os.sep
    return None


def get_parent_folder(filepath: str) -> str:
    # Assumes that filepath exists
    abs_path = os.path.abspath(filepath)
    parent_dir = os.path.dirname(abs_path)
    return os.path.basename(parent_dir)

def handle_logs(errnum=0, errmsg='Success', logging_level=logger.INFO):
    if isinstance(errnum, (int, str)):
        errnum = f"{int(errnum):06d}"
    else:
        errnum = "999999"

    if not isinstance(errmsg, str) or not errmsg.strip():
        errmsg='Error during process'

    full_message = errnum +" - "+errmsg

    if logging_level==logger.DEBUG:
        logger.debug(full_message)
    elif logging_level == logger.INFO:
        logger.info(full_message)
    elif logging_level == logger.WARNING:
        logger.warning(full_message)
    elif logging_level == logger.ERROR:
        logger.error(full_message)
    elif logging_level == logger.CRITICAL:
        logger.critical(full_message)
    else:
        logger.info(full_message)

    return errnum, errmsg



def cleanWords(sentence):

    words = word_tokenize(sentence)
    words_no_punc = []
    for w in words:
        if w.isalpha():
          words_no_punc.append(w.lower())

    tokens = []
    try:
        stopwords_corpus = stopwords.words('english') + stopwords.words('french')
    except LookupError:
        logger.warning("'Stopwords package wasn't found. Downloading now...'")
        download('stopwords')
        stopwords_corpus = stopwords.words('english') + stopwords.words('french')
    for w in words_no_punc:
        if w not in stopwords_corpus:
          tokens.append(w)
    # Stem words
    stemmer = PorterStemmer()
    tokens = [stemmer.stem(token) for token in tokens]
    # Join tokens back into a string
    text = ' '.join(tokens)
    return text


def camel_to_snake(camel_str):
    return sub(r'([a-z])([A-Z])', r'\1_\2', camel_str)
     

def clean_node_metadata(node_metadata):
    return sub(r'[^a-zA-Z0-9_ \'"]', ' ', camel_to_snake(node_metadata)).replace(" ","_")

def remove_special_chars_in_llm_output(llm_response):
    return str(llm_response).strip().replace("'","\\'").replace('"','\\"').replace("&","and")

def get_local_name(uri_ref):
    """Extract local name from URI reference"""
    uri = str(uri_ref)
    if "#" in uri:
        return uri.split("#")[-1]
    return uri.split("/")[-1]