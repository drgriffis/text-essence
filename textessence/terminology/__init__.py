from .terminology_data_models import *
import configparser

class TerminologyEnvironment:
    base_config = None
    base_config_f = None
    term_config = None
    term_config_f = None
    terminology_collection = None
    terminology = None

def initializeTerminologyEnvironment(config_f, terminology):
    env = TerminologyEnvironment()

    env.base_config = configparser.ConfigParser()
    env.base_config_f = config_f
    env.base_config.read(env.base_config_f)

    env.term_config = configparser.ConfigParser()
    env.term_config_f = env.base_config['General']['TerminologyConfig']
    env.term_config.read(env.term_config_f)

    root_dir = env.term_config['Default']['RootDirectory']
    env.terminology_collection = TerminologyCollection(root_dir)
    env.terminology = env.terminology_collection.get(terminology, None)

    return env
