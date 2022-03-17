from textessence.core.models.terminology_data_models import *
import configparser

class TerminologyEnvironment:
    base_config = None
    base_config_f = None

    terminologies_config = None
    terminologies_config_f = None

    terminology_name = None
    terminology_config = None

    terminology_collection = None
    terminology = None

def getTerminologyWorkingEnvironment(base_config_f, terminology_name):
    env = TerminologyEnvironment()

    env.base_config = configparser.ConfigParser()
    env.base_config_f = base_config_f
    env.base_config.read(env.base_config_f)

    env.terminologies_config = configparser.ConfigParser()
    env.terminologies_config_f = env.base_config['General']['TerminologyConfig']
    env.terminologies_config.read(env.terminologies_config_f)

    env.terminology_name = terminology_name
    env.terminology_config = env.terminologies_config[terminology_name]

    root_dir = env.terminologies_config['Default']['RootDirectory']
    env.terminology_collection = TerminologyCollection(root_dir)
    env.terminology = env.terminology_collection.get(terminology_name, None)

    return env
