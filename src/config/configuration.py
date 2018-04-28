import json
import os

ROOT = '/Users/deus/Documents/workspace/dsys-master/dsys/src/'
CONFIGFILES = os.path.join(ROOT, 'config', 'config_files')


class BaseParser(object):

    @classmethod
    def parse_json(cls, file_name):
        with open(os.path.join(CONFIGFILES, file_name)) as json_data_file:
            return json.load(json_data_file)


database_configuration = BaseParser.parse_json('databaseconfig.json')
queues_configuration = BaseParser.parse_json('queuesconfig.json')
logging_configuration = BaseParser.parse_json('databaseconfig.json')
