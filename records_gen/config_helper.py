import configparser

class ConfigReader:
    def __init__(self):
        self.config = configparser.ConfigParser()

    def read_connection_string(self,
                               filename: str = 'config.ini',
                               section: str = 'Database',
                               key: str = 'connection_string') -> str:
        self.config.read(filename)
        return self.config[section][key]


