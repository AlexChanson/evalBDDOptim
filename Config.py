import configparser
import json

class Config:
    """
    A simple Singleton class.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self, *args):  # Constructor with a parameter
        if not hasattr(self, 'initialized'):
            config = configparser.ConfigParser()
            self.fpath = args[0]
            USER = args[1]
            config.read(self.fpath)

            # Database connection parameters
            self.dbname = config[USER]['dbname']
            self.user = config[USER]['user']
            self.password = config[USER]['password']
            self.host = config[USER]['host']
            self.port = int(config[USER]['port'])

            # psql
            self.psql = int(config[USER]['psql'])


            # files
            self.workload = config["Common"]['workload']
            self.student = config["Common"]['student_setup']
            self.analyse = config["Common"]['analyse']
#            self.prefs = json.loads(config.get("Common", "preferred"))

            self.initialized = True