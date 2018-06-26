from clients.AbstractClient import AbstractClient
import config

class QuickstepClient(AbstractClient):

    def __init__(self):
        raise Exception("Quickstep Client Not Implemented")

    def execute(self, query_text):
        raise Exception("Quickstep Client Not Implemented")

    def _result_to_string(self):
        raise Exception("Quickstep Client Not Implemented")