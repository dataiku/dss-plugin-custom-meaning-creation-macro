# This file is the actual code for the Python runnable create-meaning
from dataiku.runnables import Runnable
import dataiku

class PluginParamValidationError(ValueError):
    """Custom exception raised when the the plugin parameters chosen by the user are invalid"""

    pass

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        parameters = self.config
        action = parameters["action"]
        dataset_name = parameters['input_dataset_name']
        column = parameters['column_name']
        
        client = dataiku.api_client()
        
        dataset = dataiku.Dataset(dataset_name, self.project_key)
        value_list = list(dataset.get_dataframe(columns=[column])[column].unique())
                
        if action == "create_new":
            new_meaning_name = parameters["new_meaning_name"]
            list_existing_meanings = [meaning['id'].lower() for meaning in client.list_meanings()]
            if new_meaning_name.lower() in list_existing_meanings:
                raise PluginParamValidationError("Meaning name already exists")

            client.create_meaning(new_meaning_name, new_meaning_name, "VALUES_LIST",
                    values=value_list,
                    normalizationMode="EXACT"
            )
            
            return "Custom meaning successfully created."
        
        if action == "update":  
            meaning_to_update = parameters["meaning_name"]
            meaning = client.get_meaning(meaning_to_update)
            definition = meaning.get_definition()
            definition['values'] = value_list
            meaning.set_definition(definition)
            
            return "Custom meaning successfully updated."
                         