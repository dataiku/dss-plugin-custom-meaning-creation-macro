# This file is the actual code for the Python runnable create-meaning
from dataiku.runnables import Runnable, ResultTable
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
        
    def get_value_list(self, dataset, column):
        value_list = list(dataset.get_dataframe(columns=[column])[column].unique())
        
        return value_list
        
    def get_key_value_mapping(self, dataset):
        
        rt = ResultTable()
        rt.add_column("key", "key", "STRING")
        rt.add_column("value", "value", "STRING")
        
        row_list =[] 
        for index, rows in dataset.get_dataframe().iterrows(): 
            # Create list for the current row 
            my_list =[rows[self.config.get("key")], rows[self.config.get("val")]] 

            # append the list to the final list
            row_list.append(my_list)
            rt.add_record(my_list)
            
        mappings = [{"from": item[0], "to": item[1]} for item in row_list]
            
        return mappings, rt

        
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
        meaning_type = parameters["meaning_type"]
        dataset_name = parameters['input_dataset_name']
        column = parameters['column_name']
        
        client = dataiku.api_client()
        
        dataset = dataiku.Dataset(dataset_name, self.project_key)
                
        if action == "create_new":
            
            print(parameters)
            
            new_meaning_name = parameters["new_meaning_name"]
            list_existing_meanings = [meaning['id'].lower() for meaning in client.list_meanings()]
            if new_meaning_name.lower() in list_existing_meanings:
                raise PluginParamValidationError("Meaning name already exists")

            if meaning_type == "list_of_values":
                
                value_list = self.get_value_list(dataset, column)
                client.create_meaning(new_meaning_name, 
                                      new_meaning_name, 
                                      "VALUES_LIST",
                                      values=value_list,
                                      normalizationMode="EXACT"
                                      )
            
                return "Custom meaning successfully created."
            
            if meaning_type == "key_value_mapping":
                
                mappings, rt = self.get_key_value_mapping(dataset)
                    
                client.create_meaning(new_meaning_name.replace(" ","_"),
                                      new_meaning_name,
                                      "VALUES_MAPPING",
                                      mappings=mappings
                                     )
                
                return rt
        
        if action == "update":  
            
            meaning_to_update = parameters["meaning_name"]
            
            if meaning_type == "list_of_values":
                
                value_list = self.get_value_list(dataset, column)
                
                meaning = client.get_meaning(meaning_to_update)
                definition = meaning.get_definition()
                definition['values'] = value_list
                meaning.set_definition(definition)
                
                return "Custom meaning successfully created."

                
            if meaning_type == "key_value_mapping":
                                
                mappings, rt = self.get_key_value_mapping(dataset)
                
                meaning = client.get_meaning(meaning_to_update)
                definition = meaning.get_definition()
                definition['mappings'] = mappings
                meaning.set_definition(definition)
        
                return rt
                                    