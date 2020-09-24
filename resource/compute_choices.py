import dataiku

from dataiku.runnables import utils

user_client = dataiku.api_client()
user_auth_info = user_client.get_auth_info()
# Automatically create a privileged API key and obtain a privileged API client
# that has administrator privileges.
admin_client = utils.get_admin_dss_client("creation1", user_auth_info)

def do(payload, config, plugin_config, inputs):
    meaning_choices = [{"value": meaning['id'], "label":meaning['id']} for meaning in admin_client.list_meanings()]
    return {"choices": meaning_choices}

