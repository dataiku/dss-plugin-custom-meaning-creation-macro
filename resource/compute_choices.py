import dataiku

def do(payload, config, plugin_config, inputs):
    client = dataiku.api_client()
    meaning_choices = [{"value": meaning['id'], "label":meaning['id']} for meaning in client.list_meanings()]
    return {"choices": meaning_choices}

