"""Cleanup insulin records."""
import openai
from typing import List, Dict, Any, AnyStr

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient



drug_prompt = """
For each string below, you must output either "Lantus" or "Novalog". Never output an string other than Lantus or Novalog. Output exactly one answer for every input.

Here are some examples:
Inputs:
1: l
2: L
3: lantis
4: n
5: N
6: novalo

Outputs:
1: Lantus
2: Lantus
3: Lantus
4: Novalog
5: Novalog
6: Novalog

Inputs:
{inputs}

Outputs:

"""


notes_cleanup = """
Each line of input is a raw human input. Your job is to extract several pieces of information into a structured json output. The information you should extract includes:
* Was this a correction dose? Example strings are "correct at" and "control". This is Boolean.
* Location. This is always either 'butt' or 'stomach' or 'split'. If you aren't sure or the input doesn't say, then say 'stomach'. If the input says "side" then return "stomach". Only use split if there are two numbers added together.
* "new_pen" if there was a new pen? The input will always say whether there was a new pen. This is Boolean. Only include in the output if the value is True. If there was a new pen, the input will always say that there was a new pen. Do not output True unless the prompt says there is a new pen.
* "split" if there are two numbers added like "3+4". In this case return "split": {{"stomach": 3, "butt": 4}}. Stomach will always be the first number. Butt will always be the second number.

Here are some examples:
Inputs:
1: "Tight control at 175"
2: "Side. New Pen"
3: "Rice. Topped at 110 them long low. Try 2? None?"
3: "2+4. Should have been 3+4"

Outputs:
1: {{'correction': True, 'location': 'stomach'}}
2: {{'correction': False, 'location': 'stomach', 'new_pen': True}}
3: {{'correction': False, 'location': 'stomach'}}
4: {{'correction': False, 'location': 'split', "split": {{"stomach": 3, "butt": }}}}

Inputs:
{inputs}

Outputs:
"""


def prep_openai_from_key(api_key):
    openai.api_key = api_key
    #print(f"Prepped openai and seeing models {openai.Model.list()}")


def run_openai_to_text(full_prompt : AnyStr) -> AnyStr:
    """Wrapper to eventually handle openai retry"""
    response = openai.Completion.create(
        model='text-davinci-003',
        prompt=full_prompt,
        temperature=0.7,
        max_tokens=3000,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    return response.choices[0].text
    

def prep_openai_from_keyvault(keyvault_url):
    """Get the openai api key to use and configure"""
    print(f"Getting secrets from {keyvault_url}")
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=keyvault_url, credential=credential)
    api_key = secret_client.get_secret("openai-api")  # should set this to be your secret key
    print(f"Key: |{api_key.value}|")
    prep_openai_from_key(api_key.value)


def get_drug_conversions(drug_types_raw):
    """My drug names are a mess. Get GPT to guess what each string means then apply."""
    drug_inputs = [f"{i}: {r}" for i, r in zip(range(1, len(drug_types_raw)+1), drug_types_raw)]

    full_prompt = drug_prompt.format(inputs='\n'.join(drug_inputs))
    raw_text = run_openai_to_text(full_prompt)
    outputs = [s.split(' ')[1] for s in raw_text.split("\n")]
    
    if len(drug_types_raw) != len(outputs):
        raise Exception(f"Different number of outputs than inputs: {len(drug_types_raw)} inputs and {len(outputs)} outputs")

    ret_val = {k: v for k, v in zip(drug_types_raw, outputs)}

    for k, v in ret_val.items():
        if v not in ("Lantus", "Novalog"):
            raise Exception(f"Value {k} translated to {v} which is not allowed.")
    return ret_val, "1.0.0_tdv003"


def convert_notes(all_rows) -> List[Dict[Any, Any]]:
    """Convert all raw notes to structured outputs."""
    group_size = 40
    row_groups = [all_rows[i:i+group_size] for i in range(0, len(all_rows), group_size)]

    for group in row_groups:
        input_rows = [f"{i}: {g['Notes']}" for i, g in zip(range(1, len(group)+1), group)]
        full_prompt = notes_cleanup.format(inputs='\n'.join(input_rows))
        response = run_openai_to_text(full_prompt)
        import pdb; pdb.set_trace()
        ###(Pdb) response.split('\n')[0]
###"1: {'correction': False, 'location': 'stomach', 'new_pen': True}"
###(Pdb) group[0]
###{'Date': Timestamp('2022-11-07 00:00:00'), 'Time': datetime.time(19, 57), 'Type': 'Lantis', 'Units': 10, 'Notes': 'New pen', 'timezone': 'PST', 'had_timezone': True, 'timestamp': datetime.datetime(2022, 11, 7, 19, 57, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>), 'CleanType': 'Lantus', 'CleanTypeVersion': '1.0.0_tdv003'}
        for row in group:
            yield row
