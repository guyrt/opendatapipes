"""Cleanup insulin records."""


drug_prompt = """
For each string below, you must output either "Lantus" or "Novalog". Never output an string other than Lantus or Novalog. Output exactly one answer for every input.

Inputs:
"""


notes_cleanup = """
Each line of input is a raw human input. Your job is to extract several pieces of information into a structured json output. The information you should extract includes:
* Was this a correction dose? Example strings are "correct at" and "control". This is Boolean.
* Location. This is always either 'butt' or 'stomach'. If you aren't sure or the input doesn't say, then say 'stomach'. If the input says "side" then return "stomach".
* "new_pen" if there was a new pen? The input will always say whether there was a new pen. This is Boolean. Only include in the output if the value is True. If there was a new pen, the input will always say that there was a new pen. Do not output True unless the prompt says there is a new pen.

Here are some examples:
Inputs:
1: "Tight control at 175"
2: "Side. New Pen"
3: "Rice. Topped at 110 them long low. Try 2? None?"

Outputs:
1: {'correction': True, 'location': 'stomach'}
2: {'correction': False, 'location': 'stomach', 'new_pen': True}
3: {'correction': False, 'location': 'stomach'}


Inputs:
"""


def get_drug_conversions(drug_types_raw):
    """My drug names are a mess. Get GPT to guess what each string means then apply."""
    pass
