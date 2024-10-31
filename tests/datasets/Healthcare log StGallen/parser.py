import xml.etree.ElementTree as ET
import yaml
from typing import Union, Dict, List, Any
import xmltodict

if __name__ == "__main__":
    sample_xml_path = "full_log.txt"

    # Load xml as string
    with open(sample_xml_path, 'r') as file:
        xml_string = file.read()

    # Parse xml string to dict
    xml_dict = xmltodict.parse(xml_string)

    # log (dict) -> trace (list) -> i (dict len=2)
    # -> list (dict len=2) -> list -> i (dict len=3) -> string (list) -> i (dict len=2) -> (key value)

    for trace in xml_dict['log']['trace']:
        print("====== Process Event ======")

        for elem in trace["list"]["list"]["list"]:
            # StreamPoint
            timestamp = elem["date"]

            for event in elem['string']:
                print(event)
            print("======")
