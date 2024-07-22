import pandas as pd

from src.classes_ import CCM, Object, Attribute, IoTEvent, ProcessEvent, Activity, SOSA, IS

def create_ccm_env() -> CCM:


    return ccm


if __name__ == "__main__":
    ccm: CCM = create_ccm_env()

    table: pd.DataFrame = ccm.get_extended_table()
    ccm.visualize("use_case_init_short.png")
    print(table)