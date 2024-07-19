import pandas as pd

from src.classes_ import CCM, Event
from src.utils.synthetic_log_provider import SyntheticLogProvider

slp: SyntheticLogProvider = SyntheticLogProvider(log_length=10)

ccm: CCM = slp.generate_synthetic_log()
table: pd.DataFrame = ccm.get_extended_table()


