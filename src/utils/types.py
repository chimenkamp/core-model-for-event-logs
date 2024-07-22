from abc import ABC, abstractmethod
from typing import List

import src


class CCMEntry(ABC):
    """
    Abstract base class to enforce serialization implementation.
    """

    def __init__(self) -> None:
        self.attributes: List["src.classes_.Attribute"] = []

    def add_attribute(self, attribute: "src.classes_.Attribute") -> None:
        self.attributes.append(attribute)

    @abstractmethod
    def serialize(self) -> dict:
        pass
