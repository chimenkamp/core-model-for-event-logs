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

    def __repr__(self) -> str:
        """
        Generate a string representation of the object.
        :return: A string representation of the object.
        """
        attrs = {k: v for k, v in self.__dict__.items()}
        attr_str = ', '.join(f"{k}={v!r}" for k, v in attrs.items())
        return f"{self.__class__.__name__}({attr_str})"