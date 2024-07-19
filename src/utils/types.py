from abc import ABC, abstractmethod


class CCMEntry(ABC):
    """
    Abstract base class to enforce serialization implementation.
    """

    @abstractmethod
    def serialize(self) -> dict:
        pass