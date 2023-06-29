from abc import ABC, abstractmethod

from modular_sdk.commons import ModularException, RESPONSE_BAD_REQUEST_CODE

ALLOWED_MODES = ["LIVE", "MAINTENANCE", "TESTING"]


class AbstractOperationModeDescriber(ABC):

    @abstractmethod
    def get_mode(self, component_name):
        pass


class AbstractOperationModeManager(AbstractOperationModeDescriber):

    @abstractmethod
    def set_mode(self, mode, component_name, applied_by, description, meta):
        pass

    @abstractmethod
    def check_mode(self, mode):
        if mode not in ALLOWED_MODES:
            raise ModularException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f"Wrong mode '{mode}' selected. Allowed modes are: "
                        f"'{ALLOWED_MODES}'"
            )
