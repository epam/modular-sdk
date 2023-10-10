import argparse
import logging
import logging.config
import sys
from abc import ABC, abstractmethod
from typing import Callable, List, Tuple, Generator

from modular_sdk.commons.constants import ALLOWED_TENANT_PARENT_MAP_KEYS, \
    ParentScope, COMPOUND_KEYS_SEPARATOR
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant

ACTION_DESTINATION = 'action'
PATCH_ALL_SCOPE_ACTION = 'all_scope'
PATCH_SPECIFIC_SCOPE_ACTION = 'specific_scope'

QUESTION1 = 'Do you really want to patch the parent? Although it has scope ' \
            'SPECIFIC multiple tenants can be linked to it. If you proceed ' \
            'the patch only the linkage with {tenant} will remain. ' \
            'Others WILL be destroyed!'
QUESTION2 = 'Parent has multiple clouds in its scope. ' \
            'Multiple clouds cannot be patched. Do you want to allow ' \
            'this parent for all the tenants independently on cloud?'


def get_logger():
    config = {
        'version': 1,
        'disable_existing_loggers': True
    }
    logging.config.dictConfig(config)
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


class ColorLog:
    def __init__(self):
        self._log = get_logger()

    def info(self, msg):
        self._log.info(TermColor.green(msg))

    def debug(self, msg):
        self._log.debug(TermColor.gray(msg))

    def warning(self, msg):
        self._log.warning(TermColor.yellow(msg))

    def error(self, msg):
        self._log.error(TermColor.red(msg))


_LOG = ColorLog()


class TermColor:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    DEBUG = '\033[90m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    @classmethod
    def blue(cls, st: str) -> str:
        return f'{cls.OKBLUE}{st}{cls.ENDC}'

    @classmethod
    def cyan(cls, st: str) -> str:
        return f'{cls.OKCYAN}{st}{cls.ENDC}'

    @classmethod
    def green(cls, st: str) -> str:
        return f'{cls.OKGREEN}{st}{cls.ENDC}'

    @classmethod
    def yellow(cls, st: str) -> str:
        return f'{cls.WARNING}{st}{cls.ENDC}'

    @classmethod
    def red(cls, st: str) -> str:
        return f'{cls.FAIL}{st}{cls.ENDC}'

    @classmethod
    def gray(cls, st: str) -> str:
        return f'{cls.DEBUG}{st}{cls.DEBUG}'


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Parent scope patch entrypoint'
    )
    sub_parsers = parser.add_subparsers(
        dest=ACTION_DESTINATION, required=True, help='Available actions'
    )
    _ = sub_parsers.add_parser(
        PATCH_ALL_SCOPE_ACTION,
        help='Patch parents with scope ALL'
    )
    patch_specific = sub_parsers.add_parser(
        PATCH_SPECIFIC_SCOPE_ACTION,
        help='Patch parents with type SPECIFIC'
    )
    patch_specific.add_argument(
        '--tenant_names', nargs='+', required=True, type=str,
        help='Tenants to patch their specific scope'
    )
    patch_specific.add_argument(
        '--types', nargs='+', required=False, type=str, default=[],
        choices=ALLOWED_TENANT_PARENT_MAP_KEYS
    )
    return parser


def query_yes_no(question, default="yes") -> bool:
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write(
                "Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


class ActionHandler(ABC):
    @abstractmethod
    def __call__(self, **kwargs):
        ...


class PatchAllScope(ActionHandler):
    def __call__(self):
        it = Parent.scan(
            rate_limit=1,
            filter_condition=(Parent.meta['scope'] == ParentScope.ALL.value)
        )
        for parent in it:
            _LOG.info(f'Going to patch parent: {parent.parent_id}')
            clouds = parent.meta.as_dict().get('clouds') or []
            if not clouds:
                _LOG.info('Parent has no clouds in its scope. '
                          'Patching with no clouds')
                parent.update(actions=[
                    Parent.type_scope.set(
                        f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.ALL}{COMPOUND_KEYS_SEPARATOR}')  # noqa
                ])
            elif len(clouds) == 1:
                _LOG.info('Parent has one cloud in its scope. '
                          'Patching with one clouds')
                parent.update(actions=[
                    Parent.type_scope.set(
                        f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.ALL}{COMPOUND_KEYS_SEPARATOR}{clouds[0]}')  # noqa
                ])
            else:  # multiple clouds
                if not query_yes_no(TermColor.blue(QUESTION2)):
                    _LOG.info('Skipping patch')
                    continue
                parent.update(actions=[
                    Parent.type_scope.set(
                        f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.ALL}{COMPOUND_KEYS_SEPARATOR}')  # noqa
                ])
            _LOG.info(f'Parent {parent.parent_id} was patched')


class PatchSpecificScope(ActionHandler):

    @staticmethod
    def iter_tenant_parents(tenant: Tenant, types: List[str]
                            ) -> Generator[str, None, None]:
        if not types:
            yield from filter(
                lambda x: bool(x), tenant.parent_map.as_dict().values()
            )
        else:
            pid = tenant.parent_map.as_dict()
            for t in types:
                if pid.get(t):
                    yield pid[t]

    def __call__(self, tenant_names: Tuple[str, ...], types: List[str]):
        _LOG.info('Going to patch specific scopes')
        if not types:
            _LOG.warning('Concrete types are not provided. All the found '
                         'types will be patched')
        for name in tenant_names:
            tenant = Tenant.get_nullable(name)
            if not tenant:
                _LOG.warning(f'{name} - not found. Skipping')
                continue
            for parent_id in self.iter_tenant_parents(tenant, types):
                _LOG.info(f'Going to patch parent: {parent_id}')
                parent = Parent.get_nullable(parent_id)
                if not parent:
                    _LOG.warning('Parent not found. Skipping')
                    continue
                if not query_yes_no(
                        TermColor.blue(QUESTION1.format(tenant=name))):
                    _LOG.info(f'Skipping patch')
                    continue
                parent.update(actions=[
                    Parent.type_scope.set(
                        f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.SPECIFIC}{COMPOUND_KEYS_SEPARATOR}{tenant.name}')  # noqa
                    # noqa
                ])
                _LOG.info(f'Parent {parent_id} was patched')
                # also we should remove scope and clouds from meta


def main():
    parser = build_parser()
    arguments = parser.parse_args()
    action = getattr(arguments, 'action', None)
    # action to handler
    mapping = {
        PATCH_ALL_SCOPE_ACTION: PatchAllScope(),
        PATCH_SPECIFIC_SCOPE_ACTION: PatchSpecificScope()
    }  # None is root action
    func: Callable = mapping.get(action) or (
        lambda **kwargs: _LOG.error('No handler'))
    if hasattr(arguments, 'action'):
        delattr(arguments, 'action')
    func(**vars(arguments))


if __name__ == '__main__':
    main()
