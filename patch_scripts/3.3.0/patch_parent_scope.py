import argparse
import json
import logging
import logging.config
import sys
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, List, Tuple, Generator, Optional

from modular_sdk.commons.constants import ALLOWED_TENANT_PARENT_MAP_KEYS, \
    ParentScope, COMPOUND_KEYS_SEPARATOR
from modular_sdk.commons.time_helper import java_timestamp
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant

ACTION_DESTINATION = 'action'
PATCH_ALL_SCOPE_ACTION = 'all_scope'
PATCH_SPECIFIC_SCOPE_ACTION = 'specific_scope'

QUESTION1 = 'Do you really want to patch the parent? Although it has scope ' \
            'SPECIFIC multiple tenants can be linked to it. If you proceed ' \
            'the patch only the linkage with {tenant} will remain. ' \
            'Others WILL be destroyed!'
QUESTION2 = 'Parent has multiple clouds in its scope. A new parent for each ' \
            'cloud will be created. Do you agree?'


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


class ParentsCol:
    def __init__(self, filename: str):
        self._filename = Path(filename)

    def add(self, pid: str):
        if not self._filename.exists():
            data = set()
        else:
            with open(self._filename, 'r') as file:
                data = set(json.load(file))
        data.add(pid)
        with open(self._filename, 'w') as file:
            json.dump(list(data), file)


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
    @staticmethod
    def divide_by_cloud(parent: Parent) -> Generator[Parent, None, None]:
        """
        Creates a new parent with the same business logic
        :param parent:
        :return:
        """
        meta = parent.meta.as_dict()
        clouds = meta.pop('clouds', None) or []  # they must be
        meta.pop('scope', None)  # ALL
        for cloud in clouds:
            yield Parent(
                parent_id=str(uuid.uuid4()),
                customer_id=parent.customer_id,
                application_id=parent.application_id,
                type=parent.type,
                description=parent.description,
                meta=meta,
                is_deleted=False,
                creation_timestamp=java_timestamp(),
                type_scope=f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.ALL}{COMPOUND_KEYS_SEPARATOR}{cloud}'
                # noqa
            )

    def __call__(self):
        old_parents = ParentsCol('old_parents.json')
        new_parents = ParentsCol('new_parents.json')
        it = Parent.scan(
            rate_limit=1,
            filter_condition=(Parent.meta['scope'] == ParentScope.ALL.value)
        )
        for parent in it:
            _LOG.info(f'Going to patch '
                      f'parent: {parent.parent_id}:{parent.type}')
            clouds = parent.meta.as_dict().get('clouds') or []
            if not clouds:
                _LOG.info('Parent has no clouds in its scope. '
                          'Patching with no clouds')
                parent.update(actions=[
                    Parent.type_scope.set(
                        f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.ALL}{COMPOUND_KEYS_SEPARATOR}')
                    # noqa
                ])
            elif len(clouds) == 1:
                _LOG.info('Parent has one cloud in its scope. '
                          'Patching with one clouds')
                parent.update(actions=[
                    Parent.type_scope.set(
                        f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.ALL}{COMPOUND_KEYS_SEPARATOR}{clouds[0]}')
                    # noqa
                ])
            elif len(clouds) == 3:  # all clouds
                _LOG.info(f'Parent contains all the '
                          f'clouds: {", ".join(clouds)}. Can be patched')
                parent.update(actions=[
                    Parent.type_scope.set(
                        f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.ALL}{COMPOUND_KEYS_SEPARATOR}')
                    # noqa
                ])
            else:  # multiple clouds but not all
                if not query_yes_no(TermColor.blue(QUESTION2)):
                    _LOG.info('Skipping patch')
                    continue
                old_parents.add(parent.parent_id)
                _LOG.info(
                    f'Parent with id {parent.parent_id} won`t be changed')
                for copy in self.divide_by_cloud(parent):
                    _LOG.info(
                        f'Creating a new parent with id {copy.parent_id}')
                    copy.save()
                    new_parents.add(copy.parent_id)
            _LOG.info(f'Parent {parent.parent_id} was patched')


class PatchSpecificScope(ActionHandler):

    def __init__(self):
        self._parent_cache = {}

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

    def get_parent(self, pid: str) -> Optional[Parent]:
        if pid in self._parent_cache:
            return self._parent_cache[pid]
        parent = Parent.get_nullable(pid)
        if parent:
            self._parent_cache[pid] = parent
            return parent

    def __call__(self, tenant_names: Tuple[str, ...], types: List[str]):
        old_parents = ParentsCol('old_parents.json')
        new_parents = ParentsCol('new_parents.json')
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
                _LOG.info(f'Going to patch parent: {parent_id} for '
                          f'tenant: {name}')
                parent = self.get_parent(parent_id)
                if not parent:
                    _LOG.warning('Parent not found. Skipping')
                    continue
                old_parents.add(parent_id)
                meta = parent.meta.as_dict()
                meta.pop('scope', None)
                meta.pop('clouds', None)
                copy = Parent(
                    parent_id=str(uuid.uuid4()),
                    customer_id=parent.customer_id,
                    application_id=parent.application_id,
                    type=parent.type,
                    description=parent.description,
                    meta=meta,
                    is_deleted=False,
                    creation_timestamp=java_timestamp(),
                    type_scope=f'{parent.type}{COMPOUND_KEYS_SEPARATOR}{ParentScope.SPECIFIC}{COMPOUND_KEYS_SEPARATOR}{tenant.name}'
                )
                new_parents.add(copy.parent_id)
                _LOG.info(f'A new specific parent: '
                          f'{copy.parent_id}:{copy.type} will be created')
                copy.save()


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
