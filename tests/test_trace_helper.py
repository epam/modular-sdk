import pytest

from modular_sdk.commons.trace_helper import (
    __get_arg_or_kwarg,
    __resolve_event,
    __resolve_context,
)

class MockContext:
    def __init__(self, aws_request_id: str):
        self.aws_request_id = aws_request_id


@pytest.mark.parametrize(
    'args,kwargs,pos,key,expected', 
    [
        (('event_val',), {}, 0, 'event', 'event_val'),
        (('event_val', 'context_val'), {}, 1, 'context', 'context_val'),
        ((), {'event': 'from_kwargs'}, 0, 'event', 'from_kwargs'),
        ((), {'context': MockContext('id')}, 1, 'context', MockContext('id')),
        (('from_args',), {'event': 'from_kwargs'}, 0, 'event', 'from_args'),  # prefers args
        ((), {}, 0, 'event', None),  # missing
        ((), {'event': None}, 0, 'event', None),  # explicit None
    ],
)
def test_get_arg_or_kwarg(args, kwargs, pos, key, expected):
    """Test __get_arg_or_kwarg returns correct value from args or kwargs"""
    result = __get_arg_or_kwarg(args, kwargs, pos, key)
    
    if isinstance(expected, MockContext):
        assert isinstance(result, MockContext)
        assert result.aws_request_id == expected.aws_request_id
    else:
        assert result == expected


@pytest.mark.parametrize(
    'args,kwargs,expected', 
    [
        (({'key': 'value'},), {}, {'key': 'value'}),
        ((), {'event': {'key': 'value'}}, {'key': 'value'}),
        (({'from': 'args'},), {'event': {'from': 'kwargs'}}, {'from': 'args'}),  # prefers args
        ((), {}, None),  # missing
        ((), {'event': None}, None),  # explicit None
        (('not_a_mapping',), {}, None),  # string instead of Mapping
        (([1, 2, 3],), {}, None),  # list instead of Mapping
        ((123,), {}, None),  # int instead of Mapping
        ((), {'event': 'string'}, None),  # string in kwargs
        ((), {'event': ['list']}, None),  # list in kwargs
    ],
)
def test_resolve_event(args, kwargs, expected):
    """Test __resolve_event returns event from args[0] or kwargs['event']"""
    result = __resolve_event(args, kwargs)
    assert result == expected


@pytest.mark.parametrize(
    'args,kwargs,expected_id', 
    [
        (({}, MockContext('id-123')), {}, 'id-123'),
        ((), {'context': MockContext('id-456')}, 'id-456'),
        (({}, MockContext('from-args')), {'context': MockContext('from-kwargs')}, 'from-args'),  # prefers args
        ((), {}, None),  # missing
        ((), {'context': None}, None),  # explicit None
        (({}, 'not_a_context'), {}, None),  # string instead of Context
        (({}, {'key': 'value'}), {}, None),  # dict without aws_request_id
        (({}, 123), {}, None),  # int instead of Context
        ((), {'context': 'string'}, None),  # string in kwargs
        ((), {'context': {'no_request_id': 'here'}}, None),  # dict in kwargs without aws_request_id
        (({}, []), {}, None),  # list instead of Context
    ],
)
def test_resolve_context(args, kwargs, expected_id):
    """Test __resolve_context returns context from args[1] or kwargs['context']"""
    result = __resolve_context(args, kwargs)
    
    if expected_id is None:
        assert result is None
    else:
        assert result is not None
        assert result.aws_request_id == expected_id
