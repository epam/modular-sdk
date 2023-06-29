RESPONSE_BAD_REQUEST_CODE = 400
RESPONSE_UNAUTHORIZED = 401
RESPONSE_FORBIDDEN_CODE = 403
RESPONSE_RESOURCE_NOT_FOUND_CODE = 404
RESPONSE_INVALID_METHOD_CODE = 405
RESPONSE_RESOURCE_CONFLICT_CODE = 409
RESPONSE_OK_CODE = 200
RESPONSE_INTERNAL_SERVER_ERROR = 500
RESPONSE_NOT_IMPLEMENTED = 501
RESPONSE_BAD_GATEWAY_CODE = 502
RESPONSE_CONFIGURATION_CODE = 503
RESPONSE_SERVICE_UNAVAILABLE_CODE = 503
RESPONSE_GATEWAY_TIMEOUT_CODE = 504
RESPONSE_INVALID_CONFIGURATION_CODE = 503

CONFIGURATION_ISSUES_ERROR_MESSAGE = 'Service is not able to serve requests ' \
                                     'due to configuration issues'
CONFIGURATION_TRANSIT_ISSUES_ERROR_MESSAGE = \
    'Service is not able to serve requests due to transit configuration issues'
INVALID_CREDENTIALS_PROVIDED_ERROR_MESSAGE = 'User does not exists or ' \
                                             'provided credentials are not ' \
                                             'valid'
MISSED_HEADERS_ERROR_MESSAGE = 'Authorization data is not provided. Missed ' \
                               'the following header(s): {headers}'
INVALID_PARAMETERS_ERROR_MESSAGE = 'The following attribute(s) violates the ' \
                                   'interface: {errors}'
TIMEOUT_ERROR_MESSAGE = 'Target service failed to respond'
INVALID_RESPONSE_ERROR_MESSAGE = 'Target service violated the ' \
                                         'interface'
RESOURCE_NOT_FOUND_MESSAGE = 'Requested resource not found'

ERROR_MESSAGE_MAP = {
    RESPONSE_BAD_REQUEST_CODE: 'Bad Request',
    RESPONSE_UNAUTHORIZED: 'Unauthorized',
    RESPONSE_FORBIDDEN_CODE: 'Forbidden',
    RESPONSE_RESOURCE_NOT_FOUND_CODE: 'Not Found',
    RESPONSE_RESOURCE_CONFLICT_CODE: 'Request conflicts with the server state',
    RESPONSE_INTERNAL_SERVER_ERROR: 'Internal server error',
    RESPONSE_CONFIGURATION_CODE: 'Service misconfiguration',
    RESPONSE_BAD_GATEWAY_CODE: 'Bad gateway',
    RESPONSE_GATEWAY_TIMEOUT_CODE: 'Gateway timeout',
    RESPONSE_INVALID_METHOD_CODE: 'Method not allowed'
}

