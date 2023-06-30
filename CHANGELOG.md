# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

# [2.1.4] - 2023-06-30
* Update setup.cfg

# [2.1.3] - 2023-06-22
* RabbitMQApplicationMeta: change config user key to `maestro_user`

# [2.1.2] - 2023-06-21
* MaestroRabbitMQTransport: change config user key to `maestro_user`

# [2.1.1] - 2023-06-21
* imports unification: update imports to use `modular_sdk` as a root

# [2.1.0] - 2023-06-14
* fix a bug with mongoDB specific types in PynamoToMongoDB adapter. 
  Now method `from_json` performs the full deep deserialization of MongoDB 
  json the same way it does it to DynamoDB JSON. Unknown types will be proxied.

# [2.0.2] - 2023-05-29
* add `AccessMeta` class which declares common model for access data to 
  another api. It contains host, port, schema and path prefix.

# [2.0.1] - 2023-05-22
* added support of applications with type `RABBITMQ`. Added code to resolve 
  credentials for such application to maestro_credentials_service.
* tenant id is returned in `account_id` attribute instead of `project` in method `get_dto`;
* Add boolean validation for settings with `_ARE_` part in setting name
* Add integer validation for settings which ends with `_LONG` or `_INT`

# [2.0.0] - 2023-05-09
* redundant API of services for the following entities was removed: Application, Parent, Tenant, Region, Customer 

# [1.4.5] - 2023-05-04
* add an ability to assume a chain of roles before querying DynamoDB and SSM. 
  Now `modular_assume_role_arn` can contain a list or role arns, split by `,`
* rewrite `BaseSafeUpdateModel` so that it could handle nested map and 
  list attributes;
* implement `update` for `PynamoToMongoDbAdapter`

# [1.4.4] - 2023-05-03
* deem all the regions where `act` attribute is absent - active within an 
  active tenant
* add an ability to use dynamic account id for applications with type AWS_ROLE

# [1.4.3] - 2023-04-24
* added ability to send a batch of requests 

# [1.4.2] - 2023-04-20
* rework encryption and decryption flow for interaction with M3 Server via RabbitMQ

# [1.4.1] - 2023-04-19
* fixed sqs service: in case `queue_url` is not set to envs a message will 
  not be sent. A warning will be shown;

# [1.4.0] - 2023-04-10
* added `maestro_credentials_service` to retrieve credentials from 
  Maestro Applications;
* added `EnvironmentContext` class to export some environment variables 
  temporarily;

# [1.3.14] - 2023-04-07
* add more parameters to `i_get_parent_by_customer`: `is_deleted`, 
  `meta_conditions`, `limit`, `last_evaluted_key`;
* added `get_parent_id` to `Tenant` model;

# [1.3.13] - 2023-04-05
* add setting deletion date (`dd`) on parent/application delete
* remove conditions converted from PynamoToMongoDBAdapter and write a
  new one which supports all the existing conditions from PynamoDB as
  well as nested attributes.

# [1.3.12] - 2023-03-31
* tenant_service: implement methods to add/remove parent id from 
  tenant parent map (pid)
* add limit and last_evaluated_key attrs to get_tenants_by_parent_id. 
  Optimize parent_service.delete

# [1.3.11] - 2023-03-27
* rework validations for setting service in add/update actions

# [1.3.10] - 2023-03-24
* add `RIGHTSIZER` type to list of supported types of Parent 
* update dependency versions

# [1.3.9] - 2023-03-22
* add `RIGHTSIZER` type to list of supported types of Application 

# [1.3.8] - 2023-03-22
* add `MODULARJobs` table 

# [1.3.7] - 2023-03-21
* fix a bug associated with incorrect group name resolving for setting service

# [1.3.6] - 2023-03-17
* compress message if `compressed` parameter in `_build_message` method is `True`

# [1.3.5] - 2023-03-16
* hide `is_permitted_to_start` check before writing to `Jobs` table
* make `compressed` parameter in `pre_process_request` function optional 

# [1.3.4] - 2023-03-14
* add ability to specify `compressed` header for requests with zipped data
* add `attributes_to_get` param to `i_get_by_dntl` and `i_get_by_acc` functions

# [1.3.3] - 2023-02-22
* add ability to manage Settings collection items via setting service 
* Add log events into rabbit_transport service
* Raise version of the `cryptography` library: 3.4.7 -> 39.0.1

# [1.3.2] - 2023-02-01
* add ability to interact with Maestro server via remote execution service
* refactored a bit, added missing attributes to Application model. 
  Added `ModularAssumeRoleClient` descriptor which simulates boto3 client but 
  uses temp credentials obtained by assuming modular_assume_role_arn;
* Fix bugs with tenants, add an alternative method to create region
  (which does not perform full table scan) (currently experimental, used only by Custodian)
* Remove inner instance from `MODULAR` and use SingletonMeta. This makes to
  code clearer and IDEA hints work
* Added `get_region_by_native_name` to region service;

# [1.3.1] - 2023-01-16
* fix resolving of nested MapAttributes where Python's var name differs from 
  attribute's `attr_name` parameter for Tenant model in MongoDB;
* add `contacts`, `DisplayNameToLowerCloudIndex`, `ProjectIndex` to Tenants model


## [1.3.0] - 2022-12-13
* Add util to trace Jobs execution result 
* Add util to trace Runtime/ScheduleRuntime segment execution result

# [1.2.0] - 2022-11-22
* fix limit and last evaluated key in PynamoDBToPymongoAdapter and refactor a bit
* add `limit` and `last_evaluated_key` params to `i_get_tenant_by_customer` 
  and `scan_tenants` methods in tenant service;
* refactor `BaseModel` and add an ability for potential users of SDK to 
  use the PynamoToMongoAdapter and BaseModel from SDK, providing their 
  MongoDB credentials and url

# [1.1.1] - 2022-11-09
* fixed expiration in cross-account access;
* added `classproperty` decorator to helpers;
* added new possible env `modular_AWS_REGION`. It has priority over `AWS_REGION` 
  and can be useful if MODULAR Tables are situated in a different region from 
  the tool that uses MODULAR SDK;
* added functions `utc_iso` and `utc_datetime` instead of `get_iso_timestamp`;

# [1.1.0] - 2022-11-04
* added `customer_id` - `type` index to `Application` model;
* added `key` - `tenant_name` index to `TenantSettings` model;
* added `CUSTODIAN:CUSTOMER` to the list of available Parent types;
* Made `modular.models.pynamodb_extension.pynamodb_to_pymongo_adapter.Result` 
  iterator instead of list;
* updated `pynamodb` version to `5.2.3`;
* fixed bugs with tenant regions;
* added some method to services to query entities in different ways;

## [1.0.0] - 2022-10-03
### Added
    -  Initial version of Modular SDK.

