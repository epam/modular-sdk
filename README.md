[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

[![PyPI - Version](https://img.shields.io/pypi/v/modular-sdk.svg)](https://pypi.org/project/modular-sdk)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/modular-sdk.svg)](https://pypi.org/project/modular-sdk)


# Modular SDK

You use Modular SDK to create, configure, and manage common entities, such as:
- Customers;
- Tenants;
- TenantSettings;
- Regions;
- Applications;
- Parents;

### Prerequisites
- Python 3.10+ required

### [Building distribution archives](https://packaging.python.org/en/latest/tutorials/packaging-projects/#generating-distribution-archives)
- Make sure you have the latest version of PyPA’s build installed: `python3 -m pip install --upgrade build`
- Run the command form the same directory where `pyptoject.toml` is located: `python3 -m build`
- This command should output a lot of text and once completed should generate two files in the dist directory:
```
dist/
    modular_sdk-{version}.tar.gz
    modular_sdk-{version}-py3-none-any.whl
```

### Installation
- `pip install modular-sdk` 

### Usage

To use Modular SDK, you must import, configure and indicate which service or 
services you're going to use:

```python
from modular_sdk.modular import Modular

modular_sdk = Modular()

parent_service = modular_sdk.parent_service()
tenant_service = modular_sdk.tenant_service()
```
For now, Modular provides 3 ways of DB access. Depends on the way of database 
connection, different sets of env variables must be set:
1. Onprem, Mongodb:  
   `modular_service_mode`: `docker`,  
   `modular_mongo_user`: `$MONGO_USER`,  
   `modular_mongo_password`: `$MONGO_PASSWORD`,  
   `modular_mongo_url`: `$MONGO_URL`,  
   `modular_mongo_db_name`: `$MONGO_DB_NAME`,  
2. SaaS, DynamoDB (cross-account access):  
   `modular_service_mode`: `saas` # Optional  
   `modular_assume_role_arn`: `$ASSUME_ROLE_ARN`  
3. SAAS, DynamoDB (same AWS account):  
   `modular_service_mode`: `saas` # Optional  
   
Alternatively, you can pass these parameters (fully or partially) on 
initialization:

```python
from modular_sdk.modular import Modular

modular_sdk = Modular(modular_service_mode='docker',
                      modular_mongo_user='MONGO_USER',
                      modular_mongo_password='MONGO_PASSWORD',
                      modular_mongo_url='MONGO_URL',
                      modular_mongo_db_name='DB_NAME')

# initialize some services
application_service = modular_sdk.application_service()
parent_service = modular_sdk.parent_service()
```

#### More examples
Example 1: List Maestro Customers

```python
from modular_sdk.modular import Modular

# initializing application service
customer_service = Modular().customer_service()

# listing available customers 
customers = customer_service.list()

# printing customer names
for customer in customers:
   print(customer.name)
```


Example 2: Update Maestro Application

```python
from modular_sdk.modular import Modular

# initializing application service
application_service = Modular().application_service()

# extracting application by id
application = application_service.get_application_by_id(
   application_id="$APP_ID")

# updating application description
description_to_set = 'Updated application description'
application_service.update(application=application,
                           description=description_to_set)

# saving updated application
application_service.save(application=application)
```

Example 3: Delete Maestro Parent

```python
from modular_sdk.modular import Modular

# initializing parent service
parent_service = Modular().parent_service()

# extracting parent by id
parent = parent_service.get_parent_by_id(parent_id="$PARENT_ID")

# deleting parent
parent_service.mark_deleted(parent=parent)
```
