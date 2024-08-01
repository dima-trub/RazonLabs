from enum import Enum


class GoogleScopes(Enum):
    cloud_platform= "https://www.googleapis.com/auth/cloud-platform"
    drive="https://www.googleapis.com/auth/drive"
    bigquery="https://www.googleapis.com/auth/bigquery"


# list_of_scopes_values=[e.value for e in google_scopes]
# print(list_of_scopes_values)

