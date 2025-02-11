# import requests
# from requests_negotiate_sspi import HttpNegotiateAuth
#
# # SharePoint URL you want to access
# sharepoint_url = "https://shinewing.sharepoint.com"
#
# # Make a request using your Windows credentials (Integrated Auth)
# response = requests.get(sharepoint_url, auth=HttpNegotiateAuth())
#
# if response.status_code == 200:
#     print("Successfully authenticated using SSO!")
#     print(response.text)
# else:
#     print(f"Failed to authenticate: {response.status_code}")

import requests
from requests_ntlm import HttpNtlmAuth

# Replace with your SharePoint site and username/password (Windows credentials)
sharepoint_url = "https://shinewing.sharepoint.com"
username = "mshf\\zhump"
password = "Rabbitjumpsoverabush82@"

# Perform NTLM authentication
response = requests.get(sharepoint_url, auth=HttpNtlmAuth(username, password))

if response.status_code == 200:
    print("Successfully authenticated using NTLM!")
else:
    print(f"Failed to authenticate: {response.status_code}")
