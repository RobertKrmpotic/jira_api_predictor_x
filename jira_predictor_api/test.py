import requests
BASE="http://127.0.0.1:8050/"
response = requests.get(BASE+"api/issue/ALGO-9999/resolve_fake")
print(response)
print(response.json())