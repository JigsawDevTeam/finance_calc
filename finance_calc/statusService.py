import requests
import traceback
import json

def sendProductFetchingStatus(apiLink, data):
    # apiLink = os.environ.get('API_URL')
    try:
        print('productFetchingStatus', data)
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post(
            f"{apiLink}/products/productupdatefailure", data=json.dumps(data), headers=headers)
        return response.json()
    except Exception as e:
        print(
            f"Error: {traceback.format_exc()}")
        return None