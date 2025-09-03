import requests
import json
import datetime


BASE_URL = "https://apache-api.onrender.com/logs"
date = datetime.datetime.now().strftime("%Y-%m-%d")


def save_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)


def extract():
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()
        save_json(response.json(), f"api_output/new_data_{date}.json")
        return response.json()
        print("Data extracted and saved successfully.")
    except requests.RequestException as e:
        print(f"Error fetching data from {BASE_URL}: {e}")
        return {"error": str(e)}

# extract()