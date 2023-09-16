# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import schedule
import requests


def call_api_endpoint():
    call_command()
    call_command_processor()


def call_command():
    url = "http://localhost:5000/health"
    # Make the API request

    response = requests.get(url)
    if response.status_code == 200:
        print("API call command successful")
        # Process the API response data as needed
        # ...
    else:
        print(f"Failed to call API. Status code: {response.status_code}")


def call_command_processor():
    url = "http://localhost:5001/health"
    # Make the API request

    response = requests.get(url)
    if response.status_code == 200:
        print("API call command processor successful")
        # Process the API response data as needed
        # ...
    else:
        print(f"Failed to call API. Status code: {response.status_code}")


schedule.every(10).minutes.do(call_api_endpoint)

if __name__ == '__main__':
    call_api_endpoint()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
