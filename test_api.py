import requests

url = "http://18.219.86.19/predict?vol_moving_avg={0}&adj_close_rolling_med={1}"

while True:
    vol_moving_avg = input("Enter vol_moving_avg: ")
    try:
        vol_moving_avg = int(vol_moving_avg)
        break
    except:
        print("Please enter a valid integer!")

while True:
    adj_close_rolling_med = input("Enter adj_close_rolling_med: ")
    try:
        adj_close_rolling_med = int(adj_close_rolling_med)
        break
    except:
        print("Please enter a valid integer!")

url = url.format(vol_moving_avg, adj_close_rolling_med)
resp = requests.get(url)
if resp.status_code == 200:
    print(resp.text)