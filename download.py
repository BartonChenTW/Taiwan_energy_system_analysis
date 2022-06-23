import json, requests 

url = requests.get("https://data.tycg.gov.tw/opendata/datalist/datasetMeta/download?id=1ddb98f7-01b2-4558-93f2-9c79b2850d16&rid=cf17af2d-b0f6-42cf-b8dc-b4c9947b8c3e")
text = url.text

# data = json.loads(text)

# user = data[0]
# print(user['name'])

# address = user['address']
# print(address)

with open("TW_data_download.json", "w") as my_file:
    my_file.write(text)