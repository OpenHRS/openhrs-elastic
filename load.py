import os, requests, json
from elasticsearch import Elasticsearch

es = Elasticsearch([os.environ["ELASTIC_URL"]], verify_certs=True)

if not es.ping():
    raise ValueError("Connection failed")
else:
    print("Successfully connected to: " + os.environ["ELASTIC_URL"])

years = ["hrs2002"]
base_url = "https://raw.githubusercontent.com/OpenHRS/openhrs-data/master/"

for year in years:
    tree = requests.get(url=base_url+year+"/"+year+"_notext.json")

    print("begin load " + year)

    for div in tree.json():
        for title in div["titles"]:
            for chap in title["chapters"]:
                if (not chap["repealed"]):
                    for sec in chap["sections"]:
                        sec_path  = "../openhrs-data/" + year + "/division/"
                        sec_path += str(div["number"]) + "/title/" + str(title["number"]) + "/chapter/" 
                        sec_path += str(chap["number"]) + "/section/" + str(chap["number"]) + "-" 
                        sec_path += str(sec["number"]) + ".json"
                        
                        data = json.load(open(sec_path))

                        

    print("finish load " + year)