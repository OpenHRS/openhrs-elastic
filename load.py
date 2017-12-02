import os, requests, json
from elasticsearch import Elasticsearch

es = Elasticsearch([os.environ["ELASTIC_URL"]], verify_certs=True)

if not es.ping():
    raise ValueError("Connection failed")
else:
    print("Successfully connected to: " + os.environ["ELASTIC_URL"] + "\n")

years = ["hrs2002","hrs2003"]
base_url = "https://raw.githubusercontent.com/OpenHRS/openhrs-data/master/"

for year in years:
    tree = requests.get(url=base_url+year+"/"+year+"_notext.json")
    payload = []

    index = {
        "index" : {
            "_index" : "hrs_statutes",
            "_type" : year[3:]
        }
    }

    print("begin load " + year)

    for div in tree.json():
        for title in div["titles"]:
            for chapt in title["chapters"]:

                doc = {
                    "year": year[3:],
                    "div_name": div["name"],
                    "div_num": div["number"],
                    "title_name": title["name"],
                    "title_num": title["number"],
                    "chapt_name": chapt["name"],
                    "chapt_num": chapt["number"],
                    "repealed": chapt["repealed"]
                }

                if (not chapt["repealed"]):
                    for sec in chapt["sections"]:
                        sec_path  = "../openhrs-data/" + year + "/division/"
                        sec_path += str(div["number"]) + "/title/" + str(title["number"]) + "/chapter/" 
                        sec_path += str(chapt["number"]) + "/section/" + str(chapt["number"]) + "-" 
                        sec_path += str(sec["number"]) + ".json"
                        
                        data = json.load(open(sec_path))

                        doc["section_name"] = data["name"]
                        doc["section_num"]  = data["number"]
                        doc["section_text"] = data["text"]

                        payload.append(index)
                        payload.append(doc)
                else:
                    payload.append(index)
                    payload.append(doc)
    
    es.bulk(payload)

    print("finish load " + year + "\n")
