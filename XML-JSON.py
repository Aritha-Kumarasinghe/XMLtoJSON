import json
import pymongo 
from pymongo import MongoClient
import xmltodict
import urllib3
import traceback



def xml_to_json():
    with open("01.xml") as xml_file:
        data_dict = xmltodict.parse(xml_file.read())
    xml_file.close()
	
	
    json_data = json.dumps(data_dict,indent=2)
	

    with open("data1.json", "w") as json_file:
        json_file.write(json_data)
        json_file.close()
    
    return 0

def open_json(file_name):
    with open(file_name) as file:
        file_data=json.load(file)
    return file_data

def connect_mongodb():
    uri = "mongodb+srv://Aritha_K:12345@cluster0.bjpmnx8.mongodb.net/test?ssl=true&ssl_cert_reqs=CERT_NONE"
 
    try:
        client = MongoClient(uri,
        connectTimeoutMS=30000,
        socketTimeoutMS=None)
        print("Connection successful")
    except:
        print("Unsuccessful")
    return client

def insert_mongodb_json():
    client = connect_mongodb()
    db=client["03"]
    collection=db["03"]
    collection.drop()
    collection.insert_one(open_json('data.json'))
    #print(collection.find_one())
    return collection


if __name__ == "__main__":
    xml_to_json()
    collection=insert_mongodb_json()
    query_1=collection.find_one({}, { "companies.company": { "$slice": [20, 1] } })
    print('Result of query one:')
    print(query_1["companies"]["company"][0]["name"])   
    print('Result of query two:')
    query_2=collection.find_one({}, { "companies.company": { "$slice": [11, 1] } })
    print(query_2)
    print("Result of 3:")
    pipeline = [
    {
        "$unwind": "$companies.company"
    },
    {
        "$unwind": "$companies.company.employees.employee"
    },
    {
        "$project": {
            "employee_name": "$companies.company.employees.employee.name",
            "company_name": "$companies.company.name"
        }
    }
    ]
    query_3 = collection.aggregate(pipeline)
    for r in query_3:
        if r["company_name"]== "Vipe":
            print(r["employee_name"])
