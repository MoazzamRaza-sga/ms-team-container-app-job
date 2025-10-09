import os
tenant = os.getenv("tenant","")
client_id = os.getenv("client_id","")
 
 
client_screte = os.getenv("client_screte","")
# SGA_UPN = "sgacommittees@sganaturalgas.org"
sga_upn=os.getenv("SGA_UPN","")   # <-- you specify SQA here in the URL, not in the token
 
 
 
def get_details(key=""):
    details =  {
        "tenant":tenant,
        "client_id":client_id,
        "client_scret":client_screte,
        "sga_upn": sga_upn
    }
    return details[key]