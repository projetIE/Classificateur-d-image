import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient





def get_blob_name(config):

    container_client = ContainerClient.from_connection_string(conn_str=config["azure_storage_connectionstring"], container_name= config["data_container_name"])

    #print names blob
    List_blobnames =[]
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print(blob.name)
        List_blobnames.append(blob.name)
            

        

    return(List_blobnames)
    
    ######################################


def check_exist(List_blobnames):
    print(len(List_blobnames))
    lenght = len(List_blobnames)
    if (lenght%2 == 0):
        print("checkin....." +"\n")
        error = 0
        error_list = []
        for i in range(int(lenght/2)):
            
            List_blobnames[i][4:]
            List_blobnames[i + int(lenght/2) ][4:]
            
            
            if (List_blobnames[i][4:] == List_blobnames[i + int(lenght/2) ][5:]):
                print(List_blobnames[i][4:], "   ==    " , List_blobnames[i + int(lenght/2) ][5:] )
            else:
                
                
                error += 1
                print(List_blobnames[i][4:] , " =! ", List_blobnames[i + int(lenght/2)][5:] , "     error  :", error)
                error_list.append(List_blobnames[i])
        print(error, "\n" , error_list)
        return(error,error_list)
    
    
    
    
    
    
    
        
        


import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


config= {
    "azure_storage_connectionstring": "DefaultEndpointsProtocol=https;AccountName=fpoazureml012112873475;AccountKey=xQhyWFJWxvePcTevgLbLD0y72ZMTYEGH+T2ZanRqRXh/BVi1/szo5/bhf/EmDAlNcZYfSEAWYqs9v0yZ6ZM/cA==;EndpointSuffix=core.windows.net",
    "data_container_name": "perceuse-campagne-12-2022",                    #Container for the CSV on Azure     "perceuse-foret10-demo"
}

List_blobnames = get_blob_name(config)

try:  
    container_client = ContainerClient.from_connection_string(conn_str=config["azure_storage_connectionstring"], container_name= config["data_container_name"])
    
    blob_list = container_client.download_blob(List_blobnames[0])
    
    
    for blob in blob_list:
        #print(blob.name)
        List_blobnames.append(blob.name)
            
        
    with open(file="./data", mode="wb") as download_file:
        download_file.write(blob_list)
    


except:
    pass








    
    

 
 
