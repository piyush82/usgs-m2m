import json
from itertools import count
from datetime import timedelta

import requests
from getpass import getpass
import sys
import time
import re
import threading
import datetime
import os
import pandas as pd
import geopandas as gpd
from dask import delayed, compute
from dask.config import set as dask_set

import warnings
warnings.filterwarnings("ignore")

dask_set(num_workers=10)

# Send http request
def sendRequest(url, data, apiKey=None, exitIfNoResponse=True):
    """
    Send a request to an M2M endpoint and returns the parsed JSON response.

    Parameters:
    endpoint_url (str): The URL of the M2M endpoint
    payload (dict): The payload to be sent with the request

    Returns:
    dict: Parsed JSON response
    """

    json_data = json.dumps(data)

    if apiKey == None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}
        response = requests.post(url, json_data, headers=headers)

    try:
        httpStatusCode = response.status_code
        if response == None:
            print("No output from service")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        output = json.loads(response.text)
        if output['errorCode'] != None:
            print(output['errorCode'], "- ", output['errorMessage'])
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        if httpStatusCode == 404:
            print("404 Not Found")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        elif httpStatusCode == 401:
            print("401 Unauthorized")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        elif httpStatusCode == 400:
            print("Error Code", httpStatusCode)
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
    except Exception as e:
        response.close()
        print(e)
        if exitIfNoResponse:
            sys.exit()
        else:
            return False
    response.close()

    return output['data']


def downloadFile(url):
    sema.acquire()
    try:
        response = requests.get(url, stream=True)
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        print(f"    Downloading: {filename}...", )

        open(os.path.join(data_dir, filename), 'wb').write(response.content)
        print(f"    Done downloading: {filename}")
        sema.release()
    except Exception as e:
        print(f"\nFailed to download from {url}. Will try to re-download.")
        sema.release()
        #runDownload(threads, url)
        # need to send the failed download back to DASC framework

def downloadFileDesc(url):
    try:
        response = requests.get(url, stream=True)
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        print(f"    Downloading: {filename}...", )

        open(os.path.join(data_dir, filename), 'wb').write(response.content)
        print(f"    Done downloading: {filename}")
    except Exception as e:
        print(f"\nFailed to download from {url}. Will try to re-download.")
        # need to send the failed download back to DASC framework
        urls = []
        urls.append(url)
        runDownloadDask(urls)

def runDownload(threads, url):
    thread = threading.Thread(target=downloadFile, args=(url,))
    threads.append(thread)
    thread.start()


def runDownloadDask(urls):
    downloads = []
    for  url in urls:
        downloads.append(delayed(downloadFileDesc)(url))
    compute(*downloads)


data_dir = '/Volumes/External 2T/usgsm2m/data'
utils_dir = 'utils'
dirs = [ data_dir, utils_dir]

for d in dirs:
        if not os.path.exists(d):
            try:
                os.makedirs(d)
                print(f"Directory '{d}' created successfully.")
            except OSError as e:
                print(f"Error creating directory '{d}': {e}")
        else:
            print(f"Directory '{d}' already exists.")


maxthreads = 10 # Threads count for downloads, 10 for M2 Max
sema = threading.Semaphore(value=maxthreads)
label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time
threads = []

# -----------------------------------------------------
#
# lets read the credentials from the creds file
#
# -----------------------------------------------------

f = open("m2m-token.txt","r")
linelist = f.readlines()
username = ""
token = ""
if len(linelist) >= 2:
    token = linelist[0].strip()
    username = linelist[1].strip()
f.close()

# def prompt_ERS_login(serviceURL):
#     print("Logging in...\n")
#
#     p = ['Enter EROS Registration System (ERS) Username: ', 'Enter ERS Account Token: ']
#
#     # Use requests.post() to make the login request
#     response = requests.post(f"{serviceUrl}login-token", json={'username': getpass(prompt=p[0]), 'token': getpass(prompt=p[1])})
#
#     if response.status_code == 200:  # Check for successful response
#         apiKey = response.json()['data']
#         print('\nLogin Successful, API Key Received!')
#         headers = {'X-Auth-Token': apiKey}
#         return apiKey
#     else:
#         print("\nLogin was unsuccessful, please try again or create an account at: https://ers.cr.usgs.gov/register.")

print("Logging in...\n")

serviceUrl = "https://m2m.cr.usgs.gov/api/api/json/stable/"
login_payload = {'username' : username, 'token' : token}

apiKey = sendRequest(serviceUrl + "login-token", login_payload)

# serviceUrl = "https://m2m.cr.usgs.gov/api/api/json/stable/"
# apiKey = prompt_ERS_login(serviceUrl)

# Print the API key only if login was successful
if apiKey:
    print("\nAPI Key: " + apiKey + "\n")

fileType = 'bundle'
bandNames = {'SR_B2', 'QA_PIXEL', 'ST_B10'} # will not be used

# from geojson import Polygon, Feature, FeatureCollection, dump
#
# polygon = Polygon([[[-148.9555, 61.4834],\
#                   [-150.9495, 61.4834],\
#                   [-150.9495, 61.0031],\
#                   [-148.9555, 61.0031],\
#                   [-148.9555, 61.4834]]])
#
# features = []
# features.append(Feature(geometry=polygon, properties={"city":"Anchorage", "state": "Alaska"}))
#
# # add more features...
# # features.append(...)
#
# feature_collection = FeatureCollection(features)
#
# with open('./utils/Anchorage_Alaska_aoi.geojson', 'w') as f:
#     dump(feature_collection, f)

# aoi_geodf =  gpd.read_file('./utils/Anchorage_Alaska_aoi.geojson') #aoi geopandas dataframe
aoi_geodf =  gpd.read_file('./utils/czech_republic_aoi.geojson') #aoi geopandas dataframe

print (aoi_geodf.crs)

import folium
m = folium.Map(location=[aoi_geodf.centroid.y[0], aoi_geodf.centroid.x[0]], zoom_start=8, tiles="openstreetmap",\
              width="90%",height="90%", attributionControl=0) #add n estimate of where the center of the polygon would be located\
                                        #for the location [latitude longitude]

for _, r in aoi_geodf.iterrows():
    sim_geo = gpd.GeoSeries(r["geometry"]).simplify(tolerance=0.001)
    geo_j = sim_geo.to_json()
    geo_j = folium.GeoJson(data=geo_j, style_function=lambda x: {"fillColor": "blue"})
    geo_j.add_to(m)

m

datasetName = 'landsat_ot_c2_l2'

# spatialFilter = {'filterType' : 'geojson',
#                  'geoJson' : {'type': 'Polygon',\
#                               'coordinates': [[[-148.9555, 61.4834],\
#                                                [-150.9495, 61.4834],\
#                                                [-150.9495, 61.0031],\
#                                                [-148.9555, 61.0031],\
#                                                [-148.9555, 61.4834]]]}}

# ------------------------------------------------------------
#
# Spatial filter is a bounding box over Czech Rebublic
#
# -------------------------------------------------------------

spatialFilter = {'filterType' : 'geojson',
                 'geoJson' : {'type': 'Polygon',\
                              'coordinates': [[[12.10623635584355,51.02018426967837],[12.10623635584355,48.53477785265926],[18.842665628085456,48.53477785265926],[18.842665628085456,51.02018426967837],[12.10623635584355,51.02018426967837]]]}}
#---------------------------------------------------------------
#
# Define the temporal filter next
#
#----------------------------------------------------------------
temporalFilter = {'start' : '2013-07-01', 'end' : '2013-08-01'}

cloudCoverFilter = {'min' : 0, 'max' : 100} #do not filter based on cloud cover

search_payload = {
    'datasetName' : datasetName,
    'sceneFilter' : {
        'spatialFilter' : spatialFilter,
        'acquisitionFilter' : temporalFilter,
        'cloudCoverFilter' : cloudCoverFilter
    }
}

# lets start tracking time needed
starttime = time.perf_counter()

# print(search_payload)

scenes = sendRequest(serviceUrl + "scene-search", search_payload, apiKey)

print(scenes['results'])

idField = 'entityId'

entityIds = []

for result in scenes['results']:
    # Add this scene to the list I would like to download if bulk is available
    if result['options']['bulk'] == True:
        entityIds.append(result[idField])
print("Count of downloadable entities available: ", len(entityIds))
print(entityIds)

listId = f"temp_{datasetName}_list" # customized list id

# # initial cleanup of the session if the files existed from past sessions
# # Now the cleanup process
# remove_scnlst_payload = {
#     "listId": listId
# }
# sendRequest(serviceUrl + "scene-list-remove", remove_scnlst_payload, apiKey)
#
# endpoint = "logout"
# if sendRequest(serviceUrl + endpoint, None, apiKey) == None:
#     print("\nLogged Out\n")
# else:
#     print("\nLogout Failed\n")
#
# #


scn_list_add_payload = {
    "listId": listId,
    'idField' : idField,
    "entityIds": entityIds,
    "datasetName": datasetName
}
print("Scene list payload to be added to M2M url:")
print(scn_list_add_payload)

count = sendRequest(serviceUrl + "scene-list-add", scn_list_add_payload, apiKey)
print("Available scenes for downloading:", count)

print("Confirming the scene list added for download")
print (sendRequest(serviceUrl + "scene-list-get", {'listId' : scn_list_add_payload['listId']}, apiKey))

download_opt_payload = {
    "listId": listId,
    "datasetName": datasetName
}

if fileType == 'band_group':
    download_opt_payload['includeSecondaryFileGroups'] = True

print("Available download options")
print(download_opt_payload)

products = sendRequest(serviceUrl + "download-options", download_opt_payload, apiKey)
print(pd.json_normalize(products))

fileGroups = sendRequest(serviceUrl + "dataset-file-groups", {'datasetName' : datasetName}, apiKey)
print(pd.json_normalize(fileGroups['secondary']))

print("Selecting products...")
downloads = []
if fileType == 'bundle':
    # Select bundle files
    print("    Selecting bundle files...")
    for product in products:
        if product["bulkAvailable"] and product['downloadSystem'] != 'folder':
            downloads.append({"entityId":product["entityId"], "productId":product["id"]})

if fileType != 'band_group':
    download_req2_payload = {
        "downloads": downloads,
        "label": label
    }

print(f"Sending download request ...")
download_request_results = sendRequest(serviceUrl + "download-request", download_req2_payload, apiKey)
print(f"Done sending download request")

if len(download_request_results['newRecords']) == 0 and len(download_request_results['duplicateProducts']) == 0:
    print('No records returned, please update your scenes or scene-search filter')
    sys.exit()

urls = []
# next start the scene downloads
# Attempt the download URLs
for result in download_request_results['availableDownloads']:
    # print(f"Get download url: {result['url']}\n")
    urls.append(result['url'])
    # runDownload(threads, result['url'])

preparingDownloadCount = len(download_request_results['preparingDownloads'])
preparingDownloadIds = []
if preparingDownloadCount > 0:
    for result in download_request_results['preparingDownloads']:
        preparingDownloadIds.append(result['downloadId'])

    download_ret_payload = {"label": label}
    # Retrieve download URLs
    print("Retrieving download urls...\n")
    download_retrieve_results = sendRequest(serviceUrl + "download-retrieve", download_ret_payload, apiKey, False)
    if download_retrieve_results != False:
        print(f"    Retrieved: \n")
        for result in download_retrieve_results['available']:
            if result['downloadId'] in preparingDownloadIds:
                preparingDownloadIds.remove(result['downloadId'])
                urls.append(result['url'])
                # runDownload(threads, result['url'])
                # print(f"       {result['url']}\n")

        for result in download_retrieve_results['requested']:
            if result['downloadId'] in preparingDownloadIds:
                preparingDownloadIds.remove(result['downloadId'])
                urls.append(result['url'])
                # runDownload(threads, result['url'])
                # print(f"       {result['url']}\n")

    # Didn't get all download URLs, retrieve again after 30 seconds
    while len(preparingDownloadIds) > 0:
        print(f"{len(preparingDownloadIds)} downloads are not available yet. Waiting for 30s to retrieve again\n")
        time.sleep(30)
        download_retrieve_results = sendRequest(serviceUrl + "download-retrieve", download_ret_payload, apiKey, False)
        if download_retrieve_results != False:
            for result in download_retrieve_results['available']:
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    # print(f"    Get download url: {result['url']}\n")
                    urls.append(result['url'])
                    # runDownload(threads, result['url'])

print("Here are all the files I need to download now:\n")
count = 0
for url in urls:
    count += 1
    print(count, ":", url)

# print("\nDownloading files... Please do not close the program\n")
# for thread in threads:
#     thread.join()

# Now sending the files to be downloaded with DASC framework
runDownloadDask(urls)

duration = timedelta(seconds=time.perf_counter()-starttime)
print("")
print('Job took: ', duration)

# Now the cleanup process
remove_scnlst_payload = {
    "listId": listId
}
sendRequest(serviceUrl + "scene-list-remove", remove_scnlst_payload, apiKey)

endpoint = "logout"
if sendRequest(serviceUrl + endpoint, None, apiKey) == None:
    print("\nLogged Out\n")
else:
    print("\nLogout Failed\n")

# listing the downloaded scenes
os.listdir(data_dir)