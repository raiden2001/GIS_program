 #if checks version is 3 then it will move on or else it will exit the program
import sys


"""
The UpdateCgnToponumes python program has the responsability to dowmload
and process the CGNDB shapefile (english/French) from the NRCan Canadian Geographical Names Open Data http web site

Updated by Jason Ma Commons -2024-2025 for migration Python 3 and ARC GIS PRO syntax
"""


required_version = (3,1,0)
if sys.version_info < required_version:
    sys.exit(" The python version must be update to vrsion 3.1, the program must exit")

import os
import traceback
import threading
from urllib.parse import urlparse
import ftplib
from urllib.error import HTTPError
import shutil
from pathlib import Path 

#add the '../Commons' to the sys path so you can import from it and gets the direcotry of current file (updateCgn)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','Commons')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','DbUtility')))
import urllib3
import requests 
from asyncio import tasks
from urllib3.exceptions import MaxRetryError,SSLError
#from Commons import * 
from Commons import ClsRegistry
from Commons import GlobalVariables
from Commons import Tools
from Commons import Helper
from Commons import Configuration
from Commons import JobStatusManager
from Commons import Email2
#from Commons.Commons import ClsRegistry, Configuration, Email2, GlobalVariables, JobStatusManager,request, Tools
#from Commons import Tools
# COndition the code must run with condition
#import urllib
#import urllib2
from datetime import datetime
# import GeoProcessing
from GeoProcessing import GeoProcessing
import re 
import subprocess
import ast
#new libraries implemented 
from typing import Optional 
import traceback2
import logging
from typing import Dict,Callable,Tuple
import arcpy
#import getpass
try:
    #username = input("Enter your ArcGIS Online username:")
    #password = getpass.getpass("Enter your ArcGIS online password:")
    arcpy.SignInToPortal("https://www.arcgis.com","DUSS_Admin","L6UerJi3lWQdYSVLcMxI")
    #arcpy.SignInToPortal("https://www.arcgis.com/",username,password)
    print("Signed in successfully with Named User credentials License.")
except Exception as e:
    print(f"Sign-in failed, please try again!: {e}")
    sys.exit(1)
    
#Verify license level (e.g., 'arcview for Basic licensing)
#result = arcpy.CheckProduct('ArcView')
#result = arcpy.CheckProduct('ArcInfo') #No argument ; returns the 'arcview','arcinfo',etc
#if result:
 #   Tools.displayMessage(f'The Named User is verified and license is {result}',False,0,False)
    #Optional validation (adjust expected_level based on your license)
  #  expected_level = 'arcview' #Change to 'arcinfo' for Advnaced if applicable
   # if result != expected_level:
    #    Tools.displayMessage(f'Warning: License level {result} does not match expected {expected_level}',True,1,False)
     #   sys.exit(1)
#else:
    #print(f"License check failed, result is  not {result}")
    #sys.exit(1)
#print(f"This license is verified and it is {result} for the User")
print(f"This license verified and it is available for the User") # should output 'arcview' or your assigned level
#Tools.displayMessage(f'The Named User is verified and license is {result}',False,0,False)

#Added validation to ensure the license is as expected 
#expected_level ='ArcView' #Adjust based on your Named User license(e.g., 'ArcView' for Basic)
#if result == expected_level:
 #   Tools.displayMessage(f"License level is {result} and  matches expected level {expected_level}.",False,0,False) #Log as warning 
#else:
 ##  sys.exit(1) #Exit if license mismatch is critical
    
from Commons import DbUtilityClass 
from zipfile import ZipFile
from collections import OrderedDict
import smtplib
import asyncio
import json
import re
logs_to_attach = []

#Added validation to ensure the license is as expected 
#expected_level ='ArcView' #Adjust based on your Named User license(e.g., 'ArcView' for Basic)
#if result == expected_level:
   # Tools.displayMessage(f"License level is {result} and  matches expected level {expected_level}.",False,0,False) #Log as warning 
#else:
    #print("This {result} license no good")
    #sys.exit(1) #Exit if license mismatch is critical

#Function clean logs (removed duplicates and unwanted lines) 
def clean_log_file(log_path):
    if not os.path.exists(log_path):
        return
    with open(log_path,'r') as f:
        lines = f.readlines()
    unique_lines =[]
    prev_line = ""
    log_start_seen = False # Flag to track if we've seen the first [INFO] Log started at line
    for line in lines:
        stripped = line.strip()
        #skip all but the first [INFO] LOg started at lines
        #if stripped.startswith('[INFO] Log started at') and unique_lines and unique_lines[-1].strip().startswith('[INFO] Log started at'):
        if stripped.startswith('[INFO] Log started at Date and Time:'): #skips all but the first [INFO]
            if log_start_seen:
                continue
            log_start_seen = True
        #Existing filters for duplicates and standaline timestamps         
        if stripped == prev_line or stripped.startswith(('Using proactor:','Success: Operation','Deleted','Preparing to Append','Post-append row count','Finish all processing Append task!','Starting:','completed successfully')) and prev_line.strip().startswith(('Using proactor:','Success:Operation','Deleted','Preparing to Append','Post-append row count','Finish all processing Append task!','Starting:','completed successfully')): #fixes these duplicates logs and unwanted repeated strings 
            continue
        if 'Using proactor: IocpProactor' in stripped:
            #continue
        #if 'Success: Operation' in stripped and prev_line.strip().startswith('Success: Operation'):
           
            continue # skips the duplicate success messages 
            #removes standalone timestamp lines (e,g. "21:34:49")
        if re.match(r'^\d{2}:\d{2}:\d{2}$',stripped): 
        #Filter Standalone timestamp lines and/or duplicates with the time(fix the duplicates)
        #if re.search(r'^\d\d:\d\d:\d\d$',stripped) or (re.search(r' at \d\d:\d\d:\d\d$',stripped) and prev_line.strip().endswith(re.search(r' at \d\d:\d\d:\d\d$',stripped).group(0))):
            continue
        unique_lines.append(line) # Removes unique extra lines and open into the log path file and writes the lines in rows
        prev_line = stripped
    with open (log_path,'w') as f:
        f.writelines(unique_lines)

#Executing program name  
#sAppName = None
#pRegistry = None
#import shlex

sAppname = None
pRegistry = None
# global StatusManager
#sAppMessagesFile = None
sAppMessagesFile = r"F:\DUSS_SCRIPTS\UpdateCgnToponymes\messages_file\UpdateCgnToponymes_Messages.xml" # initialize globaly 
StatusManager = None  
def initialize(sLicenceType:str,sApp:str) -> bool:
        global sAppMessagesFile ,pRegistry,StatusManager # ensre all passed
        #sAppName ="UpdateCgnToponymes"
        sAppName = sApp
        #pRegistry = ClsRegistry(sAppName)#<-------added
        """
        Initialization logging, staus file and ArcGIS license verification 
        
        Returns
        ---------
        bool 
        True if initialization is successfully
        """

        try:
            #print("Starting initialization....")
            #GlobalVariables.gApplicationName = sApp

            
            pRegistry = ClsRegistry("UpdateCgnToponymes")
            #print(f"Set GlobalVariables.gApplicationName to: {GlobalVariables.gApplicationName}")
            ConfigApp = Configuration()
            #ConfigApp.init()
            if not ConfigApp.init():
                Tools.pEventLog.writeEventLog('W',f"Failed to initialize Configuration",event_type=1,eventID=1,category=5,descr=[f"COnfiguration initialization failed and use fallback"],data=None,sid1=None)
                return False

            #Create registry object
            
            #set up a log directory
            #sLogDirectory = ConfigApp.get_LogDirectory()[0] + sApp + '\\'
            #set to the direcotry for the correc path 
            sLogDirectory = r"F:\DUSS_ADMIN\log\UpdateCgnToponymes"
                #if not os.path.exists(sLogDirectory):
            # log_dir = ConfigApp.get_LogDirectory()
            # if not log_dir or not log_dir[0]:
            #     sLogDirectory = r"C:\DUSS_ADMIN\log\UpdateCgnToponymes"
            #if not os.path.isdir(sLogDirectory):
            #if not ConfigApp.init():
            #     #print("[ERROR] Configuration initialization failed")
            os.makedirs(sLogDirectory,exist_ok=True)
            Tools.pEventLog.writeEventLog('W',f"Failed to initialize Configuration and use fallback{sLogDirectory} ",event_type=1,eventID=1,category=5,descr=[f"COnfiguration initialization failed and use fallback:{sLogDirectory}"],data=None,sid1=None)
                #return False 
            # else:
            #     sLogDirectory = str(Path(log_dir[0]) / sApp)
            # #Validate sLogDirecotry for allowed characters 
            # os.makedirs(sLogDirectory,exist_ok=True)# esnure the log directory exists


            

            #Set XML messages file path 
            sAppMessagesFile = r"F:\DUSS_SCRIPTS\UpdateCgnToponymes\messages_file\UpdateCgnToponymes_Messages.xml"
            #Tools.pLogFile.writeToLog(f"[DEBUG] UpdateCgnToponymes.initiaize - sAppMessagesFile set to:{sAppMessagesFile}",True,0,False)
            if not os.path.isfile(sAppMessagesFile): #fix the XML issue with updateCGnTOponymes missing

            # #if not sAppMessagesFile.is_file():  --------- cute out for now 
                #print(f"Creating missing messages file:{sAppMessagesFile}")
                #Tools.displayMessage(f"Creating missing messages file: {sAppMessagesFile}",False,1,True) #change to Tools ouput
                #sAppMessagesFile.parent.mkdir(parent=True,exist_ok=True)
                #Verfiies whether  the directory exists
                os.makedirs(os.path.dirname(sAppMessagesFile),exist_ok=True)
                with open (sAppMessagesFile,'w',encoding='utf-8') as f:
                    f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<messages>\n'
                            '<data name="LogTitle:"><value>UpdateCgnToponymes Updating ~ </value></data>\n'
                            '<data name="ArcGIS_Available"><value>ArcGIS license available.</value></data>\n'
                            '<data name="ArcGIS_NoLicense"><value>No ArcGIS license available.</value></data>\n'
                            '<data name="PathNotExist"><value>The path does not exist: ~</value></data>\n'
                            '<data name="FileNotFound"><value>File not found: ~</value></data>\n'
                            '<data name="FileNotRead"><value>Failed to read file: ~</value></data>\n'
                            '<data name="DatabaseNotConnected"><value>Cannot connect to database: ~</value></data>\n'
                            '<data name="CountCheckStatus"><value>Count Check Status: ~</value></data>\n'
                            '<data name="DatabaseNotLoaded"><value>Failed to load database: ~</value></data>\n'
                            '<data name="DataUnchanged"><value>Data unchanged since last update, not Transfer required.</value></data>\n'
                            '<data name="UnzipError"><value>Failed to unzip file: ~</value></data>\n'
                            '<data name="FileNotWritten"><value>Failed to write to file: ~</value></data>\n'
                            '<data name="LogErrorOpen"><value>Error while opening log file: ~</value></data>\n'
                            '<data name="JobStatusNoWrite"><value>Can not write JobsStatus.xml!</value></data>\n'
                            '<data name="RegistryNotFound"><value>Cannot find registry key ~</value></data>\n'
                            '<data name="Er"><value>Update CGN Toponymes has not terminated normally, Please check the log file and/or Windows event log</value></data>\n'
                            '<data name="OK"><value>Update CGN Toponymes has terminated normally</value></data>\n'
                            '<data name="EmailBodyTitleSuccess"><value>Successfully completed the \'Update CGN Toponymes\' process.</value></data>\n'
                            '<data name="EmailBodyTitleError"><value>The Update CGN Toponymes process has not terminated normally. Please check log file and/or Windows event log.</value></data>\n'
                            '</messages>')
                Tools.pEventLog.writeEventLog('W',f"Messages file not found: {sAppMessagesFile}",event_type=1,eventID=1,category=5,descr=[f"Messages file not found: {sAppMessagesFile}"],data=None,sid1=None)  
                #logging.error(f"XML messages file does not exist:{sAppMessagesFile}")
                return False # prevents further disrupton of file creation
            
            #Create log file Example of parameters: E:\\common\\log\\UpdateFirstnationadata\\" "Dbutility", "E:\\anndc\\xmls\\DUSS\paratmers\\DBUTILITy\\DBUTILUTY\\DBUTIITY_Messages.xml,","UpdateReservesAndParcels"
            #print("Creating Log File........")
            sLogDirectory = str(Path(sLogDirectory).resolve()) #path normalize 
            #if not Tools.pLogFile.createLogFile(sLogDirectory,sApp,sAppMessagesFile,sApp):      #Removed sAppName,"UpdateCgnToponymes"
            if not Tools.pLogFile.createLogFile(sLogDirectory,sAppName,sAppName,sAppMessagesFile,sAppName):
                Tools.pEventLog.writeEventLog('E',f"Create App Directory for {sLogDirectory}",event_type=1,eventID=1,category=5,descr=[f"Registry key not found:{sLogDirectory}"],data=None,sid1=None)
                return False
            

            #Retrieve and customize job status file path and jobstatus key
            #job_path = ConfigApp.get_JobStatusFilePath()
            sJobsStatusFilePath = ConfigApp.get_JobStatusFilePath()[0] if ConfigApp.get_JobStatusFilePath() and ConfigApp.get_JobStatusFilePath()[0] else r"F:\DUSS_ADMIN\xmls\DUSS_parameters\JobsStatus\JobsStatus.xml"
            #print(f"INitiali JobStatusFilePath:{sJobsStatusFilePath}")
            #if not sJobsStatusFilePath or not os.path.isabs(sJobsStatusFilePath):
            if not sJobsStatusFilePath or not os.path.isabs(sJobsStatusFilePath):
                sJobsStatusFilePath = r"F:\DUSS_ADMIN\xmls\DUSS_parameters\JobsStatus\JobsStatus.xml"
                Tools.pLogFile.writeToLog(f"Invalid job status file path,using fallback:{sJobsStatusFilePath}",True,0,False)
                
            JsPath = os.path.dirname(sJobsStatusFilePath)
            JsFireName = os.path.basename(sJobsStatusFilePath)
            #print(f"JsPath: {JsPath},JsFireName:{JsFireName}")
            #assign new name
            #sJobsStatusFilePath =JsPath + f'\\UpdateCgnToponymes_' +JsFireName
            #sJobsStatusFilePath =  JsPath + '\\UpdateCgnToponymes' + JsFireName
            sJobsStatusFilePath = os.path.join(JsPath,f"UpdateCgnToponymes_{JsFireName}")
            # print(f"Final JobStatusFilePath:{sJobsStatusFilePath}")
            #print(f"Log directory:{sJobsStatusFilePath}") #debug
            
            if not os.path.isfile(sJobsStatusFilePath):
                os.makedirs(JsPath,exist_ok=True)
                #os.makedirs(JsPath,exist_ok=True)
                with open (sJobsStatusFilePath,'w',encoding='utf-8') as f:
                    f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<jobs>\n</jobs>')
                Tools.pLogFile.writeToLog(f"Job status file is missing:{sJobsStatusFilePath}",True,0,False)# change to 0 ,False
            #if not sJobsStatusFilePath.is_file():
                #Tools.pLogFile.writeToLog(f"Job status file is missing:{sAppMessagesFile},{sJobsStatusFilePath}",True,1,True)
                #return False
            #initiaize job status manager Reactiave GLOBAL variables 
            #global StatusManager,sAppName
            StatusManager = JobStatusManager()
            if not StatusManager.init(str(sJobsStatusFilePath),str(sAppMessagesFile),sAppName):
               Tools.pEventLog.writeEventLog('E',f"Failed to initialize the StatusManager ,using fallback:{sJobsStatusFilePath} ",event_type=1,eventID=1,category=5,descr=[f"Application not found in registry,using fallback:{sJobsStatusFilePath}"],data=None,sid1=None)
            #    print(f"Log directory:{StatusManager}") #debug
            #    print("Failed to initialize StatusManager")
               #Tools.displayMessage('Failed to initialize StatusManager',True)
               return False
            #Check ArchGIS license availability
            license_Status = Tools.checkOutLicense(sLicenceType)
            #Tools.pLogFile.writeToLog(f"[DEBUG] License check for {sLicenceType} returned: {license_Status}",True,0,False)
            if license_Status == "Available":
                #Tools.displayMessage("ArcGIS License available",False,0,False)
                Tools.displayMessage(Tools.getMessage(str(sAppMessagesFile),"ArcGIS_Available"))
                
            else:
                #Tools.displayMessage("No ArcGIS License available",False,0,False)
                Tools.displayMessage(Tools.getMessage(str(sAppMessagesFile),"ArcGIS_NoLicense"))
                return False
            Tools.displayMessage("Initialization ok",False,0,False) #output true logs 
            return True
        except Exception as e:
            #Tools.displayMessage(f"[Exception] Initialization failed: {e}\n{traceback2.format_exc()}",False,1,True)
            Tools.pLogFile.writeToLog(f"Initialization failed: {e}\n{traceback2.format_exc()}",True,1,True) #added out true logs 
            return False                                                
            
def run_process():
        """
        Runs the UpdateCgnToponymes process inside a separate thread for performance
        
        """
        thread = threading.Thread(target=_execute_update_process)
        thread.start()
        thread.join() 
        
def _execute_update_process():
        """
        Executes the update process 
        """
        
        try:
            Tools.displayMessage(f"running {sAppName} update process...")
            
            #SIMULATE long process 
            
            Tools.displayMessage(f"{sAppName} process completed successfully")
            
        except Exception as e:
            Tools.pEventLog.writeEventLog("E",f"Error in update process :{e}\n{traceback2.format_exc()}",event_type=1,eventID=1,category=5,descr=[f"Intiailization error:\nException {e}"],data=None,sid1=None)
    
def cleanup():
        """
        Cleans up the resources and relasease the arcgis 
        """
        Tools.releaseLicense()
        Tools.displayMessage("Resources cleaned up and Argis leince released")
     
            
    
    
def resetSourcedate():
        
        """
        Resets the LastSourceUpdatedDate registry key
        
        """
        pRegistry.writeRegistryKey('SourceUrls','LastSourceUpdatedDate','')

    #  def errorHandling(self):
    #      """
    #      Logs a fatal error and perofrms the cleanup process
    #      """
    #      Tools.pLogFile.writeToLog("[FATAL ERROR] Application. please check registry or configuration paths",True,1,True)
    #      self.cleanup()
     
        
def getLastDataUpdateDate():
    """
    Fetches the last update date form the HTML page using urllib3
        
    Returns
    -------
    str or None
        The extracted date string or NOne if the error occurs 
    """
        #initialization urllib3 PoolManager with retry logic and optional SSL verification
                                      # Retyr up to 3 times,#wait 1,2,4 seconds between retries, #Retry on these HTTP status codes          
    retry_strategy = urllib3.util.Retry(total=5,backoff_factor=2,status_forcelist=[429,500,502,503,504])

    #Disable SSL verification if needed (with a warning) #use CERT_None but ativate for now if production 
    #http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',retries=retry_strategy,timeout=10.0)
    proxy = os.environ.get('HTTP_PROXY') or os.environ.get('HTTPS_PROXY')
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',retries=retry_strategy, timeout=10.0,proxy_url=proxy) if proxy else urllib3.PoolManager(cert_reqs='CERT_REQUIRED',retries=retry_strategy,timeout=10.0)
    
    # if proxy:
    #     http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',retries=retry_strategy, timeout=10.0,proxy_url=proxy)
    # else:
    #     http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',retries=retry_strategy, timeout=10.0)
    
    #Tools.displayMessage(f"Warning: SSK verification is diabled. This is insecure",False,1)
    Tools.displayMessage(f"Getting the source Data's Date...",False,1)
    sCgnUrlEn_result = pRegistry.readRegistryKey('SourceUrls','GeobaseCgnEn')
    if not sCgnUrlEn_result or not sCgnUrlEn_result[0]:
        Tools.displayMessage("Error: Read Registry key 'SourceUrls\GeobaseCgnEn' not found",False,1)
        Tools.pLogFile.writeToLog(f"Registr key 'SourceUrls\\GeobaseCgnEn not found or empty",True,1,True )
        return None
        #Removes spacing obtaining data from URL NRCAN site
    sCgnUrlEn = sCgnUrlEn_result[0].strip()
    #if not sCgnUrlEn or not isinstance(sCgnUrlEn[0],str) or not sCgnUrlEn[0]:
    #if not isinstance(sCgnUrlEn,str) or not sCgnUrlEn:
    if not sCgnUrlEn:
        Tools.displayMessage(f"Error: Invalid or empty URL read from registry",False,1)
        return None
    #print(f"[DEBUG] Raw URL From registry:{sCgnUrlEn}")
    #Validates the URL
    # if not sCgnUrlEn:
    #     Tools.displayMessage("Error: Registry key 'SourceUrls\GeobaseCgnEn' not found",False,1)
    #     return None
    #clean the URL by stripping whitespace and ensuring it's a vlaid string
    #sCgnUrlEn = sCgnUrlEn[0].strip()
    # #print(f"[DEBUG] URL after strip:{sCgnUrlEn}")
    # if not sCgnUrlEn:
    #     Tools.displayMessage(f"Error: URL is empty after stripping not found",False,1)
    #     return None
    if not sCgnUrlEn.startswith(('http://','https://','ftp://')):
        Tools.displayMessage(f"Error:Invalid URL format:{sCgnUrlEn}",False,1)
        return None
    
    #Debug logging to inspect the URL
    #Tools.displayMessage(f"[DEBUG] Fetching URL: {sCgnUrlEn}",False,1)
    
    #parse the URL to determine the scheme
    parsed_url = urlparse(sCgnUrlEn)
    scheme = parsed_url.scheme.lower()

    #DeBug logging to the inspect the parsed scheme
    #Tools.displayMessage(f"[DEBUG] Parsed URL scheme:{scheme}",False,1)

    if not scheme:
        Tools.displayMessage(f"Error: No scheme found in URL:{sCgnUrlEn}",False,1)
        return None

    if scheme == 'ftp':
        #Handle FTP URLS with ftplib
        try:
            ftp = ftplib.FTP(parsed_url.hostname)
            ftp.login() # Anonymous login ; add credientials if need)
            ftp.cwd(parsed_url.path)
            #Get the modification time of the directoryfile 
            mod_time = ftp.sendcmd('MDTM' + parsed_url.path.split('/')[-1])
            ftp.quit()
            #Parse the modification time and status code 213 for connection establishment (format: YYYYMMDDHHMMSS)
            if mod_time.startswith('213'):
                date_str = mod_time[4:].strip()
                parsed_date = datetime.strptime(date_str,'%Y%m%d%H%M%S')
                sLastUpdateDate = parsed_date.strftime('%Y-%m-%d')
                Tools.displayMessage(f"Found the most recent Date from FTP:{sLastUpdateDate}",False,1)
                return sLastUpdateDate
            else:
                Tools.displayMessage("no Modification date found on FTP",False,1)
                return None
        except ftplib.all_errors as e:
            Tools.displayMessage(f"FTP Error:{e}",False,1)
            return None
        finally:
            if 'ftp' in locals():
                ftp.quit() # leaves after the ftp reached 
    elif scheme in ('http','https'):

    #sCgnUrlEn = sCgnUrlEn[0]
        try:
        # fetch the web page using urllib3
            response = http.request("GET",sCgnUrlEn,redirect=True)
            if response.status != 200:
                Tools.displayMessage(f"Http Error: Status {response.status} while fetching {sCgnUrlEn}",False,1)
                return None
            htmlPage = response.data.decode('utf-8') # decode the bytes to string 

        #Regualr expression to find dates
        # regex_numeric = fr'[0-9]{4}-[0-9]{2}-[0-9]{2}' #YYYY-MM-DD format
        # regex_text = fr'[0-9]{2}-[a-zA-Z]{3}-[0-9]{4}' #DD-MM-YYY format

            regex_numeric =r'\d{4}-\d{2}-\d{2}' #YYYY-MM-DD format
            regex_text = r'\d{2}-[a-zA-Z]{3}-\d{4}'# DD-MMM-YYYY text 

        #Extract dates from both formats
            lDates_numeric= list(set(re.findall(regex_numeric,htmlPage))) # removes duplicate
            lDates_text = list(set(re.findall(regex_text,htmlPage))) # remove duplicate

        #CCOmbine dates
            all_dates =  lDates_numeric + lDates_text
            if not all_dates:
                Tools.displayMessage("No dates found on the page",False,1)
                return None
        #Convert dates to a standard format(YYYY-MM-DD) for comparison (Year-Month-Date)
            standardized_dates = []
            for date_str in all_dates:
                try:
                    if '-' in date_str and len(date_str.split('-')[0]) == 4:
                        parsed_date = datetime.strptime(date_str,'%Y-%m-%d') # YYYY-MM-DD format 
                    else:
                        parsed_date = datetime.strptime(date_str,'%d-%b-%Y') #DD-MM-YYY format 
                    standardized_dates.append((parsed_date,parsed_date.strftime('%Y-%m-%d')))
                except ValueError as e:
                    Tools.displayMessage(f"Invalid date format: {date_str}, error:{e}",False,1)
                    continue
            if not standardized_dates:
                Tools.displayMessage("no valid dates could be parsed.",False,1)
                return None
        
        #Sorted by date (first element of tuple) and get the most recent
            standardized_dates.sort(key=lambda x: x[0], reverse=True)
            sLastUpdateDate = standardized_dates[0][1] #get the formatted date string (YYYY-MM-DD)
            Tools.displayMessage(f"Found the most recent Date:{sLastUpdateDate}",False,1) 
            return sLastUpdateDate #Uses Exceptional to check errors and number retries before boots the User away.
        except urllib3.exceptions.HTTPError as e:
            Tools.displayMessage(f"HTTP ERROR: {e}",False,1)
            return None
        except urllib3.exceptions.MaxRetryError as e:
            Tools.displayMessage(f"Max retries exceed for {sCgnUrlEn}: {e}",False,1)
            return None
        except urllib3.exceptions.SSLError as e:
            Tools.displayMessage(f"HTTP ERROR: {e}",False,1)
            return None
        except Exception as e:
            Tools.displayMessage(f"Error in getLastDataUpdateDate:\nException {e}\n{traceback2.format_exc()}",False,1)
            return None
        finally:
            http.clear() #clean up connection pool 
    else:
        Tools.displayMessage(f"Unsupported URL Scheme: {scheme}",False,1)
        return None

        
def checkLastDataUpdateDate(app=None):
        """
        The getdatabase and its data determines if the updates will take  effect
        
        Updated March 06,2025 
        
        Returns
        --------
        Optional{bool]
        -True -Data updates 
        - False - No update needed 
        -None -> Operation failed 
        """
        
        Tools.displayMessage("Comparing dates...",False,1)
        
        #Load bypass flag from config.json
        #bypass_data_update= False
        #config_path
        #Get Source data for date
        sDate = getLastDataUpdateDate()
        if sDate is None:
            return None #operation failed 
            
        #Retrieves Last update date from the registry safely
        sLastupdate = pRegistry.readRegistryKey("SourceUrls","LastSourceUpdatedDate")
        sLastupdate = sLastupdate[0] if sLastupdate else ""
        
        #if last update date is missing ,stores current date and changed from unicode to regular string where python 3 is build in so need for unicode 
        if not sLastupdate:
            pRegistry.writeRegistryKey("SourceUrls","LastSourceUpdatedDate",str(sDate))
            return True
            
        #if dates are different ,update and return True
        if sLastupdate not in sDate:
            pRegistry.writeRegistryKey("SourceUrls","LastSourceUpdatedDate",str(sDate))
            return True
        return False # No updates needed once return 
        
        
    

            
def saveToFolder(sUrl: str,sDlFullPath:str,dHeaders: dict,sProcessingGDB: Optional[str]) -> bool:
    """
        Downloads a file from the internet and saves it to the local folder
        
        Updated : March 06 2025 ( Python 3 Migration - Replacing  the old modules urllib2 to updated ArCGIS PRO support only urllib3)
        
        Paramters
        -----------
        sURl: str
          The download URL
          
        sD1FullPath : string(str)
        
        dHeaderss: dict 
         THe HTTP client headers (e.g., browser headers)
         
         Returns 
         ---------
         bool 
         True if the file was downloaded successful , False otherwise 
        
        
    """
             #initialization urllib3 PoolManager for handling HTTPS connections 
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED') # disable SSL verification if needed 'CERT_REQUIRED' 
        
    bDownload = False
    Block_size = 4096 #Improved constant naming
    Max_Retries = 7 # Use of constant for retry limit
    file_size_dl = 0 # file size 
    for attempt in range(1,Max_Retries +1):

        try:
            """
            """
            # Sends  HTTP GET request 
            Tools.displayMessage(f"Attempt {attempt} - Downloading: {sUrl}",False,1)
            response = http.request("GET",sUrl,headers=dHeaders,preload_content=False,timeout=30)
            #Check for successful response ok at 200KB 
            if response.status !=200:
                Tools.displayMessage(f"Error: HTTP error{response.status} while downloading to the {sUrl}:{response.reason}", False,1,False)
                response.release_conn() #closes connection prevents memory leaks 
                #time.sleep(30) if needs since timeoutgoers 10 seonds when going on the smpt and FTP and API 
                continue 
                #Get the file size 
            file_size = response.headers.get("content-length")
            file_size = int(file_size) if file_size else "Unknown"
                        
            #Outputs the file path when download completes
            Tools.displayMessage(f"Downloading :{sDlFullPath} | file size: {file_size} bytes",False,1,True)
            os.makedirs(os.path.dirname(sDlFullPath),exist_ok=True)            
                        # Ensure direcotry exisits before saving file
            #os.makedirs(os.path.dirname(os.path.dirname(sDlFullPath),exist_ok=True))
                        
            #The download file with progress tracking and saves to file
            with open(sDlFullPath,"wb") as file:
                while True:
                    buffer = response.read(Block_size)
                    if not buffer:#break
                        break
                    file.write(buffer) #This will raise an error because buffer is empty ,change to end to 
                    file_size_dl += len(buffer)
            response.release_conn() # close connection
            Tools.displayMessage(f"Download completed for the Datasets: {sDlFullPath}",False,1)
            if sProcessingGDB:
                try:
                    #To Extract ZIp file #fix the 
                    logging.info(f"sD1FullPath type:{type(sDlFullPath)},value{sDlFullPath}")
                    if str(sDlFullPath).endswith('.zip'):
                        extract_dir = os.path.splitext(str(sDlFullPath))[0]
                        with ZipFile(str(sDlFullPath),'r') as zip_ref:
                            zip_ref.extractall(extract_dir)

                        # find the shapefile when extracted to directory
                        for root,_,files in os.walk(extract_dir):
                            for file in files:
                                if file.endswith('.shp'):
                                    shp_path = os.path.join(root,file)
                                    gp = GeoProcessing()
                                    gp.setProcessingGDB(sProcessingGDB)
                                    gp.stageToGDB(shp_path)# future enterprise
                                    Tools.displayMessage(f"Staging Data is completed to GDB for: {sProcessingGDB}/TempLayer",False,1)
                                    break
                    else:
                        Tools.displayMessage(f"Error: Expected ZIP file, got {sDlFullPath}",False,1)
                    #arcpy.FeatureClassToFeatureClass_conversion(sDlFullPath,sProcessingGDB,"TempDownload")
                                #Tools.displayMessage(f"Staging completed to :{sProcessingGDB}/TempDownload",False,1)
                except Exception as stage_err:
                    Tools.displayMessage(f"Staging to GDB has failed: {stage_err}",False,1,False)
            bDownload = True
            break # Exit hte loop after succesful download
                        #update the here to improve the function to log error details
        except urllib3.exceptions.HTTPError as e:
            #Tools.displayMessage(f"HTTP ErrorL {e}",False,1,False)
            Tools.displayMessage(f"HTTP Error:{e} (Status:{response.status if 'response' in locals() else 'Unknown'})",False,1,False)
            continue
        except urllib3.exceptions.MaxRetryError as e:
            #Tools.displayMessage(f"Max retries exceed for {sUrl}: {e}",False,1,True )
            Tools.displayMessage(f"Max retries exceed for {sUrl}: {e.reason}",False,1,True )  
            continue 
        except urllib3.exceptions.SSLError as e:
            Tools.displayMessage(f"SSL Error: {e}",False,1,False)
            continue
        except Exception as e:
            Tools.displayMessage(f"HTTP Error in download https2: \nException {e}\n{traceback2.format_exc()}",False,1,False)    
            continue
    return bDownload           
                    
                        
                       
            

   
   
          
def downloadCgndbData() -> bool:
    """
    The part is to download CgndbData method and does CGNDB data download from the HTTP server site
    THe downloads CGNDB data from the HTTP server for both English & French versions)
    Improved: Python 3 Migration and ARcGIS PRo COmpatibility 
         
    Updated : March 07 2025        
             
    Returns 
    ---------
        bool 
        True if both files was downloaded successful , False otherwise 
            
    """
    Tools.displayMessage(f"Downloading CGNDB Data",False,1 ,True)
    Tools.displayMessage(f"Start Date: {Tools.getDateAndTime()} ",False,1,True) #Explicit timestamp, bDataTime = False ,added False
    try:
       #juste added  #make sure the gProcessingGDB 
        if not GlobalVariables.gProcessingGDB or not arcpy.Exists(GlobalVariables.gProcessingGDB):
            Tools.displayMessage(f"Error: Processing GDB not set or invalid:{GlobalVariables.gProcessingGDB}",False,1,True) #Added  True
            return False
        
        #English version
        urlEn = pRegistry.readRegistryKey('SourceUrls','GeobaseCgnEn')[0] + pRegistry.readRegistryKey("SourceUrls","ZipCgnDbEn")[0]
        Tools.displayMessage(f"The ENGLISH CGNDB will be downloaded FIRST with Datasets in order process in the next line: {urlEn}",False,1,True)
        #urlEn="http://ftp.geogratis.gc.ca/pub/nrcan_rncan/vector/geobase_cgn_toponyme/prov_shp_eng/cgn_canada_shp_eng.zip"
        #French version 
        urlFr = pRegistry.readRegistryKey('SourceUrls','GeobaseCgnFr')[0] + pRegistry.readRegistryKey("SourceUrls","ZipCgnDbFr")[0]
        Tools.displayMessage(f"The FRENCH CGNDB will be downloaded SECOND with Datasets in order process after the ENGLISH CGNDB: {urlFr}",False,1,True)
        dHeaders = ast.literal_eval(pRegistry.readRegistryKey('SourceUrls','HttpClientHeader')[0])
            
        #Read the shapefile efficiently during process and handles path from the windows directory 
        enShpPath = Path(GeoProc.getLocalEnShp()).parent
        frShpPath = Path(GeoProc.getLocalFrShp()).parent
        
            
        #Download CGN English Data,/ Path(urlEn).name              (just added Global)
        if saveToFolder(urlEn,enShpPath/Path(urlEn).name,dHeaders,GlobalVariables.gProcessingGDB):
            Tools.displayMessage(f"The English CGNDB Data download successfully at Time: {Tools.getTime()}.",False,1,True) #Adeed timestampe and added True 
        else:
            Tools.displayMessage(f"The max number of attempts reached. Try again later",False,1,True) #Added true
            return False # exits once program rach a max capacity
                                                                           #(just added Global)
        #Download CGNDB French data , removed the == True boolean expression since we defined already at function for efficient
        if saveToFolder(urlFr,frShpPath/Path(urlFr).name,dHeaders,GlobalVariables.gProcessingGDB):
            Tools.displayMessage(f"The French CGNDB Data download successfully at Time:{Tools.getTime()}",False,1,True) #added timestampe and added True
        else: 
            Tools.displayMessage(f"The Max number of attempts reached. Try Again Later",False,1,True) #added True
            return False #Exits once program reaches max capacity
           #returns the network if there is an error   
        Tools.displayMessage(f"End at Date: {Tools.getDateAndTime()}",False,1,True)
        return True 
           
    except requests.RequestException as e:
            Tools.displayMessage(f"Network error during CGNDB download: {e}\n{traceback2.format_exc()}",False,1)
            return False 
    except Exception as e:
            Tools.displayMessage(f"Unexpected error:\nException {e}\n{traceback2.format_exc()}",False,1)
            return False 
    #returns the the specific date and time     
    # Tools.displayMessage(f"End at: {Tools.getDateAndTime()}",True,1)
    # return True 
    """
        The downloadCgndbData method does the CGNDB data download from the HTTP server site.
        November 15, 2017

            Returns
            -------
                True or False : boolean
                    The file was saved or not.

        
        Tools.displayMessage('Downloading CGNDB data',False,1,True)
        Tools.displayMessage('Start at: ',True,1)
        urlFr = pRegistry.readRegistryKey('SourceUrls','GeobaseCgnFr')[0] + pRegistry.readRegistryKey('SourceUrls','ZipCgnDbFr')[0]
        urlEn = pRegistry.readRegistryKey('SourceUrls','GeobaseCgnEn')[0] + pRegistry.readRegistryKey('SourceUrls','ZipCgnDbEn')[0]
        dHeaders = ast.literal_eval(pRegistry.readRegistryKey('SourceUrls','HttpClientHeader')[0])
        frShpPath = os.path.dirname(GeoProc.getLocalFrShp())
        enShpPath = os.path.dirname(GeoProc.getLocalEnShp())
        #
        #Download CGNDB english
        #
        if saveToFolder(urlEn,enShpPath + '\\' + os.path.basename(urlEn),dHeaders) == True:
            Tools.displayMessage('Done.')
        else: 
            Tools.displayMessage('The maximal number of attempt has been reached. Try later.')
            return False
        #
        #Download CGNDB french
        if saveToFolder(urlFr, frShpPath + '\\' + os.path.basename(urlFr),dHeaders) == True:
            Tools.displayMessage('Done.')
        else: 
            Tools.displayMessage('The maximal number of attempt has been reached. Try later.')
            return False
        #
        Tools.displayMessage('End at:',True,1)
        return True
    """
def extract_zip(zip_path: str,dataset_name: str,sAppMessagesFile: str) -> bool:
    """
        This part of the function will help extract the zip files and to avoid any duplications code
        
        Parameters:
        -------------
        zip_path: string(str)
            The path tothe zip file to be extracted
        dataset_name : string(str)
          The name of dataset for logging
          
        returns:
        --------
        
        bool:
            True if extraction was successful,False otherwise
        zip_file = Path(zip_path) # converts to the path object 
        output_folder = zip_file.parent #exgtract the same direcotyr
        """
    zip_file = Path(zip_path)
    output_folder = zip_file.parent
    
    try:
       # output_folder = Path(zip_path).parent #Extract to the same directory

        logging.info(f"Extracting {dataset_name} from {zip_path} to {output_folder}")
        
        #Tools.unzip(sAppMessagesFile,zip_path,output_folder)
        #Using shutil.unpack_archive() (py3 native methods) #sAppMessagesFile
        shutil.unpack_archive(zip_path,output_folder)
        #logging.info(f"Successfully extracted{dataset_name}.")
        Tools.displayMessage(f"Successfully extracted {dataset_name}",False,1,True)
        return True
    except Exception as e:
        logging.error(f"Error extracting {dataset_name}: {e}\n{traceback2.format_exc()}")
        Tools.displayMessage(f"Error extracting {dataset_name}: {e}",False,1,True) #added to prevent return false sliently
        return False 
        
        
def extractingData() -> bool:
    """
        The part is to  extractingData method form the zipfiles once downloaded
        Updated : March 07 2025 (Python 3 migration methods for ARC GIS PRO)   
        Datesets CGNDB datasets (French & English) after downloading
         
         Returns 
         ---------
         bool 
         True if both files was downloaded successful , False otherwise 
    
    """
        #Outputs the and unzip the data 
        #Tools.displayMessage(f"Unziping CGNDB data",False,1,True)
    logging.info("Extracting CGNDB data...") 
    
    
        #read URLS from registry
    try:
        urlFr = pRegistry.readRegistryKey('SourceUrls','GeobaseCgnFr')[0] + pRegistry.readRegistryKey('SourceUrls','ZipCgnDbFr')[0]
        urlEn = pRegistry.readRegistryKey('SourceUrls','GeobaseCgnEn')[0] + pRegistry.readRegistryKey('SourceUrls','ZipCgnDbEn')[0]
    
        #Read the shapefile efficiently during process and handles path from the windows directory 
        frShpPath = Path(GeoProc.getLocalFrShp()).parent / Path(urlFr).name
        enShpPath = Path(GeoProc.getLocalEnShp()).parent / Path(urlEn).name
        
        #Extract both the French and English data 

        success = all([
        extract_zip(str(frShpPath),"French CGNDB",sAppMessagesFile),
        extract_zip(str(enShpPath),"English CGNDB",sAppMessagesFile)
        ])
        logging.info(f"Extraction success:{success}") #Debug the extraction error output 
        if success:
            #logging.info("CGNDB Data extracted successfully.")
            Tools.displayMessage(f"CGNDB Data extracted successfully.",False,1,True)
        else:
            #logging.warning("Some CGNDB files failed to extract")
            Tools.displayMessage(f"Some CGNDB files failed to  extraced successfully.",False,1,True)
            return success
        return True #explicityly return true when is is all success 
    except Exception as e:
        logging.error(f"Error extracting CGNDB data: {e} \n{traceback2.format_exc()}")
        Tools.displayMessage(f"Error extracting CGNDB data: {e}",False,1,True) #added as similar to the extracting data if with the full stack trace and does not return False silently
        return False
    


def terminate() -> bool: # removed self from error
        
    global sAppMessagesFile
    """
    Terminates the updateCgndb process, logs completion and sends a notification email
    Updated: March 06 2025(Python 3 and arcGIS Pro migration with 0Auth2 email support 
    
    Returns
    ------
    bool:
        True if termination and email notification succeed, False otherwise
    
    """
    
    bRetVal = True
    Tools.displayMessage(f"The Update on CGNDB has terminated normally.",False,0,False)#added True
    Tools.displayMessage(f"End Date: {Tools.getDateAndTime()}",True,0,False) # adds timestamp,Tools
    
    try:
        #Send email notification and avoids the attribute error : 'NOneType' object has no attribute 'getsubsystemStatus'
        # if StatusManager and hasattr(StatusManager,"getSubSystemStatus") and callable(StatusManager.getSubSystemStatus))
        #     subsystem = StatusManager.getSubSystemStatus()
        # else:
        #     subsystem = "unknown"   
        #[Tools.pLogFile.get_XmlMessagesFile()]
        #subsystem = StatusManager.getSubSystemStatus() if (StatusManager and hasattr(StatusManager,"getSubSystemStatus") and callable(StatusManager.getSubSystemStatus)) else "unknown" 
       #Validate the correct XML messages file
        #global sAppMessagesFile #SET HERE xml FILE Ppath
        if not sAppMessagesFile or not os.path.isfile(sAppMessagesFile):    #or not os.path.isfile(sAppMessagesFile)
            sAppMessagesFile = r"F:\DUSS_SCRIPTS\UpdateCgnToponymes\messages_file\UpdateCgnToponymes_Messages.xml"
            Tools.pLogFile.writeToLog(f"Warning: Using fallback XML messages file: {sAppMessagesFile}",True,0,False)

            if not os.path.isfile(sAppMessagesFile):
                os.makedirs(os.path.dirname(sAppMessagesFile),exist_ok=True)
                with open(sAppMessagesFile,'w',encoding='utf-8') as f:
                    f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<messages>\n'
                            '<data name="LogTitle:"><value>UpdateCgnToponymes Updating ~ </value></data>\n'
                            '<data name="ArcGIS_Available"><value>ArcGIS license available.</value></data>\n'
                            '<data name="ArcGIS_NoLicense"><value>No ArcGIS license available.</value></data>\n'
                            '<data name="PathNotExist"><value>The path does not exist: ~</value></data>\n'
                            '<data name="FileNotFound"><value>File not found: ~</value></data>\n'
                            '<data name="FileNotRead"><value>Failed to read file: ~</value></data>\n'
                            '<data name="DatabaseNotConnected"><value>Cannot connect to database: ~</value></data>\n'
                            '<data name="CountCheckStatus"><value>Count Check Status: ~</value></data>\n'
                            '<data name="DatabaseNotLoaded"><value>Failed to load database: ~</value></data>\n'
                            '<data name="DataUnchanged"><value>Data unchanged since last update, not Transfer required.</value></data>\n'
                            '<data name="UnzipError"><value>Failed to unzip file: ~</value></data>\n'
                            '<data name="FileNotWritten"><value>Failed to write to file: ~</value></data>\n'
                            '<data name="LogErrorOpen"><value>Error while opening log file: ~</value></data>\n'
                            '<data name="JobStatusNoWrite"><value>Can not write JobsStatus.xml!</value></data>\n'
                            '<data name="RegistryNotFound"><value>Cannot find registry key ~</value></data>\n'
                            '<data name="ER"><value>Update CGN Toponymes has not terminated normally, Please check the log file and/or Windows event log</value></data>\n'
                            '<data name="OK"><value>Update CGN Toponymes has terminated normally ~</value></data>\n'
                            '<data name="EmailBodyTitleSuccess"><value>Successfully completed the \'Update CGN Toponymes\' process.</value></data>\n'
                            '<data name="EmailBodyTitleError"><value>The Update CGN Toponymes process has not terminated normally. Please check log file and/or Windows event log.</value></data>\n'
                            '</messages>')
                Tools.pEventLog.writeEventLog('W', f"Create missing messages file:{sAppMessagesFile}",event_type=1,eventID=1,category=5,descr=[f"Created missing messages file:{sAppMessagesFile}"],data=None,sid1=None)
        #Initialize Email2 with the correct XML messages file and log file as attachment(added for checking xml log files )
        #validate log files
        log_file = Tools.pLogFile.get_LogFileName()
        if log_file and os.path.exists(log_file):
            clean_log_file(log_file)
      
         
         #Collect all logs: main + DbUtility logs (dev/prod)
        all_logs = [log_file] #logs_to_attach from task loop 
        
        subsystem = StatusManager.getSubSystemStatus() if (StatusManager and hasattr(StatusManager,"getSubSystemStatus") and callable(StatusManager.getSubSystemStatus)) else "unknown"
        Mail = Email2(sAppName,subsystem,'OK',sAppMessagesFile,all_logs) #[Tools.pLogFile.get_LogFileName()]
        #Mail = Email2(sAppName,subsystem,StatusManager.getSubSystemStatus(),Tools.pLogFile.get_XmlMessagesFile(),[Tools.pLogFile.get_LogFileName()]) ,['eric.rooen@sac-isc.gc.ca']
        
        #Log the attachments for debugging (remove after testing)
        Tools.pLogFile.writeToLog(f"Attaching logs to email:{all_logs}",True,0,False) #Added 
        # Check if the email setup succeeds using the async
        async def send_mail():
            if not await Mail.initialize():
                Tools.pLogFile.writeToLog(f"Error: Mail.initialize() failed. Check email configuration(e.g.,Email/Bcc regsitry key)",True,0,False)
                Tools.displayMessage(f"Warning: Email sending skipped due to invalid configuration",False,0,False)
                #return False #Exit immediately on failure 
                return False #Continue despite email failure change back to False from True
            #Send email with OAuth2 
            if not await Mail.sendEmail():
                Tools.pLogFile.writeToLog(f"Error: Mail.sendEmail() failed",True,0,False)
                return False 
            #else:
            #Tools.displayMessage(f"Email sent successfully",False,0,False)#added true
            return True
            #Terminates after error is determines
        bRetVal = asyncio.run(send_mail()) 
        Tools.pLogFile.closeLogFile()
    except Exception as e:
        bRetVal = False
        Tools.pLogFile.writeToLog(f"Error in terminate(): {e}\n{traceback2.format_exc()}",True,0,False)
    return bRetVal
        
    
    
    """
    The terminate method writes to the log file that the program has successfully terminated and send 
    a notification email with the log file as an attachment.
    November 17, 2017
            
    Returns
            -------
                bRetVal : boolean
                    The operation succeed or not. 
    """
        
    """
        bRetVal = True
        Tools.displayMessage('The UpdateCgndb has terminated normally.',False,1)
        Tools.displayMessage('End at: ',True,1)
        #
        #Send email notification   
        Mail = Email2(sAppName,StatusManager.getSubSystemStatus(),Tools.pLogFile.get_XmlMessagesFile(),[Tools.pLogFile.get_LogFileName()])

        if Mail.initialize() == False:
            Tools.pLogFile.writeToLog("Error in Mail.initialize",True,1,True)
            bRetVal = False
            return bRetVal
        if not Mail.sendEmail():
            Tools.pLogFile.writeToLog("Error in Mail.sendEmail",True,1,True)
            bRetVal = False
        Tools.displayMessage('Email sent.',False,1)
        return bRetVal
        """

def errorHandling(sErrorMessage:str =''):
    global sAppMessagesFile 
    """
        
            The handles by logging messages, updating job status, and sending a notification email 
        THe Updated: March 2025 ( Python 3 MIgration -Proved Logging and Excepion handling
        
        Parameters
        -------------
        sErrorMessage: str,optional
        The error message to log and include in email (default is"")
        
        addes try /exception to get email errors and improve log files 
        
        Notes
        ------
        -Uses try/except to cpature errors in logging and email sending
        - Logs errors to the application log 
        -Attempts to send an email notification when error occurs 
    """
        
    try:
        sAppMessagesFile =r"F:\DUSS_SCRIPTS\UpdateCgnToponymes\messages_file\UpdateCgnToponymes_Messages.xml"
        #Tools.pLogFile.writeToLog(f"[DEBUG]UpdateCgnTonymes.ErrorHandling - sAppMessagesFile set to:{sAppMessagesFile}",True,0,False)
        #no need for other comparisons which is not python syntax and no need for other comparison 
        if sErrorMessage:
            Tools.displayMessage(sErrorMessage,False,1)
        Tools.displayMessage('Terminates with error.',False,1)
        Tools.displayMessage(f'End Date:{Tools.getDateAndTime()} ',True,1) 
        #Ensure the logs witer is initiailize
        if not Tools.pLogFile.get_LogFileName():
            #Falls back: initialize the basic log file if not already set up
            log_dir = r"F:\DUSS_ADMIN\log\UpdateCgnToponymes"
            os.makedirs(log_dir,exist_ok=True)   #change i add sAppName for this function 
            Tools.pLogFile.createLogFile(str(log_dir),sAppName, sAppName,str(sAppMessagesFile),sAppName)


        #Prepares to Send email notification
        #Defines the mail at top 
        #Mail = None #just  add this StatusManager
        if StatusManager is not None:
            StatusManager.setStatus('ER')
        else:
            Tools.pLogFile.writeToLog("Error: StatusManager is not initializatd",True,1,True) 

        #global sAppMessagesFile #SET HERE xml FILE Ppath
        if not sAppMessagesFile or not os.path.isfile(sAppMessagesFile):    #or not os.path.isfile(sAppMessagesFile)
            sAppMessagesFile = r"F:\DUSS_SCRIPTS\UpdateCgnToponymes\messages_file\UpdateCgnToponymes_Messages.xml"
            Tools.pLogFile.writeToLog(f"Warning: Using fallback XML messages file: {sAppMessagesFile}",True,0,False)

            if not os.path.isfile(sAppMessagesFile):
                os.makedirs(os.path.dirname(sAppMessagesFile),exist_ok=True)
                with open(sAppMessagesFile,'w',encoding='utf-8') as f:
                    f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<messages>\n'
                            '<data name="LogTitle:"><value>UpdateCgnToponymes Updating ~ </value></data>\n'
                            '<data name="ArcGIS_Available"><value>ArcGIS license available.</value></data>\n'
                            '<data name="ArcGIS_NoLicense"><value>No ArcGIS license available.</value></data>\n'
                            '<data name="PathNotExist"><value>The path does not exist: ~</value></data>\n'
                            '<data name="FileNotFound"><value>File not found: ~</value></data>\n'
                            '<data name="FileNotRead"><value>Failed to read file: ~</value></data>\n'
                            '<data name="DatabaseNotConnected"><value>Cannot connect to database: ~</value></data>\n'
                            '<data name="CountCheckStatus"><value>Count Check Status: ~</value></data>\n'
                            '<data name="DatabaseNotLoaded"><value>Failed to load database: ~</value></data>\n'
                            '<data name="DataUnchanged"><value>Data unchanged since last update, not Transfer required.</value></data>\n'
                            '<data name="UnzipError"><value>Failed to unzip file: ~</value></data>\n'
                            '<data name="FileNotWritten"><value>Failed to write to file: ~</value></data>\n'
                            '<data name="LogErrorOpen"><value>Error while opening log file: ~</value></data>\n'
                            '<data name="JobStatusNoWrite"><value>Can not write JobsStatus.xml!</value></data>\n'
                            '<data name="RegistryNotFound"><value>Cannot find registry key ~</value></data>\n'
                            '<data name="ER"><value>Update CGN Toponymes has not terminated normally, Please check the log file and/or Windows event log</value></data>\n'
                            '<data name="OK"><value>Update CGN Toponymes has terminated normally</value></data>\n'
                            '<data name="EmailBodyTitleSuccess"><value>Successfully completed the \'Update CGN Toponymes\' process.</value></data>\n'
                            '<data name="EmailBodyTitleError"><value>The Update CGN Toponymes process has not terminated normally. Please check log file and/or Windows event log.</value></data>\n'
                            '</messages>')
                Tools.pEventLog.writeEventLog('W', f"Create missing messages file:{sAppMessagesFile}",event_type=1,eventID=1,category=5,descr=[f"Created missing messages file:{sAppMessagesFile}"],data=None,sid1=None)

        #Send email notification
        #try:
            #subsystem = StatusManager.getSubSystemStatus() if StatusManager else "unknown"
            #Safe way to get subsystem
            # if StatusManager is None:
            #     Tools.displayMessage("StatusManager is not initialized",False,1)
            #     subsystem = "unknown"
            # else:
            #     subsystem = StatusManager.getSubSystemStatus() if (hasattr(StatusManager,"getSubSystemStatus") and callable(StatusManager.getSubSystemStatus)) else "unknown"
        
        #log_file = Tools.pLogFile.get_LogFileName()
        #if log_file and os.path.exists(log_file):
          #  clean_log_file(log_file)
        log_file = Tools.pLogFile.get_LogFileName() 
        if not log_file or not os.path.isfile(log_file):
            Tools.pLogFile.writeToLog(f"Error: Log file not found: {log_file}",True,0,False)
            return False
        #Collect all logs: main + DbUtility logs(dev/prod)
        all_logs = [log_file] #logs to attached from task 
        #PLacement : similar snippet for error email(with 'ER' status)
        #Suppress SMTP debug(same as success path)
        original_set_debuglevel = smtplib.SMTP.set_debuglevel
        smtplib.SMTP.set_debuglevel = lambda self,level: None #Disable the debug mode 
           
        subsystem = StatusManager.getSubSystemStatus() if (StatusManager and hasattr(StatusManager,"getSubSystemStatus") and callable(StatusManager.getSubSystemStatus)) else "unknown"
        
            
        Mail = Email2(sAppName,subsystem,'ER',sAppMessagesFile,all_logs)
            #Mail = Email2(sAppName,subsystem,Tools.pLogFile.get_XmlMessagesFile(),[Tools.pLogFile.get_LogFileName()])
            #The function order #AppName, #Jobstatus(external access, #XML Message File, #Log File attachemnt
           
            #Changes the initialize email system (no need for boolean expression ==True,example)
        async def send_error_email_with_init():
            #nonlocal Mail #Explicitly reference mail form enclosing scope
            if not await Mail.initialize():
                Tools.pLogFile.writeToLog("Error in Mail.initialize(),check the email configuration",True,0,False ) #change from 1 ,True
                #Tools.displayMessage(f"Warning: email skipped due to invalid configuration",False,0,False)
                return False #change back to true if needed  
            #Send the email & log any errors   
            if not await Mail.sendEmail():
                Tools.pLogFile.writeToLog("Error in Mail.sendEmail() Failed in errorHandling after initialization,",True,0,False)
                return False
            else:
                Tools.displayMessage('Email sent successfully.',False,0,False) #added 0 , and False 
                return True
        #bRetVal = asyncio.run(send_error_email())
        asyncio.run(send_error_email_with_init())
        
        #Restored original
        smtplib.SMTP.set_debuglevel = original_set_debuglevel
        
        Tools.pLogFile.closeLogFile()
        #return bRetVal
    except Exception as e:
        error_trace = traceback2.format_exc()
        Tools.pLogFile.writeToLog(f"Email notification failed: {e}\n{error_trace}",True,1,True)
        return False 
   

def cleanDataFolders() -> bool:
    """
        The Cleaning of Data Folders messages, recreates the local English and French shapefile directories 
        THe Updated: March 10 2025 ( Python 3 MIgration -Proved Bilangual data cleansing 
        
        Returns:
        -------------
        bool: True if the operation succeeds, False otherwise
      
        
        addes try /exception to get email errors and improve log files 
         
    """
    try:
            
        #Tools.displayMessage('Deleting files from data folders',False,1,True)
        #Uses the paths for direct parent directory
        sLocalEnShpPath = Path(GeoProc.getLocalEnShp()).parent
        sLocalFrShpPath = Path(GeoProc.getLocalFrShp()).parent
            
        Tools.displayMessage("Deleting files from data folders...",False,1,True)
            
        #Removes the directories only if htey exist 
        if sLocalEnShpPath.exists():
            shutil.rmtree(sLocalEnShpPath)
        if sLocalFrShpPath.exists():
            shutil.rmtree(sLocalFrShpPath)
                
        Tools.displayMessage(f"The data folders are deleted successfully",False,1,True)
            
            #Recreate directories safely making sure the directories exist
        sLocalEnShpPath.mkdir(parents=True,exist_ok=True)
        sLocalFrShpPath.mkdir(parents=True,exist_ok=True)
            
        Tools.displayMessage("New data folders created successfully",False,1,True)
        return True
        
    except Exception as e:
        Tools.displayMessage(f"Error in cleanDataFolders(): {e}\n{traceback2.format_exc()}",False,1,True)
        return False
        
        
  
  
def copyToProductionAndDevelopment(sDbOperation:str) -> bool:
    """
          The copyStagingToProduction does the copy of the Fire Emergency sde feature classes from staging database to production database
        using the DbUtility program. THe fire Emergency SDE feature classes from staging to production or development database 
        March 10, 2025
        
           Parameters
            ----------
            sDbOperation : string (str) 
                The database operation to be executed. which expected values:
                    -'CopyStagingToDevelopmentDb'
                    -'CopyStagingToProductionDb'
            Returns
            -------
            bool :  bReVal 
                The backup is done or not. The True operation copy succeeds , otherwise returns false 
                
            Notes:
            ---------    
                i) Uses dictionary mapping instead of multiple 'if' condition
                ii)Uses `subprocess.run()` for the better error handling than using Popen() whichs handles errors automatically and it will avoid using proces.wait() which delays the process time 
                iii) Improves error logging and exception handling
    """
        
    bRetVal = True
    iReturnCode = None
    sToDoOperation = None 
    result= None
    # xml_messages_file = r"C:\DUSS_SCRIPTS\UpdateCgnToponymes\messages_file\UpdateCgnToponymes_Messages.xml"

    # logging.debug(f"Starting operation:{sDbOperation}")

    # if not os.path.isfile(xml_messages_file):
    #     error_msg = f"Error: XML messages file not found:{xml_messages_file}"
    #     Tools.displayMessage(error_msg,False,1,True)
    #     Tools.pLogFile.writeToLog(error_msg,True,1,True)
    #     return False
    
    # #Defines valid operation using a dictionary 
    #pRegistry = ClsRegistry("DbUtility")
    operations: Dict[str,tuple] = {
        "CopyStagingToProductionDb":("Database","CopyStagingToProductionDb","Starting copy from Staging to Production..."),
        "CopyStagingToDevelopmentDb":("Database","CopyStagingToDevelopmentDb","Starting copy from Staging to Development...")
    }
        
    # #Validate the operation
    if sDbOperation not in operations:
        bRetVal = False
        Tools.displayMessage(f"Error: Unknown operation: '{sDbOperation}'",False,0,False) # added True output without logs 
        Tools.pLogFile.writeToLog(f"Invalid operation:{sDbOperation}",True,0,False) #added new to log if invlaid operation 
        #logging.info(f"Subprocess command:{sToDoOperation}")
        return bRetVal # Exit early if the operation is invalid 
            
     # # #Extract registry key & Message 
    registry_section,registry_key,start_message = operations[sDbOperation]

   
        
    #Read the operation command form the registry
    #validate operation
    # if sDbOperation == 'CopyStagingToProductionDb':
    #     try:
    #         #sToDoOperation = f'python C:\\DUSS_SCRIPTS\\DbUtility\\DbUtility.py copy C:\\DUSS_ADMIN\\xmls\\DUSS_parameters\\Database_xml\\UpdateCgnToponymes\\Database_DbUtility_UpdateCgnToponymes.xml C:\\DUSS_ADMIN\\sdeFiles\\DUSS[REF_NRCAN]_stg1.sde C:\\DUSS_ADMIN\\sdeFiles\\DUSS[REF_NRCAN]_prd1.sde notNotify ref_nrcan ref_nrcan'
    #         sToDoOperation = pRegistry.readRegistryKey('Database','CopyStagingToProductionDb')[0]
    #         Tools.displayMessage('start the copy of the CGNDB data from staging to production database...',False,1,True) # added True
    #         logging.info(f"Subprocess command:{sToDoOperation}")
    #     except Exception as e:
    #         bRetVal = False
    #         Tools.displayMessage(f"Error: Failed to read registry key 'Database\\CopyStagingToProductionDb':{traceback2.format_exc()}",False,1,True) #Added True
    #         Tools.pLogFile.writeToLog(f"Registry read error:{e}\n{traceback2.format_exc()}",True,1,True)
    #         return bRetVal
    # elif sDbOperation == 'CopyStagingToDevelopmentDb':
    #     try:
    #         #sToDoOperation = f'python C:\\DUSS_SCRIPTS\\DbUtility\\DbUtility.py copy C:\\DUSS_ADMIN\\xmls\\DUSS_parameters\\Database_xml\\UpdateCgnToponymes\\Database_DbUtility_UpdateCgnToponymes.xml C:\\DUSS_ADMIN\\sdeFiles\\DUSS[REF_NRCAN]_stg1.sde C:\\DUSS_ADMIN\\sdeFiles\\DUSS[REF_NRCAN]_dev1.sde notNotify ref_nrcan ref_nrcan'
    #         sToDoOperation = pRegistry.readRegistryKey('Database','CopyStagingToDevelopmentDb')[0]
    #         Tools.displayMessage('start the copy of the CGNDB data from staging to development database...',False,1,True)#Added true
    #         logging.info(f"Subprocess command:{sToDoOperation}")
    #     except Exception as e:
    #         bRetVal = False
    #         Tools.displayMessage(f"Error: Failed to read regsitry key 'Database\\CopyStagingToDevelopmentDb':{traceback2.format_exc()}",False,1,True)#Added True
    #         Tools.pLogFile.writeToLog(f"Registry read error:{e}\n{traceback2.format_exc()}",True,1,True)
    #         return bRetVal
    # else:
    #     bRetVal = False
    #     error_msg = f"Error The operation: '{sDbOperation}' is unknown"
    #     Tools.displayMessage(error_msg,False,1,True)#added true
    #     Tools.pLogFile.writeToLog(error_msg,True,1,True)
    #     return bRetVal
    try:
       
        sToDoOperation = pRegistry.readRegistryKey(registry_section,registry_key)[0]
        #logging.debug(f"Subprocess command{sToDoOperation}") #added
        #check ofr email display: 
        #sToDoOperation = sToDoOperation.replace('notNotify','notify')
        sToDoOperation = f"{sToDoOperation} F:\\DUSS_ADMIN\\log\\UpdateCgnToponymes"
        Tools.displayMessage(start_message,False,0,False)# added True again for output 
        #print(sToDoOperation)
    #except Exception as e:
       # bRetVal = False                                             #Original : {sDbOperation}
        #Tools.displayMessage(f"Error:Unable to read registry key for '{registry_section}/{registry_key}':{e}",False,1,True) #Added True
        #Tools.pLogFile.writeToLog(f"Registry read error: {e}\n{traceback2.format_exc()}",True,1,True) #added True and Tools.pLogFile
        #return bRetVal
    
            
    
        #Execute the operation with the subprocess.run() instead of Popen 
    #try:
        #Set the environment for subprocess
        # env = os.environ.copy()
        # #Ensure Oracle client is in Path
        # oracle_client_path = r"C:\oracle\instantclient_19_12" #adjust to your Oracle client
        # env["PATH"] - f"{oracle_client_path};{env['PATH']}"
        # #Optional:set TNS_ADMIN if using tnsames.ora 
        # env["TNS_ADMIN"] = r"C:\oracle\network\admin" # adjust if needed 
        # #Split command into the list to avoid shell= True
        # command = [
        #     "python",
        #     r"C:\DUSS_SCRIPTS\DbUtility\DbUtiity.py",
        #     "copy",
        #     r"C:\DUSS_ADMIN\xmls\DUSS_parameters\Database_xml\UpdateCgnToponymes\Database_DbUtility_UpdateCgnToponymes.xml",
        #     r"C:\DUSS_ADMIN\sdeFiles\DUSS[REF_NRCAN]_stg1.sde",
        #     r"C:\DUSS\ADMIN\sdeFiles\DUSS[REF_NRCAN]_dev1.sde",
        #     r"C:\DUSS\ADMIN\sdeFiles\DUSS[REF_NRCAN]_prd1.sde",
        #     "notNotify",
        #     "ref_nrcan",
        #     "ref_nrcan"
        # ]
        #sToDoOperation,shell=False  command,env=env,
        
        #The split(separates by whitespace,handles better quoted arguments and avoid accident break commands inside path when running commands in DBUtility.py) the registry command string into the list of arguments for subprocess.run()
        #This avoids the use of shell=True and ensures the safer execution,and ensure DbUtility executes properly especially if the command contains spaces
        command = sToDoOperation.split()
        result = subprocess.run(command,shell=False ,check=True,capture_output=True,text=True)
        success_msg = f"Success: Operation '{sDbOperation}' completed with exit code {result.returncode}"
        #Completes the operation of the subprocess successfully for variables and registry keys  
        Tools.displayMessage(success_msg,False,1,True)  
        #Tools.pLogFile.writeToLog(f"{success_msg}\stdout:{result.stdout}\nstderr:{result.stderr}",True,1,True) #Just added new operaion output log    
        Tools.pLogFile.writeToLog(success_msg,True,1,True)
        return bRetVal
      
        #Catches the specific subprocess errors by using .run() and CalledProcessError(), change from result to subprocess
    except subprocess.CalledProcessError as e:
          bRetVal = False  #Added  bReval boolean
          #Tools.displayMessage(f"Error: Operation '{sDbOperation}'\nException failed.\n{e.stderr}",False,1) #added different f-string function below
          #error_msg = (f"Error: Operation '{sDbOperation}' failed with exit code {e.returncode}\n" f"Command:{e.cmd}\n" f"{e.stdout}\n" f"Stderr: {e.stderr}") #added True 
          error_msg = f"Error: Operation '{sDbOperation}' failed with exit code {e.returncode}\nCommand:{e.cmd}" 
          Tools.displayMessage(error_msg,False,0,False)
          #Tools.pLogFile.writeToLog(f"subprocess error in '{sDbOperation}':{e}\n{error_msg}\nTraceback:{traceback2.format_exc()}",True,0,False) #added log print
          Tools.pLogFile.writeToLog(f"Subprocess error in '{sDbOperation}':{error_msg}",True,0,False)
        #return False
          return bRetVal  #--------
            #bReVal = False
    
    except Exception as e: 
        bRetVal = False
        Tools.displayMessage(f"Unexpected error executing '{sDbOperation}': {e}\n{traceback2.format_exc()}",False,0,False) #added True
        #Tools.displayMessage(f"Error: An exception occurred in copyToProductionAndDevelopment:{traceback2.format_exc()}",False,1)
        Tools.pLogFile.writeToLog(f"Unexpected error in '{sDbOperation}':{e}\nTraceback:{traceback2.format_exc()}",True,0,False)#added log print
        return bRetVal
            
    #Get the application name without extension
sAppName =  os.path.basename(sys.argv[0])[:-3]  # removes the '.py'

#Validate sAppName
# print(f"sys.argv[0]: {sys.argv[0]}")
# print(f"sAppName: {sAppName} is ok?")

#if not sAppName:
#     print(f"Warning: Unable to determine application name from sys.argv[0]: {sys.argv[0]}")
#    sAppName = "UpdateCgnToponymes" #Fallback to default name
#     print(f"use of the fall back to get the application name:{sAppName}")
#     #sys.exit(1)


#Set the name of the application, the is to make the application name available in
#all the application classes. Set GlobeName ,'ArcInfo', sAppName
GlobalVariables.gApplicationName = sAppName 
pRegistry = ClsRegistry("UpdateCgnToponymes") #Added the variable to be read in the Windows registry,if removed this program will crash!!!!!!!!!
#Create an object instance of the ClsRegistry class.
#pRegistry = ClsRegistry(GlobalVariables.gApplicationName)      
if not GlobalVariables.gApplicationName:
    raise ValueError("GlobalVariables.gApplicationName is not set Geoprocessing Initiaization") 
        
#Process initialization
#app = UpdateCgnToponymes(sApp="UpdateCgnToponymes",sLicenceType="ArcInfo")
#for Concurrent uses (typical default to Advanced) but for Named User licensing change from "ArcInfo" to "ArcView" for User Basic Level
if not initialize("ArcInfo",sAppName):   #Changed from "ArcInfo" to "ArcView" for Named User Basic level
    #if not app.initialize():
    if StatusManager is not None:
        StatusManager.setStatus('ER')
    else:
        Tools.displayMessage("StatusManger is not initialized",True)
    errorHandling()
    #os._exit(1) #more idomatic than os.exit(1(
    sys.exit(1)
        
pRegistry = ClsRegistry(GlobalVariables.gApplicationName)

#Check data update date (just added)
bypass_data_update = False
try:
    with open(r'F:\DUSS_SCRIPTS\UpdateCgnToponymes\config.json','r') as f:
        config = json.load(f)
    bypass_data_update = config.get('BypassDataUpdateCheck',False)
except Exception as e:
    Tools.pLogFile.writeToLog(f"Warning: Failed to read BypassDataUpdateCheck from config.json: {e}. Defaulting to False",True,0,False)
    
    #Check data update date
update = checkLastDataUpdateDate() #pass since we not using the app object,app=None
if update == None and not bypass_data_update: #The method of bypass time ,will run as schedule for future tasks which not required to wait for the next date for update
    if StatusManager is not None:
        StatusManager.setStatus('ER')
    errorHandling()
    sys.exit(1)
elif update == False and not bypass_data_update:
    #update  = True #for testing purpose
    if StatusManager is not None:
        StatusManager.setStatus('OK')
    #update = True # add this line
    Tools.displayMessage(f"The CGNDB Data sources have not been updated.",False,1)
    Tools.displayMessage(f'End Date: ',True,1)
    sys.exit(0)
        
Tools.displayMessage(f"Updating the CGNDB Data",False,1,True)
    #
    #Initialize Geoprocessing 
#GeoProc = GeoProcessing.GeoProcessing()
GeoProc = GeoProcessing()
if not GeoProc.init():
    resetSourcedate()
    #if 'StatusManager' in globals() and StatusManager is not None:
    StatusManager.setStatus('ER')
    errorHandling()
    sys.exit(1)
      
    #Define a dictionary of tasks , Dict[str,Callable[[],bool]]
tasks = {
    "Download CGNDB Data": downloadCgndbData,
    "Extract Data": extractingData,
    "Update Staging Database": GeoProc.updateCgndb,
    "Copy to Development Database":lambda: copyToProductionAndDevelopment("CopyStagingToDevelopmentDb"), #captialization issue causing hte error 
    "Copy to Production Database":lambda: copyToProductionAndDevelopment("CopyStagingToProductionDb"),
   
    "Clean Data Folders": cleanDataFolders,
    #"Terminates Process":terminate,
    }
    
    


    #Iterate through each tasks dynamically(MOdfied to collect the DbUtility logs )
dev_log = None
prod_log = None
logs_to_attach = [] #Collect all logs for final email

for task_name,task_function in tasks.items():
    Tools.displayMessage(f"Starting: {task_name} at Time: {Tools.getTime()}",True,1,True)#reduce False ,removed {task_function}
    try:
        success = task_function()
        #Added the dev DbUtility log path
        #if task_name == "Copy to Development Database":
        if task_name in ["Copy to Development Database", "Copy to Production Database"]:
            log_file = Tools.pLogFile.get_LogFileName()
            #dev_log = Tools.pLogFile.get_LogFileName() #Get the dev DbUtility log path 
            #clean_log_file(dev_log) #cleans the extra material
            if log_file and os.path.exists(log_file):
                clean_log_file(log_file) #clean the DbUtility log
                #logs_to_attach.append(log_file) #collects the final email
            else:
                Tools.pLogFile.writeToLog(f"Warning: Log file for {task_name} not found",True,1,True)
        #elif task_name == "Copy to Production Database":
            #prod_log = Tools.pLogFile.get_LogFileName() # Get the prod DbUtility log path 
            #clean_log_file(prod_log) #CLeans the extra materials
            
        if not success and task_name != "Terminates Process": #added just incase if the email fails then it will go , change later 
            #if not task_function():
            resetSourcedate()
            if 'StatusManager' in globals() and StatusManager is not None:
                StatusManager.setStatus("ER")
            errorHandling(f"Error: {task_name} failed")
            sys.exit(1)
        Tools.displayMessage(f"{task_name} completed successfully at Time: {Tools.getTime()}",False,1,True) #True add last ,but set False
            #Important to set bTime= False to prevent extra timestamp append
    except Exception as e:
        resetSourcedate()
        if 'StatusManager' in globals() and StatusManager is not None:
            StatusManager.setStatus("ER")
        errorHandling(f"Exception in {task_name}: {e}\n{traceback2.format_exc()}")
        sys.exit(1) # Exit with the failure the exception occurs and scripts ends 



#Main UpdateCgn log (your script's primary log)
update_cgn_log = Tools.pLogFile.get_LogFileName()

#Suppress SMTP debug
original_set_debuglevel = smtplib.SMTP.set_debuglevel
smtplib.SMTP.set_debuglevel = lambda self, level: None #disable debug

#send single email with attachments
subsystem = StatusManager.getSubSystemStatus() if StatusManager else "unknown"
Mail = Email2(sAppName,subsystem,'OK',sAppMessagesFile,[update_cgn_log]) # attached two only

#Call terminate() to handle email sending and cleanup
if not terminate():
    Tools.pLogFile.writeToLog("Error: Termination failed, check logs for details",True,0,False)
    
#Restore original (cleanup)
smtplib.SMTP.set_debuglevel = original_set_debuglevel

#Manually termiantes (call yout orignal terminate logic without email , or exit)
#Assumes the termiante() logs completion and closes logs; call it but skip its email part if possible 
#if terminate() sends email, comment out its Mail.sendEmail() line in Commons.py temporarily, or duplicate its non-email code here
Tools.displayMessage(f"The Update on CGN has terminated normally.",False,0,False)
Tools.displayMessage(f'End Date: {Tools.getDateAndTime()}',True,0,False)
Tools.pLogFile.closeLogFile()

    #exit successfully
sys.exit(0)
            




    
