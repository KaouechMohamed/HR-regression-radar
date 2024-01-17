import pandas as pd
import csv
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime

BCG_file_path=".csv"
HR_file_path=".txt"
output_file="file_"
########################################## prepare BCG FILE
BCG = pd.read_csv(BCG_file_path, delimiter=';')

########################################## prepare HR FILE

data = []
with open(HR_file_path, 'r') as file:
    lines = file.readlines()[1:]  

for line in lines:
    timestamp, value = line.strip().split('\t')
    data.append([timestamp, value.replace(',', '.')]) 

with open('HR.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Timestamp', 'HR [bpm]'])  
    csvwriter.writerows(data)
HR=pd.read_csv('HR.csv')

############################################# DATA TRANSFORMATION
def timestamp_to_utc(timestamp_ms):
    # Convert milliseconds to seconds by dividing by 1000
    timestamp_seconds = int(timestamp_ms / 1000)
    utc_time = datetime.datetime.utcfromtimestamp(timestamp_seconds).time()
    return utc_time

BCG["TimeUtc"]=BCG["TimeSinceEpoch"].apply(lambda x : timestamp_to_utc(x))
HR["TimeUtc"]=HR["Timestamp"].apply(lambda x : timestamp_to_utc(x))
    
BCG_SIGNALS=BCG[["TimeUtc","BCG"]]
HR=HR[["TimeUtc","HR [bpm]"]]
###################################################################### GROUPING DATA PER SECONDE(TIMEUTC)
grouped_BCG = pd.DataFrame(BCG_SIGNALS.groupby('TimeUtc')['BCG'].apply(np.array))
grouped_BCG['BCG_length']=grouped_BCG['BCG'].apply(lambda x : len(x))
grouped_HR = pd.DataFrame(HR.groupby('TimeUtc')['HR [bpm]'].apply(lambda x: x.mean()))

####################################################################### MERGING BCG AND HR
DATA=grouped_BCG.merge(grouped_HR,left_index=True, right_index=True)
####################################################################### SELECTING BCG ARRAYS WITH LENGTH OF 20 (high frequency)
DATA=DATA[DATA["BCG_length"]==20.0]
DATA.reset_index(inplace=True)
######################################################################## saving to csv format (spliting the BCG arrays into 20 columns "Rq:we cant save a numpy array to csv file!!!")
def save_data(data,file_name):
    x=DATA.copy()
    BCG=np.stack(np.array(x['BCG']))
    BCG_amp=["BCG_amp"+str(i) for i in range(1,21)]
    BCG_df=pd.DataFrame(BCG,columns=BCG_amp)
    x=pd.concat([x,BCG_df],axis=1)
    x=x[["TimeUtc"]+BCG_amp+["HR [bpm]"]]
    file_name=file_name+".csv"
    x.to_csv(file_name,index=False)
    
save_data(DATA,output_file)
