"""
May, 2022

Created by:
    Jorge Garcia

#***************************************
API settings needed:
KEY    = 'some key'
SECRET = 'some secret'
#***************************************


Flickr_Manager (FM)  reads data from the
flickrapi and saves it as a csv.

Since smaller bounding boxes (BBs) yield
more observations, FM allows the user to
indicate the number of segments to divide
the BB along each axis (num_seg). Therefore
total number of subareas is num_seg^2.

Queries to flickrapi are limited to 4000
observations by the API, and afterthere
these are repeated.

The Flickr_Grid_Manager class ensures 
queries sent are <= 4000, by creating 
several Flickr_Data_Retriever objects 
with a proper timestep that ensures 
this constraint.


"""
import multiprocessing as mp
import pandas as pd
import flickrapi
import time
import os
from math import ceil
from datetime import datetime,date,timedelta





class Flickr_Manager:  # ------------------------------------------------------
    def __init__(self,run_name,bbox,num_seg,year_init,year_end,**kwargs):
        self.run_name = run_name
        self.bboxes   = divide_bbox(bbox,num_seg)
        self.dates    = getDates(year_init,year_end)
        self.kwargs   = kwargs
    
    def run(self):
        print('--*-*-*-*-\t Run started \t-*-*-*-*--')
        pool     = mp.Pool(processes=os.cpu_count())      
        data_all = pool.map(self.callMultiProcessFGM,enumerate(self.bboxes))
        pool.close()
        pool.join()
        data_all = pd.concat(data_all,axis=0)
        data_all.sort_values(by=['datetaken'],inplace=True,ignore_index=True)
        data_all.drop_duplicates(inplace=True,ignore_index=True)
        print('\n\n--*-*-*-*-\t Run finished \t-*-*-*-*--')
        print(f'Total number of obs retrieved:\t{data_all.shape[0]}')
        self.data = data_all
    
    def callMultiProcessFGM(self,args):
        FGM = Flickr_Grid_Manager(
                grid_num = args[0]+1,
                bbox     = args[1],
                dates    = self.dates,
                **self.kwargs
                )
        return FGM.collectData()
    
    def saveCSV(self,folderToSaveFile,zipped=False):
        '''Saves csv in folder. If zipped = True saves a zip file.'''
        folderpath = folderToSaveFile + os.path.sep
        fileName = f'{self.run_name}.csv'
        if zipped:
            self.data.to_csv(
                folderpath + f'{self.run_name}.zip',
                compression = {
                    'method':       'zip',
                    'archive_name': fileName
                    }
                )
        else:
            self.data.to_csv(folderpath + fileName)
# -----------------------------------------------------------------------------



class Flickr_Grid_Manager: # --------------------------------------------------
    '''Class that manages Flickr_Data_Retriever'''
    def __init__(self,grid_num,bbox,dates,**kwargs):
        self.grid_num  = grid_num
        self.bbox      = bbox
        self.dates_abs = dates
        self.requests  = 0
        self.kwargs    = kwargs
        # First batch
        self.FR_batch  = Flickr_Data_Retriever(
            bbox  = self.bbox,
            dates = self.dates_abs,
            **self.kwargs
            )
        self.total_obs   = self.FR_batch.total_obs
        self.batch_obs   = self.FR_batch.total_obs
        self.dates_batch = self.FR_batch.dates
        self.delta_days  = self.dates_batch[1] - self.dates_batch[0]
    
    def collectData(self):
        # 1. Creates data list
        data_grid = []
        
        while True:
            
            # 2. Increase timestep if low
            if self.batch_obs < 3000 and self.batch_obs > 0:
                self.increaseTimeStepAndSetNewBatchDates()
                self.updateBatchAndTotals()
            
            # 3. Reduce timestep if needed
            while self.batch_obs > 4000:
                self.reduceTimeStepAndSetNewBatchDates()
                self.updateBatchAndTotals()
            
            # 4. Gets data
            data_grid.append(self.FR_batch.searchPhotos())
            self.requests += 1
            print(f'Current date:\t{self.dates_batch[1]}')
            print(f'Num requests:\t{self.requests}')
            
            # 5. Updates dates
            self.dates_batch = self.dates_batch[1]-timedelta(days=1),\
                               self.dates_batch[1]+self.delta_days
            
            # 6. Update Batch if date has not exceeded timeframe
            if self.dates_batch[0] > self.dates_abs[1] or self.batch_obs == 0:
                break
            self.updateBatchAndTotals()
            
        # 6. Post-process
        data_grid = pd.concat(data_grid,axis=0)
        if data_grid.shape[0] > 0:
            data_grid.sort_values(by=['datetaken'],inplace=True,ignore_index=True)
            drop_count = data_grid.shape[0]
            data_grid.drop_duplicates(inplace=True,ignore_index=True)
            drop_count = drop_count - data_grid.shape[0]
            data_grid['dateupload'] = data_grid['dateupload'].apply(
                lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d %I:%M:%S")
                )
            data_grid['year']  = data_grid['datetaken'].apply(lambda x: x[0:4])
            data_grid['month'] = data_grid['datetaken'].apply(lambda x: x[5:7])
            data_grid['day']   = data_grid['datetaken'].apply(lambda x: x[8:10])
        print(f'\nGrid: {self.grid_num},',end='\t')
        print(f'Initial obs:\t{self.total_obs}',end='')
        print(f'\nGrid: {self.grid_num},',end='\t')
        print(f'Observations retrieved :\t{data_grid.shape[0]}')
        return data_grid

    def updateBatchAndTotals(self):
        self.FR_batch = Flickr_Data_Retriever(
            bbox  = self.bbox,
            dates = self.dates_batch,
            **self.kwargs
            )
        self.batch_obs = self.FR_batch.total_obs
        
    def reduceTimeStepAndSetNewBatchDates(self):
        print(f'Batch observations > 4000:\t{self.batch_obs}')
        print(f'Reducing timestep from {self.delta_days.days}',end=' ')
        self.delta_days = self.delta_days * 0.85
        print(f'to {self.delta_days.days} days.')
        self.dates_batch = self.dates_batch[0],self.dates_batch[0]+self.delta_days
    
    def increaseTimeStepAndSetNewBatchDates(self):
        print(f'Increasing timestep from {self.delta_days.days}',end = ' ')
        self.delta_days = self.delta_days * 1.25
        print(f'to {self.delta_days.days} days.')
        self.dates_batch = self.dates_batch[0],self.dates_batch[0]+self.delta_days
# -----------------------------------------------------------------------------



class Flickr_Data_Retriever: # ------------------------------------------------
    """Class to retrieve data from Flickr API."""
    def __init__(self,bbox,dates,**kwargs):
        self.bbox   = convertListToString(bbox)
        self.dates  = dates
        self.kwargs = kwargs
        self.flickr = flickrapi.FlickrAPI(
            api_key = 'some_api_key',
            secret  = 'some_secret',
            format  = 'parsed-json'
            )
        self.setTotalObs()
    
    def setTotalObs(self):
        '''Retrieves the total number of observations.'''
        self.total_obs = self.flickr.photos.search(
            page      = 1,
            per_page  = 1,
            bbox      = self.bbox,
            min_taken_date = self.dates[0],
            max_taken_date = self.dates[1],
            **self.kwargs
            )['photos']['total']
        print(f'\n---- Batch observations:\t{self.total_obs}')
    
    def getData(self,page):
        '''Retrieves data by page.'''
        print(f'Page {page} out of {self.pages}')
        return self.flickr.photos.search(
            page     = page,
            per_page = 250,
            bbox     = self.bbox,
            min_taken_date = self.dates[0],
            max_taken_date = self.dates[1],
            **self.kwargs
            )['photos']['photo']
    
    def searchPhotos(self):
        '''Core function to retrieve data and return it as data frame.'''
        start_time = time.time()
        self.pages = ceil(self.total_obs / 250)
        data       = map(self.getData,range(1,self.pages+1))
        data       = [entry for row in data for entry in row]
        end_time   = round(time.time() - start_time,3)
        print(f'\nProcess finished\nTime:\t{end_time} seconds')
        return pd.DataFrame(data)
# -----------------------------------------------------------------------------



# Other functions
def getDates(year_init,year_end):
    return date(year_init,1,1),date(year_end+1,1,1)

def divide_bbox(bbox_list,n):
    x0,y0 = bbox_list[0],bbox_list[1]
    x1,y1 = bbox_list[2],bbox_list[3]
    dx = (x1 - x0) / n
    dy = (y1 - y0) / n
    return [[
        x0 + i*dx,
        y0 + j*dy,
        x0 + i*dx + dx,
        y0 + j*dy + dy
        ] for j in range(n) for i in range(n)]

def convertListToString(dataInList):
    '''Converts list to string'''
    return str(dataInList).replace('[','').replace(']','')



if __name__ == '__main__':
    
    # Bounding box
    example_bbox =[
        -79.2201,43.7838,
        -78.7961,44.0474
        ]
    
    # Example options
    kwargs = {
        'has_geo': 1,
        'sort'   : 'date-taken-asc',
        'extras' : 'url_c,date_upload,date_taken,owner_name,geo,tags,views'
        }
    
    # Path to folder
    path = '/some/folder/path'
    
    # Get data
    FM = Flickr_Manager(
        run_name  = 'example_run',
        bbox      = example_bbox,
        num_seg   = 2,
        year_init = 2008,
        year_end  = 2010,
        **kwargs
        )
    
    # Run
    FM.run()
    
    # Save
    FM.saveCSV(path,True)
    
