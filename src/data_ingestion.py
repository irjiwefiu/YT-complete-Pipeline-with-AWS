import os
import pandas as pd
from sklearn.model_selection import train_test_split
import logging
#make directory
log_dir= "./logs"
os.makedirs(log_dir,exist_ok=True)

# Logging object
logger= logging.getLogger("data_ingestions")
logger.setLevel("DEBUG")
# console logger
cons_log =logging.StreamHandler()
cons_log.setLevel("DEBUG")
# File Logger
log_file_path=os.path.join(log_dir,"data_ingestion.log")
file_logger=logging.FileHandler(log_file_path)
file_logger.setLevel("DEBUG")
# Log Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
cons_log.setFormatter(formatter)
file_logger.setFormatter(formatter)

# Adding Loggers to the logging object
logger.addHandler(cons_log)
logger.addHandler(file_logger)


def load_data(data_url: str)->pd.DataFrame:
    '''
    Load data from a CSV file
    '''
    try:
        df= pd.read_csv(data_url)
        logger.debug("Data loaded from %s" ,data_url)
        return df
    
    except pd.errors.ParserError as e:
        logger.error("Failed to parse the CSV file :%s",e)
        raise
    
    except Exception as e:
        logger.error("Unexpected Error Occured: %s",e)
        raise

def preprocess_data(df: pd.DataFrame)->pd.DataFrame:
    '''Pre process the data initially'''
    try:
        df.drop(columns=['Unnamed: 2','Unnamed: 3','Unnamed: 4'],inplace=True)
        df.rename(columns={'v1':'target','v2':'text'},inplace=True)
        logger.debug("Data Pre processing completed ")
        return df
    except KeyError as e:
        logger.debug("Missing columns in the dataframe: %s" ,e)
        raise
    except Exception as e:
        logger.error("Unexpected Error Occured: %s",e)
        raise


def save_data(train_data:pd.DataFrame,test_data:pd.DataFrame,data_path:str)->None:
    '''save the train and test dataset'''
    try:
        path= os.path.join(data_path,'raw')
        os.makedirs(path,exist_ok=True)
        train_data.to_csv(os.path.join(path,'train.csv'),index=False)
        test_data.to_csv(os.path.join(path,'test.csv'),index=False)
        logger.debug('Train and Test data been saved')
    
    except Exception as e:
        logger.error('Unexcpeted error occured: %s',e)
        raise

def main():
    try:
        test_size=0.2
        df=load_data("https://raw.githubusercontent.com/vikashishere/Datasets/refs/heads/main/spam.csv")
        df=preprocess_data(df)
        train,test= train_test_split(df,test_size=test_size,random_state=2)
        save_data(train,test,'data')
    except Exception as e:
        logger.error("preprocessing not completed: %s",e)
        raise
if __name__=='__main__':
    main()
