from src import Config
from src.etl.extract import SrcChecker, DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader

print(f'Data Directory: {Config.DATA_DIR}')
print(f'Data Warehouse Directory: {Config.DATABASE_DIR}')

import os                            
import logging                      # manage loginfo
from src import Config
# Get emoji :# https://emojipedia.org

# Setup logging
logging.basicConfig(level=getattr(logging, Config.LOG_LEVEL),
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger(__name__)

class ETLPipeline:
    """ETL Pipeline class to manage the ETL process"""

    def __init__(self):
        self.config = Config()
        self.check_src = SrcChecker()
        self.extractor = DataExtractor() # self.extractor คือ instance ของ class DataExtractor
        self.transformer = DataTransformer()
        self.loader = DataLoader()

    def run_check_src(self,src: list[str]=['csv']) -> bool:
        """
        Check if the source CSV files exist
        """
        logger.info("Checking source files...")
        for src_type in src:
            if 'csv' in src_type:
                success = self.check_src.check_src_csv()
         
        return success
    
    def run_extract(self):
        """
        Run the extraction step and return raw data
        """
        logger.info("Running extraction step...")
        raw_data = self.extractor.extract_data()
        if raw_data:
            logger.info("✅ Complete all reading the file.")
        else:
            logger.error("❌ Extraction failed.")
        return raw_data    
    
    def run_transform(self, raw_data: dict) -> dict:
        """
        Run the transformation step on the raw data
        """
        logger.info("\n"+"="*50)
        logger.info("Starting data transformation...")
        logger.info("="*50)
      
        # Transform the raw data using the DataTransformer
        transformed_data = self.transformer.transform_all_data(raw_data)
        if transformed_data is not None:
            logger.info("✅ Transformation completed successfully.")    
        else:
            logger.error("❌ Transformation failed.")
        return transformed_data
    
    def run_load(self, transformed_data: dict) -> bool:
        """
        Run the loading step to load transformed data into the data warehouse
        """
        logger.info("\n"+"="*50)
        logger.info("Starting data loading...")
        logger.info("="*50)

        #Load the DataLoader class
        success = self.loader.load_all_data(transformed_data)
        
        if success:
            logger.info("✅ Data loading completed successfilly.")
        else:
            logger.error("❌ Data loading failed.")
        
        #Disconnect from the database
        self.loader.disconnect()  # Ensure the database connection is closed

        return success

def main():
    logger.info('🚀 ❤️ Starting Data Warehouse ETL Pipeline')
    # Run ETL pipeline
    pipeline = ETLPipeline()  # Create an instance of the ETLPipeline class
    success = pipeline.run_check_src()
    if success:
        raw_data = pipeline.run_extract()
        if raw_data:
            transformed_data = pipeline.run_transform(raw_data)
            if transformed_data:

                success= pipeline.run_load(transformed_data)
                if success:    
                    logger.info("✅ ETL pipeline completed successfully.")  
                    logger.info("You can now start the dashboard with: streamlit run.")
                else:
                    logger.error("❌ ETL pipeline failed during loading.")

    else:   
        logger.error("❌ Missing source files. Please check the logs for details.")
        return

if __name__ == "__main__":
    main()