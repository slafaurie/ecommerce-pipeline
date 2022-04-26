import os
import logging
import shutil
import argparse

from prep.prep_utils.constants import FilePath, Folders
from common.data_model import DataModel


# logger
logging.basicConfig(
    level= logging.INFO,
    format= "ETL Prep  - %(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# functions
def create_root(ROOT):
    """
    Create root folder if not exists already.
    """
    if not os.path.exists(ROOT):
        logger.info(f"Creating root at {ROOT}")
        os.makedirs(ROOT)
    else:
        logger.info("Root found. Checking folders existence. Create new ones if are not present")


def create_zone_folders(ROOT, folders):
    """
    Create all folders within root if not present.
    """
    for f in folders:
        new_folder = f"{ROOT}\\{f}"
        if not os.path.exists(new_folder):
            logger.info(f"Creating folder {f}...")
            os.makedirs(new_folder)
        else:
            logger.info(f"folder {f} already exists")


def create_data_folder(ROOT, folders):
    """
    Create complete tree folder
    """
    create_root(ROOT)
    create_zone_folders(ROOT, folders)


def delete_data_folder(ROOT):
    """
    Remove folder with its content.
    """
    if not os.path.exists(ROOT):
        logging.info(f"{ROOT} is not found. Nothing to delete")
        return 
    logger.info("Deleting ROOT and contents...")
    shutil.rmtree(ROOT)
       

if __name__ == "__main__":

    DataModel.set_mode(True)
   
    ROOT = FilePath.ROOT
    folders = Folders.folders

    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", type=str)
    args = parser.parse_args()

    DataModel.set_dir_to_parent()

    if args.reset:
        delete_data_folder(ROOT)
    create_data_folder(ROOT, folders)







    