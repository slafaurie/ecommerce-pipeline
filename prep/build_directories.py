import os
import logging
import shutil


logging.basicConfig(
    level= logging.INFO,
    format= "ETL Prep  - %(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def create_root(ROOT):
    if not os.path.exists(ROOT):
        logger.info(f"Creating root at {ROOT}")
        os.makedirs(ROOT)
    else:
        logger.info("Root found. Checking folders existence. Create new ones if are not present")


def create_folders(ROOT, folders):
    for f in folders:
        new_folder = f"{ROOT}\\{f}"
        if not os.path.exists(new_folder):
            logger.info(f"Creating folder {f}...")
            os.makedirs(new_folder)
        else:
            logger.info(f"folder {f} already exists")


def create_data_folder(ROOT, folders):
    create_root(ROOT)
    create_folders(ROOT, folders)


def delete_data_folder(ROOT):
    if not os.path.exists(ROOT):
        logging.info(f"{ROOT} is not found. Nothing to delete")
        return 
    logger.info("Deleting ROOT and contents...")
    shutil.rmtree(ROOT)

def reset_data_folder(ROOT, folders):
    logger.info("reseting data...")
    delete_data_folder(ROOT)
    create_data_folder(ROOT, folders)

def set_dir_to_parent():
    currentdir = os.path.dirname(os.path.realpath(__file__))
    parentdir = os.path.dirname(currentdir)
    os.chdir(parentdir)
       

if __name__ == "__main__":
    ROOT = "data"
    folders = ["kaggle", "raw", "transient", "staging", "curated"]

    set_dir_to_parent()
    create_data_folder(ROOT, folders)







    