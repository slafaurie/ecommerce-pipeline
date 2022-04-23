import os
import logging
import shutil
import argparse


# logger
logging.basicConfig(
    level= logging.INFO,
    format= "ETL Prep  - %(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# functions
def set_dir_to_parent():
    """
    Set the working directory the root of the project.
    """
    currentdir = os.path.dirname(os.path.realpath(__file__))
    parentdir = os.path.dirname(currentdir)
    os.chdir(parentdir)


def create_root(ROOT):
    """
    Create root folder if not exists already.
    """
    if not os.path.exists(ROOT):
        logger.info(f"Creating root at {ROOT}")
        os.makedirs(ROOT)
    else:
        logger.info("Root found. Checking folders existence. Create new ones if are not present")


def create_folders(ROOT, folders):
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
    create_folders(ROOT, folders)


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

    ROOT = "data"
    folders = ["kaggle", "raw", "transient", "staging", "curated"]

    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", type=bool)
    args = parser.parse_args()

    set_dir_to_parent()
    if args.reset:
        delete_data_folder(ROOT)
    create_data_folder(ROOT, folders)







    