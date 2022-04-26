import os
import enum


class DateColumns:
    TIMESTAMP_COLUMN = "order_purchase_timestamp"
    DATE_COLUMN = "purchase_date"
    

class FilePath:
    # TODO Integrate functions into common.data_model.DataModel class. 
    # Use the DataModel class manage load/writing files also in the prep part
    ROOT = "data"
    ZONE = "kaggle"
    SAVE_ZONE = "raw"
    ORDERS = "olist_orders_dataset.csv"
    ITEMS =  "olist_order_items_dataset.csv"
    PAYMENTS =   "olist_order_payments_dataset.csv"
    

    # @classmethod
    # def return_root(cls):
    #     return os.path.join(cls.ROOT, cls.ZONE)

    @classmethod
    def return_datasets_to_chunk(cls):
        return [cls.ITEMS, cls.ORDERS, cls.PAYMENTS]

    # @classmethod
    # def return_dataset_path(cls, dataset):
    #     # if dataset not in cls.return_datasets_to_chunk():
    #     #     raise Exception("Dataset is not present")
    #     return os.path.join(cls.return_root(), dataset)

    # @classmethod
    # def return_orders_path(cls):
    #     return cls.return_dataset_path(cls.ORDERS)

    # @classmethod
    # def return_items_path(cls):
    #     return cls.return_dataset_path(cls.ITEMS)

    # @classmethod
    # def return_payments_path(cls):
    #     return cls.return_dataset_path(cls.PAYMENTS)

    # @classmethod
    # def return_save_root(cls):
    #     return os.path.join(cls.ROOT, cls.SAVE_ZONE)



class Folders:
    folders = ["kaggle", "raw", "transient", "staging", "curated"]