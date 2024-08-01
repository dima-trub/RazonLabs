import os


path = os.path.abspath(__file__)
FOLDER_PATH = os.path.dirname(path)


class Config:
    INPUT_DIR = os.path.dirname(path) + '/Data/'
    TARGET_DIR = os.path.dirname(path) + '/TargetFile/'
