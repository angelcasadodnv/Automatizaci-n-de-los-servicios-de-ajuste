import os 

def makeDir(dir_name): 
    os.makedirs(dir_name, exist_ok=True)