import pandas as pd
import argparse
import configparser
import os


configFile = "config.txt"
if os.path.exists(configFile):       #if config file is available
    config = configparser.ConfigParser()
    config.read(configFile)
    bpFileName = config.get('accessory files', 'bp_fileName')
    lsFileName = config.get('accessory files', 'ls_fileName')

# Declare dictionaries to be used globally. It is best to combine bodyParts and lifeStages in one file in one file.
keyCol="InputTerm"
valCol1="BestMatch"
valCol2="URI"
# list of file paths - bodyPart (anatomicalEntity) and lifeStage (developmentalStage)
file_names = [bpFileName,lsFileName]
# read and combine all files into one DataFrame
mapDf = pd.concat([pd.read_csv(file, sep=",", quoting=0, dtype=str) for file in file_names], ignore_index=True)
eNames = mapDf.dropna(subset=[valCol1]).query(f"{valCol1}.str.strip() != ''", engine='python')
eNamesDict, eNamesSet = eNames.set_index(keyCol)[valCol1].to_dict(), set(eNames[keyCol])
eURI = mapDf.dropna(subset=[valCol2]).query(f"{valCol2}.str.strip() != ''", engine='python')
eURIDict, eURISet = eURI.set_index(keyCol)[valCol2].to_dict(), set(eURI[keyCol])

fungiTerms = {
    "dematiaceous anamorph": "dematiaceous anamorph",
    "coelomycetous anamorph": "coelomycetous anamorph",
    "anamorph": "anamorph",
    "synnemata": "synnemata",
    "synnema": "synnemata"
}



