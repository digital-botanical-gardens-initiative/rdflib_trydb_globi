import pandas as pd
import numpy as np
import gzip
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('wd_lineage_aligned_file', type=str, help="Enter the wd lineage file")
wd_lineage_file = args.wd_lineage_aligned_file
wd_lineage_df = pd.read_csv(wd_lineage_file, usecols=['WdID','WdName','kingdom'], sep=",", dtype=str)
wd_lineage_df["WdID"] = wd_lineage_df["WdID"].str.replace("http://www.wikidata.org/entity/", "", regex=False)
wd_lineage_df["kingdom"] = wd_lineage_df["kingdom"].replace({np.nan: None, pd.NA: None, "": None})
wd_name_to_id_set = set(wd_lineage_df["WdName"])
wd_name_to_id = (
    wd_lineage_df.set_index(["WdName", "kingdom"])["WdID"]
    .to_dict()
)


def process_row(row):
    if row['AccSpeciesName'] in wd_name_to_id_set:
        tempVar = wd_name_to_id.get((row['AccSpeciesName'],'Plantae'))
        if tempVar:
            best_wd_id = tempVar
            kingdomV = "Plantae"
        else: 
            best_wd_id = wd_name_to_id.get((row['AccSpeciesName'],np.nan))
            kingdomV = "None"
        status = "ID-MATCHED-BY-NAME-direct"
    else:
        best_wd_id = None
        kingdomV = None
        status = "NAME-NOT-MATCHED"
    row["WdID"] = best_wd_id
    row["Match_Status"] = status
    row["kingdom"] = kingdomV
    print(row)
    return row



#main execution
if __name__ == "__main__":
    #create the argument parser
    parser = argparse.ArgumentParser()

    #add arguments
    parser.add_argument('inputFile', type=str, help="Enter the tryDb gzip file")
    parser.add_argument('outputFile', type=str, help="Enter the output file name")

    #parse the arguments
    args = parser.parse_args()
    trydbFile = args.inputFile
    outputFile = args.outputFile

    trydb_df = pd.read_csv(trydbFile, usecols=['SpeciesName','AccSpeciesName'], sep="\t", dtype=str, encoding="iso-8859-1") #read source
    d = { 'AccSpeciesName' : list(set(trydb_df['AccSpeciesName']))}
    trydb_df = pd.DataFrame(d)
    trydb_dfX = trydb_df.apply(process_row, axis=1).apply(pd.Series) #same df
    trydb_dfX.to_csv(outputFile, sep="\t", index=False)




