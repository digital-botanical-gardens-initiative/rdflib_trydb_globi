import pandas as pd
import sys
import re
import os
import importlib
import rdflib
from rdflib import URIRef, Literal, Namespace, RDF, RDFS, XSD, DCTERMS, Graph, BNode



sys.path.append('./functions')  # Add the 'src' directory to the sys.path
import data_processing as dp
import temp_matchNamesBiologicalGender as tmbg


# Second set : Namespaces for body part (anatomical entity) and life stage (developmental) names
aeo = Namespace("http://purl.obolibrary.org/obo/AEO_")
chebi = Namespace("http://purl.obolibrary.org/obo/CHEBI_")
clyh = Namespace("http://purl.obolibrary.org/obo/CLYH_")
envo = Namespace("http://purl.obolibrary.org/obo/ENVO_")
fao = Namespace("http://purl.obolibrary.org/obo/FAO_")
fbdv = Namespace("http://purl.obolibrary.org/obo/FBdv_")
hao = Namespace("http://purl.obolibrary.org/obo/HAO_")
ncit = Namespace("http://purl.obolibrary.org/obo/NCIT_")
omit = Namespace("http://purl.obolibrary.org/obo/OMIT_")
pato = Namespace("http://purl.obolibrary.org/obo/PATO_")
po = Namespace("http://purl.obolibrary.org/obo/PO_")
poro = Namespace("http://purl.obolibrary.org/obo/PORO_")
ro = Namespace("http://purl.obolibrary.org/obo/RO_")
uberon = Namespace("http://purl.obolibrary.org/obo/UBERON_")



prefix_to_namespace = {
    "AEO:" : aeo,
    "CHEBI:" : chebi,
    "CLYH:" : clyh,
    "ENVO:" : envo,
    "FAO:" : fao,
    "FBdv:" : fbdv,
    "HAO:" : hao,
    "NCIT:" : ncit,
    "OMIT:" : omit,
    "PATO:" : pato,
    "PORO:" : poro,
    "RO:" : ro,
    "FAO:" : fao,
    "NCIT:" : ncit,
    "OMIT:" : omit,
    "UBERON:" : uberon,
    "PO:" : po
}


def getListOfNames(dataFile,fileInName):
    # load dictonary data from life stage or body part names. Make a dict
    mapDf = pd.read_csv(dataFile, sep=",", quoting=3, dtype=str)
    mapping_dict = dict(zip(mapDf['InputTerm'].str.lower(), mapDf['URI']))
    # process the raw terms-inclusive of random delimiters etc.
    fileIn=pd.read_csv(fileInName, sep="\t", dtype=str)
    fullRecords = pd.DataFrame()
    for _, row in fileIn.iterrows():
        if pd.notna(row["id"]):
            fullRecords = pd.concat([fullRecords, tmbg.countTerms(row["name"],mapping_dict, mapping_set)], ignore_index=True)
    return fullRecords


# Function for adding ambiguous entities to the graph
def add_entity_to_graphX(entity, entityID, ns, eURIDict, eURISet, eNamesDict, eNamesSet):
    emiBox = Namespace("https://purl.org/emi/abox#")
    if dp.is_none_na_or_empty(entityID): 
        if any(entityID.startswith(prefix) for prefix in prefix_to_namespace): #handle prefixed identifiers like 'UBERON:' #check if the URI-Id is already provided
            for prefix, namespace in prefix_to_namespace.items():
                if entityID.startswith(prefix):
                    entity_Id = entityID[len(prefix):]
                    entityURI = namespace[entity_Id]
                    print(entityID,ns, entityURI,"EXISTING-1",sep="\t")
                    break
        elif entityID.startswith("http"):
            ent=URIRef(entityID)
            print(entityID,ns, ent,"EXISTING-2",sep="\t")
    elif entity in eURISet: #check if the entity name exists in eURISet (in case the URI-Id is not already availabe in globi records)
        modEntityURI = URIRef(eURIDict[entity])  # Use standardized URI
        modEntityName = eNamesDict[entity]  # Use standardized URI
        print(entity,ns,modEntityURI,"URI-FETCHED-1",sep="\t")
    elif entity in eNamesSet: #check if the entity name exists in eURISet (in case the URI-Id is not already availabe in globi records)
        modEntityName = eNamesDict[entity]  # Use standardized URI
        print(entity,ns,modEntityName,"URI-FETCHED-1a",sep="\t")
    else:
        records = tmbg.listTerms(entity,eURIDict,eURISet,eNamesDict,eNamesSet,ns)

def testLifeStageBodyPartAssignments(input_csv_gz, batch_size=100000):
    keyCol="InputTerm"
    valCol1="BestMatch"
    valCol2="URI"
    BPfileName="../ontology/data/globi/correctedBodyPartNamesGlobi.csv"
    mapDf = pd.read_csv(BPfileName, sep=",", quoting=3, dtype=str)
    eNames = mapDf.dropna(subset=[valCol1]).query(f"{valCol1}.str.strip() != ''", engine='python')
    eNamesDictBP, eNamesSetBP = eNames.set_index(keyCol)[valCol1].to_dict(), set(eNames[keyCol])
    eURI = mapDf.dropna(subset=[valCol2]).query(f"{valCol2}.str.strip() != ''", engine='python')
    eURIDictBP, eURISetBP = eURI.set_index(keyCol)[valCol2].to_dict(), set(eURI[keyCol])
    
    LSfileName="../ontology/data/globi/correctedLifeStageNamesGlobi.csv"
    mapDf = pd.read_csv(LSfileName, sep=",", quoting=3, dtype=str)
    eNames = mapDf.dropna(subset=[valCol1]).query(f"{valCol1}.str.strip() != ''", engine='python')
    eNamesDictLS, eNamesSetLS = eNames.set_index(keyCol)[valCol1].to_dict(), set(eNames[keyCol])
    eURI = mapDf.dropna(subset=[valCol2]).query(f"{valCol2}.str.strip() != ''", engine='python')
    eURIDictLS, eURISetLS = eURI.set_index(keyCol)[valCol2].to_dict(), set(eURI[keyCol])

    chunks = pd.read_csv(input_csv_gz, sep="\t", compression="gzip", chunksize=batch_size, dtype=str, encoding="utf-8")
    for batch_data in chunks:
        for _, row in batch_data.iterrows():
	        # for body part names
            if (dp.is_none_na_or_empty(row['sourceBodyPartName']) or dp.is_none_na_or_empty(row['sourceBodyPartId'])):
                add_entity_to_graphX(row['sourceBodyPartName'],row['sourceBodyPartId'],"ANATOMICAL_ENTITY",eURIDictBP, eURISetBP, eNamesDictBP, eNamesSetBP)
            if (dp.is_none_na_or_empty(row['targetBodyPartName']) or dp.is_none_na_or_empty(row['targetBodyPartId'])):
                add_entity_to_graphX(row['targetBodyPartName'],row['targetBodyPartId'],"ANATOMICAL_ENTITY",eURIDictBP, eURISetBP, eNamesDictBP, eNamesSetBP)
	        # for life stage names
            if (dp.is_none_na_or_empty(row['sourceLifeStageName']) or dp.is_none_na_or_empty(row['sourceLifeStageId'])):
                add_entity_to_graphX(row['sourceLifeStageName'],row['sourceLifeStageId'],"DEVELOPMENTAL_STAGE", eURIDictLS, eURISetLS, eNamesDictLS, eNamesSetLS)
            if (dp.is_none_na_or_empty(row['targetLifeStageName']) or dp.is_none_na_or_empty(row['targetLifeStageId'])):
                add_entity_to_graphX(row['targetLifeStageName'],row['targetLifeStageId'],"DEVELOPMENTAL_STAGE", eURIDictLS, eURISetLS, eNamesDictLS, eNamesSetLS)


#################################################################################
# Next few lines to list terms only
#fileInName="../ontology/data/globi/verbatim_unmappedLifeStageNamesGlobi.csv"       #BodyPart names
#fileInName="../ontology/data/globi/verbatim_unmappedBodyPartNamesGlobi.csv" #LifeStage names
#dataFile = "../ontology/data/globi/correctedLifeStageNamesGlobi.csv"        #LifeStage names
#dataFile = "../ontology/data/globi/correctedBodyPartNamesGlobi.csv"
#getListOfNames(dataFile,fileInName)
##################################################################################



##################################################################################
#lifeStageSet = set()
#bodyPartSet = set()
#testLifeStageBodyPartAssignments(input_csv_gz)
##################################################################################
