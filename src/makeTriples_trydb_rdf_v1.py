'''
    File name: makeTriples_trydb_rdf_v1.py
    Author: Disha Tandon
    Date created: 12/11/2024
    Python Version: 3
'''


import pandas as pd
from rdflib import URIRef, Literal, Namespace, RDF, RDFS, XSD, DCTERMS, Graph, BNode
import gzip
import rdflib
import argparse
import sys
import configparser
import os
import re

sys.path.append('./functions')  # Add the 'src' directory to the sys.path
import data_processing as dp
from config import traitNames


rdflib.plugin.register('turtle_custom', rdflib.plugin.Serializer, 'turtle_custom.serializer', 'TurtleSerializerCustom')

# Namespace declarations
emi = Namespace("https://purl.org/emi#")
emiUnit = Namespace("https://purl.org/emi/unit#")
emiBox = Namespace("https://purl.org/emi/abox#")
sosa = Namespace("http://www.w3.org/ns/sosa/")
dcterms = Namespace("http://purl.org/dc/terms/")
wd = Namespace("http://www.wikidata.org/entity/")
nTemp = Namespace("http://example.com/base-ns#")
qudt = Namespace("https://qudt.org/2.1/schema/qudt/")
qudtUnit = Namespace("http://qudt.org/vocab/unit/")




def generate_rdf_in_batches(input_csv_gz, wdMapping_csv, join_csv, output_file, join_column1, join_column2, batch_size=1000, ch=1):
    """
    Generate RDF triples in compact Turtle format using batches of rows and rdflib for serialization.

    :param input_csv_gz: Path to the gzipped CSV input file.
    :param join_csv: Path to the secondary CSV file for joining.
    :param output_file: Path to the output Turtle file.
    :param join_column: Column name for joining the two CSVs.
    :param batch_size: The number of rows to process per batch.
    """
    # Load input data
    data1 = pd.read_csv(input_csv_gz, compression="gzip", sep="\t", dtype=str, encoding="iso-8859-1")
    data2 = pd.read_csv(wdMapping_csv, compression="gzip", sep="\t", dtype=str)
    
    # read units dict file
    dictFileNameQudt = "../ontology/data/trydb/qudtMappingToTryDb.txt"
    dictFileNameEmi = "../ontology/data/trydb/EmiMappingToTryDb.txt"
    eNamesDict1 = dp.create_dict_from_csv(dictFileNameQudt, "origUnit", "mapUnit")
    eNamesDict2 = dp.create_dict_from_csv(dictFileNameEmi, "origUnit", "mapUnit")

    # Perform the join
    merged_data = pd.merge(data1, data2[[join_column1, "WdID"]],
                           left_on="AccSpeciesName", right_on=join_column1, how="left")
    merged_data.drop(columns=[join_column1], inplace=True)
    if(ch == 1):
        data3 = pd.read_csv(join_csv, compression="gzip", sep="\t", dtype=str)
        merged_data = merged_data[merged_data['WdID'].isin(data3[join_column2])]

        #merged_data = dp.filter_file_runtime(input_csv_gz, data3, key_column=join_column2)
    #print(merged_data.shape)

    
    # Open the output file for writing
    with gzip.open(output_file, "wt", encoding="utf-8") as out_file:
        # Write prefixes directly to the file
        out_file.write("@prefix emi: <https://purl.org/emi#> .\n")
        out_file.write("@prefix emiUnit: <https://purl.org/emi/unit#> .\n")
        out_file.write("@prefix : <https://purl.org/emi/abox#> .\n")
        out_file.write("@prefix sosa: <http://www.w3.org/ns/sosa/> .\n")
        out_file.write("@prefix dcterms: <http://purl.org/dc/terms/> .\n")
        out_file.write("@prefix wd: <http://www.wikidata.org/entity/> .\n")
        out_file.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
        out_file.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n")
        out_file.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n")
        out_file.write("@prefix qudt: <https://qudt.org/2.1/schema/qudt#> .\n")
        out_file.write("@prefix qudtUnit: <http://qudt.org/vocab/unit/> .\n\n")


    # Process in batches
    for start_row in range(0, len(merged_data), batch_size):
        end_row = min(start_row + batch_size, len(merged_data))
        batch_data = merged_data[start_row:end_row]
        #print(batch_data.shape)
        #print(start_row)
        # Initialize a new graph for this batch
        graph = Graph()
        graph.bind("", emiBox)  # ":" will now map to "https://purl.org/emi/abox#"
        graph.bind("emi", emi)  # Bind the 'emi' prefix explicitly
        graph.bind("emiUnit", emiUnit)  # Bind the 'emiUnit' prefix explicitly
        graph.bind("sosa", sosa)  # Bind the 'sosa' prefix explicitly
        graph.bind("dcterms", dcterms)  # Bind the 'dcterms' prefix explicitly
        graph.bind("wd", wd)  # Bind the 'wd' prefix explicitly
        graph.bind("qudt", qudt)  # Bind the 'qudt' prefix explicitly
        graph.bind("qudtUnit", qudtUnit)  # Bind the 'qudtUnit' prefix explicitly
        #graph.namespace_manager.bind("_", nTemp)

        # Process each row in the batch
        for _, row in batch_data.iterrows():
#            graph.namespace_manager.bind("_", nTemp)
            # Define URIs
            # Define URIs (ensure spaces are replaced with underscores)
            sample_uri = emiBox[f"SAMPLE-{dp.format_uri(row['AccSpeciesName'])}-{row['ObservationID']}"]
            dataset_uri = emiBox[f"DATASET-{dp.format_uri(row['Dataset'])}"] if dp.is_none_na_or_empty(row['Dataset']) else None
            observation_uri = emiBox[f"OBSERVATION-{dp.format_uri(row['ObservationID'])}"]
            organism_uri = emiBox[f"ORGANISM-{dp.format_uri(row['AccSpeciesName'])}"]
            result_bnode = emiBox[f"RESULT-{row['ObsDataID']}"] if dp.is_none_na_or_empty(row['Dataset']) else None
            #result_bnode = BNode("RESULT-" + row['ObsDataID'])


            # Add triples to the graph
            graph.add((sample_uri, RDF.type, sosa.Sample))
            graph.add((sample_uri, RDFS.label, Literal(row['AccSpeciesName'], datatype=XSD.string)))
            graph.add((sample_uri, sosa.isSampleOf, organism_uri))
            graph.add((sample_uri, sosa.isFeatureOfInterestOf, observation_uri))

            if dp.is_none_na_or_empty(dataset_uri):
                graph.add((sample_uri, dcterms.isPartOf, dataset_uri))

            graph.add((dataset_uri, dcterms.bibliographicCitation, Literal(row['Reference'], datatype=XSD.string)))

            graph.add((observation_uri, sosa.hasResult, result_bnode))
            if dp.is_none_na_or_empty(result_bnode):
            #    if dp.is_none_na_or_empty(row['DataType']):
            #        if (row['DataType'] == "Trait"):
                if dp.is_none_na_or_empty(row['TraitName']):
                    graph.add((result_bnode, RDF.type, emi.Trait))
                    if dp.is_none_na_or_empty(row['OrigValueStr']):
                        #pattern = r"[-]?[0-9]+[\.]?[0-9]*"
                        pattern = r"-?[0-9]+(\.[0-9]+)?(E[+-][0-9]+)?"
                        if(re.fullmatch(pattern, row['OrigValueStr'])):
                            graph.add((result_bnode, RDF.value, Literal(row['OrigValueStr'], datatype=XSD.double)))
                        else:
                            graph.add((result_bnode, RDF.value, Literal(row['OrigValueStr'], datatype=XSD.string)))
            #       elif (row['DataType'] == "Non-trait"):
                else:
                    graph.add((result_bnode, RDF.type, emi.NonTrait))
                    if dp.is_none_na_or_empty(row['OrigValueStr']):
                        graph.add((result_bnode, RDF.value, Literal(row['OrigValueStr'], datatype=XSD.string)))
            if dp.is_none_na_or_empty(row['DataName']):
                graph.add((result_bnode, RDFS.label, Literal(row['DataName'], datatype=XSD.string)))
            if dp.is_none_na_or_empty(row['DataID']):
                graph.add((result_bnode, dcterms.identifier, Literal(row['DataID'], datatype=XSD.string)))
            #if dp.is_none_na_or_empty(row['OrigValueStr']):
            #    graph.add((result_bnode, RDF.value, Literal(row['OrigValueStr'], datatype=XSD.string)))

            #Add units
            if dp.is_none_na_or_empty(row['OrigUnitStr']):
                entity = row['OrigUnitStr']
                if entity in eNamesDict1:
                    #print(row['OrigUnitStr']," ",qudtUnit[eNamesDict1[entity]])
                    graph.add((result_bnode, qudt.hasUnit, URIRef(qudtUnit[eNamesDict1[entity]])))
#                    graph.add((result_bnode, qudt.hasUnit, URIRef(emiUnit[dp.format_uri(entity.strip())])))
                elif dp.is_none_na_or_empty(row['UnitName']):
                    entity1 = row['UnitName']
                    if entity1 in eNamesDict1:
                        #print(entity1," ",qudtUnit[eNamesDict1[entity1]])
                        graph.add((result_bnode, qudt.hasUnit, URIRef(qudtUnit[eNamesDict1[entity1]])))
                    elif entity1 in eNamesDict2:
                        #print(entity1," ",eNamesDict2[entity1.strip()])
                        graph.add((result_bnode, qudt.hasUnit, URIRef(eNamesDict2[entity1.strip()])))
                elif entity in eNamesDict2:
                    #print(row['OrigUnitStr']," ",eNamesDict2[entity.strip()])
                    graph.add((result_bnode, qudt.hasUnit, URIRef(eNamesDict2[entity.strip()])))
                    #print(row['OrigUnitStr']," ",eNamesDict2[dp.format_uri(entity.strip())])
                    #graph.add((result_bnode, qudt.hasUnit, URIRef(eNamesDict2[dp.format_uri(entity.strip())])))
                graph.add((result_bnode, RDFS.comment, Literal(entity.strip(), datatype=XSD.string)))



            if pd.notna(row['WdID']):
                graph.add((organism_uri, emi.inTaxon, URIRef(wd[dp.format_uri(row['WdID'])])))

        dp.add_inverse_relationships(graph)
        # Serialize the graph for the batch and write to the file
        with gzip.open(output_file, "at", encoding="utf-8") as out_file:  # Append mode
            out_file.write(graph.serialize(format="turtle_custom"))
        #out_file.flush()

        # Clear the graph to free memory
    #    graph.remove((None, None, None))
        del graph
        #print(out_file)

    print(f"RDF triples saved to {output_file}")

# Main execution
if __name__ == "__main__":
    configFile = "config.txt"
    if os.path.exists(configFile):       #if config file is available
        config = configparser.ConfigParser()
        config.read(configFile)
        csv_file1 = config.get('input tsv files', 'trydb_tsv')
        csv_file2 = config.get('accessory files', 'trydb_wd')
        csv_file3 = config.get('accessory files', 'enpkg_wd')
        output_file = config.get('output files', 'trydb_ttl')
    else:                               #else use command line arguments
        # Create the argument parser
        parser = argparse.ArgumentParser()
    
        # Add arguments
        parser.add_argument('inputFile', type=str, help="Enter the file name for which you want the triples")
        parser.add_argument('wdMappingFile', type=str, help="Enter the file name which will be used for mapping to WdIDs")
        parser.add_argument('joinFile', type=str, help="Enter the file name which will be used for filtering or joining the input_file")
        parser.add_argument('outputFile', type=str, help="Enter the output file name")
    
        # Parse the arguments
        args = parser.parse_args()
        csv_file1 = args.inputFile
        csv_file2 = args.wdMappingFile
        csv_file3 = args.joinFile
        output_file = args.outputFile

    generate_rdf_in_batches(csv_file1, csv_file2, csv_file3, output_file, join_column1="TRY_AccSpeciesName",  join_column2 = "wd_taxon_id", batch_size=10000)

