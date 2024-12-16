'''
    File name: makeTriples_interactions_rdf_v1.py
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

sys.path.append('./functions')  # Add the 'src' directory to the sys.path
import data_processing as dp

rdflib.plugin.register('turtle_custom', rdflib.plugin.Serializer, 'turtle_custom.serializer', 'TurtleSerializerCustom')

# Namespace declarations
emi = Namespace("https://purl.org/emi#")
emiBox = Namespace("https://purl.org/emi/abox#")
sosa = Namespace("http://www.w3.org/ns/sosa/")
dcterms = Namespace("http://purl.org/dc/terms/")
wd = Namespace("http://www.wikidata.org/entity/")
nTemp = Namespace("http://example.com/base-ns#")

def is_none_na_or_empty(value):
    return value is None or value == '' or pd.isna(value)


def generate_rdf_in_batches(input_csv_gz, join_csv, output_file, join_column, batch_size=1000):
    """
    Generate RDF triples in compact Turtle format using batches of rows and rdflib for serialization.

    :param input_csv_gz: Path to the gzipped CSV input file.
    :param join_csv: Path to the secondary CSV file for joining.
    :param output_file: Path to the output Turtle file.
    :param join_column: Column name for joining the two CSVs.
    :param batch_size: The number of rows to process per batch.
    """
    # Load input data

    data2 = pd.read_csv(join_csv, sep="\t", dtype=str)
    merged_data = dp.filter_file_runtime(input_csv_gz, data2, key_column='wd_taxon_id')

    #data1 = pd.read_csv(input_csv_gz, compression="gzip", sep="\t", dtype=str, encoding="utf-8", quoting=3)
    
    # Filter the interactions based on WdID in enpkg and taxonomy data
    #valid_taxons = data2['wd_taxon_id'].unique()
    #merged_data = data1[(data1['source_WD'].isin(valid_taxons)) | 
    #    (data1['target_WD'].isin(valid_taxons))
    #]
    print(merged_data)
    merged_data.to_csv('intxns_subset_20241212_with_enpkg_wdIds.tsv.gz', sep='\t', compression='gzip', index=False)


    
    # Open the output file for writing
    with gzip.open(output_file, "wt", encoding="utf-8") as out_file:
        # Write prefixes directly to the file
        out_file.write("@prefix emi: <https://purl.org/emi#> .\n")
        out_file.write("@prefix : <https://purl.org/emi/abox#> .\n")
        out_file.write("@prefix sosa: <http://www.w3.org/ns/sosa/> .\n")
        out_file.write("@prefix dcterms: <http://purl.org/dc/terms/> .\n")
        out_file.write("@prefix wd: <http://www.wikidata.org/entity/> .\n")
#       out_file.write("@prefix p_: <http://example.com/base-ns#> .\n")
        out_file.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
        out_file.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n")
        out_file.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n")
#
#
#    # Process in batches
    for start_row in range(0, len(merged_data), batch_size):
        end_row = min(start_row + batch_size, len(merged_data))
        batch_data = merged_data[start_row:end_row]
        print(batch_data.shape)
        print(start_row)
#        # Initialize a new graph for this batch
        graph = Graph()
        graph.bind("", emiBox)  # ":" will now map to "https://purl.org/emi/abox#"
        graph.bind("emi", emi)  # Bind the 'emi' prefix explicitly
        graph.bind("sosa", sosa)  # Bind the 'emi' prefix explicitly
        graph.bind("dcterms", dcterms)  # Bind the 'emi' prefix explicitly
        graph.bind("wd", wd)  # Bind the 'emi' prefix explicitly
#        graph.bind("prov", prov)  # Bind the 'emi' prefix explicitly
#        graph.namespace_manager.bind("_", nTemp)
        i=start_row
        # Process each row in the batch
        for _, row in batch_data.iterrows():
            # Define URIs (ensure spaces are replaced with underscores)
            source_taxon_uri = emi[f"{row['sourceTaxonId']}"] if is_none_na_or_empty(row['sourceTaxonId']) else None
            target_taxon_uri = emi[f"{row['targetTaxonId']}"] if is_none_na_or_empty(row['targetTaxonId']) else None
            intxn_type_uri = emi[f"{row['interactionTypeName']}")] if is_none_na_or_empty(row['interactionTypeName']) else None
            intxn_type_Id_uri = URIRef(f"http://purl.obolibrary.org/obo/{row['interactionTypeId']}") if is_none_na_or_empty(row['interactionTypeId']) else None #maybe add RO as namespace
            intxnRec_uri = emiBox[f"inRec{i}"]
#            sample_uri = emiBox[f"SAMPLE-{format_uri(row['AccSpeciesName'])}-{row['ObservationID']}"]
#            organism_uri = emiBox[f"ORGANISM-{format_uri(row['AccSpeciesName'])}"]


# ADD checks for NA           # Add triples to the graph for interaction Record
            graph.add((intxnRec_uri, emi.hasSource, source_taxon_uri)) if is_none_na_or_empty(source_taxon_uri)
            graph.add((intxnRec_uri, emi.hasTarget, target_taxon_uri)) if is_none_na_or_empty(target_taxon_uri)
            graph.add((intxnRec_uri, emi.hasInteractionType, intxn_type_uri)) if is_none_na_or_empty(intxn_type_uri)
            graph.add((intxnRec_uri, emi.hasInteractionType, intxn_type_Id_uri)) if is_none_na_or_empty(intxn_type_Id_uri)
            graph.add((intxnRec_uri, prov.atLocation, Literal(row['localityName'], datatype=XSD.string))) if is_none_na_or_empty(row['localityName'])
            graph.add((intxnRec_uri, dcterms.bibliographicCitation, Literal(row['referenceDoi'], datatype=XSD.string))) if is_none_na_or_empty(row['referenceDoi'])
            graph.add((intxnRec_uri, dcterms.bibliographicCitation, Literal(row['sourceDoi'], datatype=XSD.string))) if is_none_na_or_empty(row['sourceDoi'])
            graph.add((intxnRec_uri, URIRef("http://www.w3.org/2003/01/geo/wgs84_pos#lat"), Literal(row['decimalLatitude'], datatype=XSD.string))) if is_none_na_or_empty(row['decimalLatitude'])
            graph.add((intxnRec_uri, URIRef("http://www.w3.org/2003/01/geo/wgs84_pos#long"), Literal(row['decimalLongitude'], datatype=XSD.string))) if is_none_na_or_empty(row['decimalLongitude'])


            #Add triples for source and targets
            graph.add((source_taxon_uri, sosa.isSampleOf, emiBox[f"{row['sourceTaxonName']}"])) if is_none_na_or_empty(row['sourceTaxonName'])
            graph.add((source_taxon_uri, emi.inTaxon, wd[f"{row['source_WD']}"])) if is_none_na_or_empty(row['source_WD'])

            
            graph.add((target_taxon_uri, sosa.isSampleOf, emiBox[f"{row['targetTaxonName']}"])) if is_none_na_or_empty(row['targetTaxonName'])
            graph.add((target_taxon_uri, emi.inTaxon, wd[f"{row['target_WD']}"])) if is_none_na_or_empty(row['target_WD'])
        
        i = i + 1
        dp.add_inverse_relationships(graph)

            # Write body part, physiological state, and other taxon attributes (if available)
#            graph.add((source_taxon_uri, emi.hasAnatomicalEntity, URIRef(f"{bpName[row['sourceBodyPartName']]}")))
#            graph.add((source_taxon_uri, emi.hasDevelopmentalStage, URIRef(f"{lsName[row['sourceLifeStageName']]}")))
#            graph.add((source_taxon_uri, emi.hasPhysiologicalStage, Literal(row['sourcePhysiologicalStageName'], datatype=XSD.string)))
#            graph.add((source_taxon_uri, emi.hasSex, Literal(row['sourceSexName'], datatype=XSD.string)))
#            graph.add((URIRef(f"{bpName[row['sourceBodyPartName']]}"), RDFS.comment, Literal(row['sourceBodyPartName'],datatype=XSD.string)))
#            graph.add((URIRef(f"{lsName[row['sourceLifeStageName']]}"), RDFS.comment, Literal(row['sourceLifeStageName'],datatype=XSD.string)))


        # Serialize the graph for the batch and write to the file
        with gzip.open(output_file, "at", encoding="utf-8") as out_file:  # Append mode
            out_file.write(graph.serialize(format="turtle_custom"))
        # Clear the graph to free memory
        del graph
        print(out_file)

    print(f"RDF triples saved to {output_file}")

# Main execution
if __name__ == "__main__":

    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument('inputFile', type=str, help="Enter the file name for which you want the triples")
    parser.add_argument('joinFile', type=str, help="Enter the file name which will be used for filtering or joining the input_file")
    parser.add_argument('outputFile', type=str, help="Enter the output file name")

    # Parse the arguments
    args = parser.parse_args()
    csv_file1 = args.inputFile
    csv_file2 = args.joinFile
    output_file = args.outputFile
    generate_rdf_in_batches(csv_file1, csv_file2, output_file, join_column="TRY_AccSpeciesName", batch_size=10000)
