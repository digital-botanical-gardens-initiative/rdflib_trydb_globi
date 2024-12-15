'''
    File name: matchNamesToOntology.py
    Author: Disha Tandon
    Date created: 12/13/2024
    Python Version: 3
'''


from owlready2 import get_ontology
from sentence_transformers import SentenceTransformer, util
import csv
import argparse

# Function to load and parse multiple ontologies
def load_ontologies(ontology_paths):
    ontologies = {}
    for ontology_name, path in ontology_paths.items():
        ontologies[ontology_name] = get_ontology(path).load()
    return ontologies

# Load a Sentence Transformer model
model = SentenceTransformer('all-MiniLM-L6-v2')  # Lightweight and effective model

# Extract labels and IRIs from the ontologies
def extract_terms_from_ontology(ontology):
    ontology_terms = []
    for cls in ontology.classes():
        if cls.label:  # Ensure the term has a label
            ontology_terms.append((cls.label[0], cls.iri))
    return ontology_terms

# Generate embeddings for ontology terms
def generate_ontology_embeddings(ontologies, model):
    all_ontology_terms = []
    for ontology_name, ontology in ontologies.items():
        terms = extract_terms_from_ontology(ontology)
        all_ontology_terms.extend(terms)
    
    ontology_labels = [term[0] for term in all_ontology_terms]
    ontology_embeddings = model.encode(ontology_labels, convert_to_tensor=True)
    return all_ontology_terms, ontology_embeddings

# Function to find the best match using Sentence Transformers
def find_best_match(input_term, ontology_terms, ontology_embeddings, model):
    input_embedding = model.encode(input_term, convert_to_tensor=True)

    # Compute cosine similarity
    similarities = util.pytorch_cos_sim(input_embedding, ontology_embeddings)
    best_match_idx = similarities.argmax().item()
    best_match_label = ontology_terms[best_match_idx][0]
    best_match_uri = ontology_terms[best_match_idx][1]
    best_match_score = similarities[0][best_match_idx].item()

    return best_match_label, best_match_uri, best_match_score


# Function to process input terms and find the best match
def process_terms(input_file, output_file, ontology_paths):
    # Load ontologies
    ontologies = load_ontologies(ontology_paths)

    # Generate embeddings for ontology terms
    all_ontology_terms, ontology_embeddings = generate_ontology_embeddings(ontologies, model)
    
    # Read terms from input file
    with open(input_file, "r") as infile:
        input_terms = [line.strip() for line in infile if line.strip()]

    # Open the output CSV file for writing the results
    with open(output_file, "w", newline="") as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(["Input Term", "Best Match", "URI", "Similarity Score"])  # Header row
        
        # Process each term and find the best match
        for terms in input_terms:
            if terms:  # Skip empty lines
                # Split the line by '/' or ',' to handle multiple terms
                tx = [term.strip() for term in terms.replace('/', ',').split(',')]

                for i in tx:
                    best_label, best_uri, best_score = find_best_match(i, all_ontology_terms, ontology_embeddings, model)
                    csv_writer.writerow([i, best_label, best_uri, f"{best_score:.4f}"])
                    print(f"Processed: {i} -> {best_label} ({best_score:.4f})")

# Run the code
if __name__ == "__main__":

    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument('inputFile', type=str, help="Enter the file name , which has the terms to be matched")
    parser.add_argument('outputFile', type=str, help="Enter the output file name")

    #Parse the arguments
    args = parser.parse_args()
    input_file = args.inputFile  # Input file with term
    output_file = args.outputFile  # Output file to save the results
    ontology_paths = {
            "UBERON" :  "http://purl.obolibrary.org/obo/uberon.owl",  # UBERON ontology path
        "PO" :  "http://purl.obolibrary.org/obo/po.owl",  # PO ontology path
        "ENVO" :    "http://purl.obolibrary.org/obo/envo.owl",  # ENVO ontology path
        "PATO" :    "http://purl.obolibrary.org/obo/pato.owl",  # PATO ontology path
        "WBLS" :
        }
    process_terms(input_file, output_file, ontology_paths)

