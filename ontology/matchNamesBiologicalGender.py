import pandas as pd
import re
from autocorrect import Speller

spell = Speller(lang='en')  # Autocorrect for English; customize as needed


# Preprocessing functions
def preprocess_term(term):
    """Lowercase, autocorrect, and remove extra characters (plural)."""
    term = term.lower().strip()  # Convert to lowercase and remove extra spaces
    #term = spell(term)  # Autocorrect misspelled words
    if "mono" not in term and "auto" not in term:
        if term.endswith('s'):
            term = term[:-1]  # Remove trailing 's' to handle plurals
    return term

def map_terms_to_values(training_df, test_df):
    mapping_dict = dict(zip(training_df['input'].str.lower(), training_df['output']))  # Lowercase all terms in training data
    conjunction_patterns1 = re.compile(r'\b(and|y)\b', re.IGNORECASE)
    conjunction_patterns2 = re.compile(r'\b(or)\b', re.IGNORECASE)
    pre_post_fix = re.compile(r"(adult[as]?|tortere|juvenil[e]?|maybe|\(?torete[s]?\)?)", re.IGNORECASE)
    delimiters_regex = re.compile(r"[,;/|&]+", re.IGNORECASE)                          # Removed '-'
    delimiters_regex1 = re.compile(r"[\[\]\(\)\?\#:`]+", re.IGNORECASE)                # Removed '-'
    delimiters_regex2 = re.compile(r"[+.,]+", re.IGNORECASE)
    delimiters_regex3 = re.compile(r"\s\s", re.IGNORECASE)
    #pattern = r"(\d+)\s*(\w+)|(\w+)\s*(\d+)"
    pattern = r"(\d+)\s*([\w-]+)|([\w-]+)\s*(\d+)"
    #counts_template = {key: 0 for key in mapping_dict.keys()}
    counts_template = {value: 0 for value in mapping_dict.values()}

    for term in test_df['input']:
        mapping_count = counts_template.copy()
        term = term.lower().strip()  # Convert to lowercase and remove extra spaces
        term=conjunction_patterns1.sub(',', term)  # Replace "and/or/y" with a comma
        term=conjunction_patterns2.sub('', term)  # Replace "and/or/y" with a comma
        term=delimiters_regex.sub(',', term)  # Replace "and/or/y" with a comma
        term=delimiters_regex1.sub(' ', term)  # Replace "and/or/y" with a comma
        term=delimiters_regex3.sub(' ', term)  # Replace "and/or/y" with a comma

        # Replace delimiters (+, ., ,) with spaces for consistent splitting
        if term not in mapping_dict:
            #cleaned_row = re.sub(r"[+.,]", " ", term)
            terms = delimiters_regex2.split(term)
            for term in terms:

                cleaned_row = re.sub(r"[+.,]", " ", term)
                matches = re.findall(pattern, cleaned_row)
                if matches:
                    for match in matches:
                        # Match groups: (number, term) or (term, number)
                        number1, term1, term2, number2 = match
                        term = term1 if term1 else term2  # Choose the non-empty term
                        count = number1 if number1 else number2  # Choose the non-empty number
                        term = preprocess_term(term.strip())  # Normalize to lowercase
                        count = int(count)  # Convert count to integer
                        #Find the corresponding key in reference_dict
                        if term in mapping_dict:
                            mapping_count[mapping_dict[term]]= mapping_count[mapping_dict[term]] + int(count)
                            termX = mapping_dict[term]
                            countX = mapping_count[mapping_dict[term]]
                        else:
                            term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
                            if term in mapping_dict:
                                termX = mapping_dict[term]
                                countX = 1
                            else:                                               # Unmapped 
                                termX = "unknown"
                                countX = 1
                else:
                    terms = delimiters_regex2.split(term)
                    for term in terms:
                        term = preprocess_term(term.strip())  # Normalize to lowercase
                        if term in mapping_dict:
                            count = mapping_count[mapping_dict[term]]
                            mapping_count[mapping_dict[term]] = count + 1
                            termX = mapping_dict[term]
                            countX = mapping_count[mapping_dict[term]]
                        else:
                            term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
                            if term in mapping_dict:
                                count = mapping_count[mapping_dict[term]]
                                mapping_count[mapping_dict[term]] = count + 1
                                termX = mapping_dict[term]
                                countX = mapping_count[mapping_dict[term]]
                            else:                                               # Unmapped
                                termX = "unknown"
                                countX = 1
                print(termX, "\t", countX)
        else:
            mapping_count[mapping_dict[term]] = 1
            termX = mapping_dict[term]
            countX = 1
            print(termX, "\t", countX)
        

#    return counts



# File paths
dataFile = "/home/drishti/Documents/Projects/DBGI/gitReposMine/rdflib_trydb_globi/ontology/data/globi/correctedBiologicalSexNames.tsv"
testFile = "/home/drishti/Documents/Projects/DBGI/gitReposMine/rdflib_trydb_globi/ontology/data/globi/temp/test.tsv"

# Load training and test data
training_data = pd.read_csv(dataFile, sep="\t", quoting=3, dtype=str)
test_data = pd.read_csv(testFile, sep="\t", dtype=str, quoting=3)

# Map the terms in test data to their respective values
result = map_terms_to_values(training_data, test_data)

# Output the result (you can save it to a CSV or print it)
#print(result)
#result.to_csv("results.csv", sep="\t", index=False)

