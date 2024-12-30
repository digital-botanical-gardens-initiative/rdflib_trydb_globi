import pandas as pd
import re
from autocorrect import Speller

spell = Speller(lang='en')  # Autocorrect for English; customize as needed


# Preprocessing function
def preprocess_term(term):
    """Lowercase, autocorrect, and remove extra characters (plural)."""
    term = term.lower().strip()  # Convert to lowercase and remove extra spaces
    #term = spell(term)  # Autocorrect misspelled words
    if term.endswith('s'):
        term = term[:-1]  # Remove trailing 's' to handle plurals
    return term


def map_terms_to_values(training_df, test_df):
    mapping_dict = dict(zip(training_df['input'].str.lower(), training_df['output']))  # Lowercase all terms in training data
    conjunction_patterns1 = re.compile(r'\b(and|y)\b', re.IGNORECASE)
    conjunction_patterns2 = re.compile(r'\b(or)\b', re.IGNORECASE)
    pre_post_fix = re.compile(r"(adult[as]?|tortere|juvenile)", re.IGNORECASE)
    delimiters_regex = re.compile(r'[,;/|&\-]+', re.IGNORECASE)
    delimiters_regex1 = re.compile(r'[()\?\#\-:`]+', re.IGNORECASE)
    delimiters_regex2 = re.compile(r"[+.,]+", re.IGNORECASE)
    pattern = r"(\d+)\s*(\w+)|(\w+)\s*(\d+)"
    #counts_template = {key: 0 for key in mapping_dict.keys()}
    counts_template = {value: 0 for value in mapping_dict.values()}



    for term in test_df['input']:
        mapping_count = counts_template.copy()
        #print(mapping_count)
        termB = term
        print("ACTUAL TERM ",termB)
        term=conjunction_patterns1.sub(',', term)  # Replace "and/or/y" with a comma
        term=conjunction_patterns2.sub('', term)  # Replace "and/or/y" with a comma
        term=delimiters_regex.sub(',', term)  # Replace "and/or/y" with a comma
        term=delimiters_regex1.sub(' ', term)  # Replace "and/or/y" with a comma

        # Replace delimiters (+, ., ,) with spaces for consistent splitting
        if term not in mapping_dict:
            #cleaned_row = re.sub(r"[+.,]", " ", term)
            terms = delimiters_regex2.split(term)
            for term in terms:
                print("\t\tCLEANED TERM", term)
                cleaned_row = re.sub(r"[+.,]", " ", term)
                matches = re.findall(pattern, cleaned_row)
                print("\t\tFULL MATCH", matches)
                if matches:
	                for match in matches:
	                    # Match groups: (number, term) or (term, number)
	                    print("\t\tFULL MATCH", match)
	                    number1, term1, term2, number2 = match
	                    term = term1 if term1 else term2  # Choose the non-empty term
	                    count = number1 if number1 else number2  # Choose the non-empty number
	                    term = preprocess_term(term)  # Normalize to lowercase
	     #               count = int(count)  # Convert count to integer
	                    #if count != "":
	                    #    count = 1
	                    # Find the corresponding key in reference_dict
	                    if term in mapping_dict:
	                        #mapping_count[mapping_dict[term]]=int(count)
	                        mapping_count[mapping_dict[term]]= mapping_count[mapping_dict[term]] + int(count)
	                        print("\t\tActual mapping")
	                        print("\t\t",mapping_dict[term], mapping_count[mapping_dict[term]], sep="\t" )
	                        #mapping_count[term]=int(count)
	                    else:
	                        print("\t\tUNMAPPED TERM ",term)
	                        term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
	                        if term in mapping_dict:
	                            print("\t\tAlternate mapping 3")
	                            print("\t\t",mapping_dict[term], "1", sep="\t" )
                else:
	                 terms = delimiters_regex2.split(term)
	                 for term in terms:
	                    term = preprocess_term(term.strip())  # Normalize to lowercase
	                    if term in mapping_dict:
	                        count = mapping_count[mapping_dict[term]]
	                        mapping_count[mapping_dict[term]] = count + 1
	                        print("\t\tAlternate mapping 1")
	                        print("\t\t",mapping_dict[term], mapping_count[mapping_dict[term]], sep="\t" )
	                        #mapping_count[term] = int(mapping_count[term]) + 1
	
	                    else:
	                        print("\t\tUNMAPPED TERM ",term)
	                        term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
	                        if term in mapping_dict:
	                            count = mapping_count[mapping_dict[term]]
	                            mapping_count[mapping_dict[term]] = count + 1
	                            print("\t\tAlternate mapping 4")
	                            print("\t\t",mapping_dict[term], mapping_count[mapping_dict[term]], sep="\t" )
	                            #mapping_count[term] = int(mapping_count[term]) + 1

        else:
            print("\t\tAlternate mapping 2")
            mapping_count[mapping_dict[term]] = 1
            print("\t\t",mapping_dict[term], mapping_count[mapping_dict[term]], sep="\t")


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
print(result)
#result.to_csv("results.csv", sep="\t", index=False)

