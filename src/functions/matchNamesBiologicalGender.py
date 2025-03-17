import pandas as pd
import re


# Preprocessing functions - Lowercase, autocorrect, and remove extra characters (plural)
def preprocess_term(term):
    term = term.lower().strip()  # Convert to lowercase and remove extra spaces
    if "mono" not in term and "auto" not in term:
        if term.endswith('s'):
            term = term[:-1]  # Remove trailing 's' to handle plurals
    return term

# Functions for mapping biological gender - Match the biological gender values
def map_terms_to_values(term):
    # File paths
    dataFile = "/home/drishti/Documents/Projects/DBGI/gitReposMine/rdflib_trydb_globi/ontology/data/globi/correctedBiologicalSexNames.tsv"
    # Load data
    mapDf = pd.read_csv(dataFile, sep="\t", quoting=3, dtype=str)

    # declare and fill dictionary
    mapping_dict = dict(zip(mapDf['input'].str.lower(), mapDf['output']))

    # conjunction patterns 
    conjunction_patterns1 = re.compile(r'\b(and|y)\b', re.IGNORECASE)
    conjunction_patterns2 = re.compile(r'\b(or)\b', re.IGNORECASE)

    # pre_post fixes 
    pre_post_fix = re.compile(r"(adult[as]?|tortere|juvenil[e]?|maybe|\(?torete[s]?\)?)", re.IGNORECASE)

    # delimiters to be considered
    delimiters_regex = re.compile(r"[,;/|&]+", re.IGNORECASE)                          # Removed '-'
    delimiters_regex1 = re.compile(r"[\[\]\(\)\?\#:`]+", re.IGNORECASE)                # Removed '-'
    delimiters_regex2 = re.compile(r"[+.,]+", re.IGNORECASE)
    delimiters_regex3 = re.compile(r"\s\s", re.IGNORECASE)

    # patterns for matching strings like "12 male, 3 female"
    pattern = r"(\d+)\s*([\w-]+)|([\w-]+)\s*(\d+)"

    # create a template dictionary from the values of mapping_dict
    counts_template = {value: 0 for value in mapping_dict.values()}

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
                            mapping_count[mapping_dict[term]] = countX
                        else:                                               # Unmapped 
                            termX = "unknown"
                            countX = 1
                            mapping_count[mapping_dict["unknown"]] = mapping_count[mapping_dict["unknown"]] + countX
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
                            mapping_count[mapping_dict["unknown"]] = mapping_count[mapping_dict["unknown"]] + countX
            #print(termX, "\t", countX)
    else:
        mapping_count[mapping_dict[term]] = 1
        termX = mapping_dict[term]
        countX = 1
        mapping_count[mapping_dict[term]] = countX
        #print(termX, "\t", countX)
    # Use dictionary comprehension to filter out pairs with value 0
    filtered_dict = {k: v for k, v in mapping_count.items() if v != 0}
    #print(filtered_dict)
    return filtered_dict
