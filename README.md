This repo consists of codes for generating triples from trydb, globi and taxonomy files for the DBGI work. It also containes codes that I use often for matching random terms to ontology entities.
```
.
├── ontology
│   ├── data
│   │   ├── globi                                   #folder for ontology matches of body part and life stage names to ontologies like UBERON, PO, ENVO, PATO, etc.
│   │   │   ├── correctedBodyPartNamesGlobi.csv
│   │   │   ├── correctedLifeStageNamesGlobi.csv
│   │   │   ├── mappedBodyPartNamesGlobi.csv
│   │   │   ├── mappedLifeStageNamesGlobi.csv
│   │   │   ├── unmappedBodyPartNamesGlobi.csv
│   │   │   └── unmappedLifeStageNamesGlobi.csv
│   │   ├── README.md
│   │   └── trydb                                   #folder for ontology matches of units from trydb to quft
│   │       ├── qudtMappingToTryDb_full.txt
│   │       └── qudtMappingToTryDb.txt
│   └── matchNamesToOntology.py                     #given a list of names, match them to ontology entities from UBERON, PO, ENVO, PATO, etc.
├── README.md
├── requirements.txt                                #pip install -r requirements.txt
└── src                                             #main codes and functions for generating triples in turtle format
    ├── functions
    │   └── data_processing.py                      #functions to parse files, dataframes and strings
    ├── makeTriples_globi_rdf_v1.py                 #main code to generate triples for globi interaction data
    ├── makeTriples_taxonomy_rdf_v1.py              #main code to generate triples for taxonomy
    ├── makeTriples_trydb_rdf_v1.py                 #main code to generate triples for trydb traits data
    └── turtle_custom
        └── serializer.py                          
.
├── README.md
├── requirements.txt                        #pip install -r requirements.txt
├── src                                     #main codes and functions for generating triples in turtle format.
│   ├── functions
│   │   └── data_processing.py              #functions to parse files, dataframes and strings
│   ├── makeTriples_taxonomy_rdf_v1.py      #main code to generate triples for taxonomy
│   └── makeTriples_trydb_rdf_v1.py         #main code to generate triples for trydb
├── ontology                                
    └── matchNamesToOntology.py             #given a list of names, match them to ontology entities from UBERON, PO, or ENVO

```
