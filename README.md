This repo consists of codes for generating triples from trydb, globi and taxonomy files for the DBGI work. It also containes codes that I use often for matching random terms to ontology entities.
```
.
├── ontology
│   ├── data
│   │   ├── globi                                   #folder for ontology matches of body part and life stage names to ontologies like UBERON, PO, ENVO, PATO, etc.
│   │   │   ├── correctedBodyPartNamesGlobi.csv
│   │   │   ├── mappedBodyPartNamesGlobi.csv
│   │   │   ├── unmappedBodyPartNamesGlobi.csv
│   │   │   ├── correctedLifeStageNamesGlobi.csv
│   │   │   ├── mappedLifeStageNamesGlobi.csv
│   │   │   │── unmappedLifeStageNamesGlobi.csv
│   │   │   │── correctedBiologicalSexNames.tsv
│   │   │   ├── mappedBiologicalSexNames.tsv
│   │   │   ├── unmappedBiologicalSexNames.tsv
│   │   ├── README.md
│   │   └── trydb                                   #folder for ontology matches of units from trydb to quft
│   │       ├── EmiMappingToTryDb.txt               #mapping of units (not in qudt)
│   │       └── qudtMappingToTryDb.txt              #mapping of units present in qudt
│   │── matchNamesToOntology.py                     #given a list of names, match them to ontology entities from UBERON, PO, ENVO, PATO, etc.
│   └── matchNamesBiologicalGender.py               #given a list of gender names formatted inconsistently, match them to the vocabulary from PATO or emi
│── src                                             #main codes and functions for generating triples in turtle format
│   ├── functions
│   │   │── data_processing.py                      #functions to parse files, dataframes and strings
│   │   └── matchNamesBiologicalGender.py           #function to assign biological gender while maing the rdf triples
│   ├── makeTriples_globi_rdf_v1.py                 #main code to generate triples for globi interaction data
│   ├── makeTriples_taxonomy_rdf_v1.py              #main code to generate triples for taxonomy
│   ├── makeTriples_trydb_rdf_v1.py                 #main code to generate triples for trydb traits data
│   └── turtle_custom
│       └── serializer.py                          
├── qlever                                          #settings for qlever
│   ├── cors_server.py                              #start a server with CORS enabled in python
│   ├── data                                        #data for building qlever index
│   ├── Qlever.try_globi                            #configuration for qlever without void and example ttls
│   ├── Qlever.try_globi.spql_editor                #configuration for qlever with -void, examples- required for sparql-editor
│   └── sparql_editor_index.html                    #implement sparql editor
├── README.md           
└── requirements.txt                                #pip install -r requirements.txt
```
