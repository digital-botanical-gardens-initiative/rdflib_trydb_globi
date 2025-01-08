This repo consists of codes for generating triples from trydb, globi and taxonomy files for the DBGI work. It also containes codes that I use often for matching random terms to ontology entities.

Steps to follow:
1. Generate globi tsv file

```
cd modGLoBI
python globiDown.py <Globi tsv-file> categories.txt <output-file>
```


2. Generate ontology mappings [LifestageNames (DevelopmentalStage) and BodyPartNames (AnatomicalEntity)]

```
cd ontology
python matchNamesToOntology.py <input-file> <output-file>
```


3. Generate triples

```
cd src
python makeTriples_trydb_rdf_v1.py <input trydb tsv-file> <wd mapping file to trydb species names> <enpkg wd ids> <output-file>
python makeTriples_globi_rdf_v1.py <input globi tsv-file> <enpkg wd ids> <output-file>
python makeTriples_taxonomy_rdf_v1.py <input taxonomy tsv-file> <enpkg wd ids> <output-file>
```


4. Generate qlever sparql endpoint

a) Generate first set of index and start the server

```
cd qlever
qlever --qleverfile Qlever.try_globi index --overwrite-existing --parallel-parsing false
qlever --qleverfile Qlever.try_globi start
```

b) Generate void file for the triples 

Obtain [void-generator jar file](https://github.com/JervenBolleman/void-generator)

```java -jar void-generator-0.6-SNAPSHOT-uber.jar -r "http://localhost:<port-name>" --void-file void-trydbglobi1.ttl -i http://localhost:<port-name>/.voidX/ --max-concurrency 1 -f```

c) Generate second set of index and start the server for querying

```
qlever --qleverfile Qlever.try_globi stop
qlever --qleverfile Qlever.try_globi.spql_editor index --overwrite-existing --parallel-parsing false```
qlever --qleverfile Qlever.try_globi start
```

5. Obtain the html file from [sparql-editor](https://github.com/JervenBolleman/void-generator)

a) Copy the index.html file obtained to ```qlever/sparql-editor-index.html```. Add the appropriate js-file and the sparql endpoint to the html file.

b) Start the server inside the qlever directory

```python cors_server.py```

c) Open the ```qlever/sparql-editor-index.html``` to query and explore the class overview.


Tree structure and comments for each file

```
.
├── modGLoBI                                        #folder for automating globi tsv file
│   ├── categories.txt                              #taxonomy to be considered for globi
│   └── globiDwn.py                                 #script to generate tsv file
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
│   └── matchNamesToOntology.py                     #given a list of names, match them to ontology entities from UBERON, PO, ENVO, PATO, etc.
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
