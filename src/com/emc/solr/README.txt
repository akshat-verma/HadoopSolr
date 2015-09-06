Author - Akshat Verma
This project develops a Solr based search engine for searching within pdf files stored Hadoop File system and retrieving them.
If files are significantly small in size compared to Hadoop block size, they can be converted to Hadoop Archive (.har) file(to take care of small file problem) and hadoop archive file path can be an input to the program while creating index files
How to run?
1.Run Create_Solr_Index.java with path of pdf file as arguement.Index files are created.
2.Run Search_and_Retrieve.java with search key as arguement.The paths of files which contain the search key are outputted.
