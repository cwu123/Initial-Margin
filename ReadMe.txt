The entire inital margin run is automated and scheduled through visualcron on server nyfoapplegacy. The job batch name is PCSPAN. All the code is stored in
http://nyfodevtfs:8080/tfs/DefaultCollection/_git/FrontOfficeTools
The projects are 'PCSPAN Push to PFS'
http://nyfodevtfs:8080/tfs/DefaultCollection/_git/FrontOfficeTools?path=%2FJobs%2FPCSPAN%20Push%20to%20PFS&version=GBmaster&_a=contents
SpanXmlReportGenerator
http://nyfodevtfs:8080/tfs/DefaultCollection/_git/FrontOfficeTools?path=%2FJobs%2FSpanXmlReportGenerator&version=GBmaster&_a=contents
The project to replace the margin generator is InitialMarginMultipleRun
http://nyfodevtfs:8080/tfs/DefaultCollection/_git/FrontOfficeTools?path=%2FJobs%2FInitialMarginMultipleRun&version=GBmaster&_a=contents

'PCSPAN Push to PFS' is an executable that downloads parameter files from the exchanges, pulls positions from allegro, cxl and tempest stores them in the DB and generates positions files for the black boxes - ICE SPAN and PCSPAN.
SpanXmlReportGenerator is an executable that locates the latest parameter ont the network and runs the black boxes with the parameter and position files. The result files are then combined into an excel file using VBA.