PostProcessing of PLANETSCOPE DATA

01 --> cube Planet and Sentinel data into level2_raw

02 --> rename products and subset 8band to 4band BOA for Planet 

03 --> do some checks regarding size, readability and number of SR and UDM products

04 --> delete incomplete scenes

05 --> remove the winter months files 

06 --> rename bands of BOA and udm and TSS products

07 --> cube NDVI S2

08 --> Calculate NDVI for 01 and 02 version and create TSS for all versions 

09 --> cube lafis, gdd, frost and masks

10 --> delete duplicated files from ndvi calculation (not needed if NDVI calculated on level2_daily)

datacube_generation.R - generates a new data cube defintion file with certain properties that you can speficy

dailyPlanetMask_ --> generate daily planet images for different versions




