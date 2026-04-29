PostProcessing of PLANETSCOPE DATA

00 --> Order and Download PlanetScope data

01 --> cube Planet data into level2_sites_raw

02 --> do some checks regarding size, readability and number of SR and UDM products, move winter months

03 --> Geometric alignment

04 --> improve udm mask. delete falsely classfied clear pixel & small patches

05 --> build daily images with blue band threshold

06 --> Calculate Color Difference for shadow mask

07 --> build shadow and cloud mask 

08 --> create daily images with new mask

09 --> Calculate NDVI for Planet daily and create planet ndvi time series

10 --> cube sentinel

11 --> do several checks 

12 --> unstack Sentinel 2 time series

13 --> cube the S2 NDVI to resample to 3m 

14 --> Create S2 NDVI TSS and S2 + PlanetScope NDVI TSS

15 --> Cube frost, gdd and lafis 



