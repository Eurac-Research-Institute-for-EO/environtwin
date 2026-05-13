# Grassland Management Workflow – Short Overview

## 1. Data Acquisition (Planet API)
Downloading single sites:  00_automated_download.R 

Downloading multiple sites in a row:  00_planet_multiPoly_process.R

## 2. Data Prepatation
### 2.1 PlanetScope
Scripts: 
* 01_clip_raster_to_sites 
* 02_check_planet_columns_rows.sh, 02_check_sr_udm.sh, 02_check_bands_valid.sh, 
* 03_move_winterMonth.sh 

*Cropping, filtering, and validation of PlanetScope imagery.*

### 2.2 Sentinel 2
Scripts: 
* 10_s2_cubing.sh,
* 10_s2_cubing_qai.sh
* 11_check_s2_readability.sh 

*Resampling from 10m to 3m.*

## 3. Geometric Correction
Script: 04_arosics_exe.py 

*Co-registration of imagery using AROSICS.*

## 4. Masking (Cloud, Shadow, Snow, Haze) 
### 4.1 Simple udm mask improvement
Script: 05_LinuxOptimized_udm_improvement_script.R 

* Remove invalid pixels
* Apply sieve filter (>1000 pixels) 

### 4.2 Advanced shadow detection and cloud buffering

Scripts: 06_ColorMeasure.R & 07_shadow_cloudBuffer.py 

## 5. Daily Composites 
Scripts: 08_daily_images_V0.py, 08_daily_images_V1.py, 08_daily_images_V3.py 

*Daily mosaics created by averaging observations using different masks.*

## 6. NDVI Processing 
Planet scripts: 09_calculate_ndvi.sh, 09_ndvi_tss.sh 

Sentinel scripts: 
* 12_unstack_sentinel.R
* 13_s2_cubing_ndvi.sh, 13_clip_s2_ndvi_to_extent.R,
* 14_mosaic_s2_sites_ndvi.sh,
* 15_ndvi_tss_sen2.sh, 15_ndvi_tss_pla_sen2.sh
  
## 7. Additional Data Layers 
### 7.1 Temperature and GDD 
Scripts: FORCE/scripts/gdd/ 

* 01_process_temperature.R 
* 02_firstover5.sh, 
* 02_firstFrost.sh,  
* 03_GDD_cum_ST.py,  
* 04_firstDOYGDD.sh 

### 7.2 LAFIS & WBS 
Scripts: FORCE/scripts/lafis_wbs/  

* join_wbs_lafis
* scriptToClipLafis.R
* wbs_pdf_to_points.R 

*Harmonization and integration of shapefiles and parcel data.* 

### 7.3 Site Masks 
Scripts: 16_cube_*.sh 

*Resample GDD, lafis, Frost, site masks to 3m raster*

## 8. Statistical Analysis 
Scripts:  
/Environtwin/FORCE/scripts/analysis/

* Compare LAFIS and WBS datasets
* Evaluate data availability
* NDVI-based vegetation analysis 

