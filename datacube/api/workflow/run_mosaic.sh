#!/bin/bash
cd /g/data/u46/bb/test
ls  *.nc|grep GREEN > input_gr.txt
ls  *.nc|grep BLUE > input_bl.txt
ls  *.nc|grep RED > input_rd.txt
rm if*
#split into max of 500 files out of more than 1100 datasets
split -l500 input_gr.txt ifg
split -l500 input_rd.txt ifr
split -l500 input_bl.txt ifb
#step 1 create virtual fules 
for i in $(ls if*);do gdalbuildvrt -input_file_list $i $i.vrt;done
#step 2  Now run translate for all 9 files
for i in $(ls if*.vrt); do qsub -v fl=$i run_gdal_translate.pbs;done;
# combine again to nine files to single mosaic once earlier jobs finished ie step 1 and 2 finished
ls ifr*.tif > input_file_red.txt
ls ifg*.tif > input_file_green.txt
ls ifb*.tif > input_file_blue.txt
gdalbuildvrt -input_file_list input_file_red.txt input_file_red.vrt
gdalbuildvrt -input_file_list input_file_green.txt input_file_green.vrt
gdalbuildvrt -input_file_list input_file_blue.txt input_file_blue.vrt
for i in $(ls input_file_*.vrt); do qsub -v fl=$i run_gdal_translate.pbs ; done;
