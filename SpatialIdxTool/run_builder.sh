#!/bin/sh
./nise_spatial_index_builder -i burgerKing_polygon.txt -o Idx_file
./nise_spatial_index_reader -i Idx_file -z -n 5 < burgerKing_location.txt | cat  > output.txt


./nise_spatial_index_builder -i cntrybd.txt -o cntry_idx
./nise_spatial_index_reader -i cntry_idx -z -n 1 < testllus.txt | cat  > testllus_out.txt


./nise_spatial_index_reader -i cntry_idx -z -n 1 < sample_new.txt | cat  > sample_new_out.txt

spatialIdx_US

./nise_spatial_index_builder -i zipbd_uk.txt -o spatialIdx_UK


./nise_spatial_index_builder -i mmb.txt -o mmb_idx

./nise_spatial_index_reader -i brdidfence_idx -z -n 1 < testll.txt | cat  > sample_new_out.txt