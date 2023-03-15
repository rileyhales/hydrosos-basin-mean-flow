#!/bin/bash

#ffmpeg -pattern_type glob -i './figures/DR_map_*.png' -r 1 -pix_fmt yuv420p -vcodec libx264 -vf scale=640:-2,format=yuv420p -y monthly_mean_basin_flow.mp4
#ffmpeg -pattern_type glob -i './figures/DR_map_*.png' -r 1 -pix_fmt yuv420p -vcodec libx264 -y monthly_mean_basin_flow.mp4

#ffmpeg -framerate 25 -i image_%03d.png -c:v libx264 -pix_fmt yuv420p output.mp4
#ffmpeg -framerate 4 -i ./figures/DR_map_%06d.png -c:v libx264 -pix_fmt yuv420p -y output.mp4
#ffmpeg -framerate 4 -pattern_type glob -i './figures/DR_map_%06d.png' -c:v libx264 -pix_fmt yuv420p -y output.mp4
ffmpeg -framerate 4 -pattern_type glob -i './figures/DR_map_*.png' -c:v libx264 -pix_fmt yuv420p -y output.mp4
