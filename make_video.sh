#!/bin/bash

#ffmpeg -framerate 4 -pattern_type glob -i './figures/DR_map_*.png' -c:v libx264 -pix_fmt yuv420p -y output.mp4
ffmpeg -framerate 3 -pattern_type glob -i './figures/DR_map_quarterly*.png' -c:v libx264 -pix_fmt yuv420p -y output_2.mp4
