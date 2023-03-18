#!/bin/bash

ffmpeg -framerate 4 -pattern_type glob -i './figures/DR_map_*.png' -c:v libx264 -pix_fmt yuv420p -y output.mp4
