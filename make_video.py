import ffmpeg

(
    ffmpeg
    .input('/Users/rchales/hydrosos/figures/*.png',
           pattern_type='glob',
           framerate=3,
           vcodec='libx264',
           pix_fmt="yuvj420p",
           # vf="scale=640:-2,format=yuv420p",
           # y="",
           )
    .output('monthly_mean_basin_flow.mp4')
    .run()
)
# ffmpeg -pattern_type glob -i './figures/*.png' -r 1 -pix_fmt yuv420p -vcodec libx264 -vf scale=640:-2,format=yuv420p -y monthly_mean_basin_flow.mp4
