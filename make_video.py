import ffmpeg

(
    ffmpeg
    .input('/Users/rchales/hydrosos/figures/*.png', pattern_type='glob', framerate=3)
    .output('monthly_mean_basin_flow.mp4')
    .run()
)