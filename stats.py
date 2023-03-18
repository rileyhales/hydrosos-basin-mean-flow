import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('./tables/central_america_monthly_percentiles.parquet').transpose()
df

# make a histogram of the percentiles and save the figure
fig, ax = plt.subplots(figsize=(10, 6))
ax.set_xlabel('Percentile')
ax.set_ylabel('Count')
ax.set_title('Distribution of Percentiles')
ax.hist(df.values.flatten(), bins=20)
fig.savefig('./central_america_percentile_distribution.png')
plt.close(fig)
