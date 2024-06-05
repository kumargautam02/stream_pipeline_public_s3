
import matplotlib.pyplot as plt
import matplotlib.pyplot as pyplt
import numpy as np
from scipy.interpolate import make_interp_spline
import datetime
from scipy.ndimage import gaussian_filter1d

def generatring_line_graph_for_top_5_publishers(df,destination_path):
    try:
        """
        This function is used to generate the line_graph and save it graph folder.
        Parameters:
        destination_path[String]: Path of graph folder to save the output line-graph.
        df[Pyspark-dataframe]: Dataframe.
        returns: None
        """
        pyplt.rcParams["figure.figsize"] = (50,15)
        plt.rcParams["figure.autolayout"] = True
        plt.set_loglevel('WARNING')

        df = df.toPandas()
        x_axis = sorted(df['file_creation_date'].drop_duplicates().to_list())
        print(x_axis)
        # fig, ax = plt.subplots(figsize=(5, 2.7), layout='constrained')
        color_schema = ['r','y','g','c','k']
        # unique_publisher_id = sorted(list(set(df.select("publisher_id").rdd.flatMap(lambda x: x).collect())))
        
        unique_publisher_id = sorted(df['publisher_id'].drop_duplicates().to_list())
        for i in range(len(unique_publisher_id)):
            print(unique_publisher_id[i])

            x_axis = np.array(sorted(df[df['publisher_id'] == unique_publisher_id[i]]['file_creation_date'].drop_duplicates().to_list()))

            y_axis = np.array(df[df['publisher_id'] == unique_publisher_id[i]]['sum(total_clicks)'].to_list())//1000

            y_smooth = gaussian_filter1d(y_axis, sigma=1)

            # Plot smooth curve
            plt.plot(x_axis, y_smooth,  f'x-{color_schema[i]}',label=unique_publisher_id[i], linewidth=4)


        plt.xlabel('Date', size = 50, labelpad=38)
        plt.ylabel('Clicks (x 1000)', size = 50, labelpad= 38)
        plt.title('QPS', size = 50, pad = 6)
        plt.xticks(fontsize=42,rotation=45,ha='right')
        plt.yticks(fontsize=42)
        specific_y_ticks = np.arange(0, 1200, 100)

        plt.gca().set_yticks(specific_y_ticks)
        plt.grid(visible = True,axis='y', which='both',color='k', linestyle='-', linewidth=0.6, in_layout=True)
        plt.grid(visible = True,axis='x', which='both',color='k', linestyle='-', linewidth=0.6, in_layout=True)
        plt.legend(prop={'size':50})
        os.makedirs("graph",exist_ok=True)
        plt.savefig(f'{destination_path}/graph/line_graph.png', dpi=300, bbox_inches='tight')
        plt.show()
    except Exception as e:
        logger.info(f"Error has been encountered at generatring_line_graph_for_top_5_publishers {e}")
