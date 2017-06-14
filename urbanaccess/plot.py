import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib import collections as mc
import numpy as np
import time

from urbanaccess.utils import log


def plot_net(nodes, edges, x_col=None, y_col=None, from_col=None,
             to_col=None, bbox=None,
             fig_height=6, margin=0.02,
             edge_color='#999999', edge_linewidth=1, edge_alpha=1,
             node_color='black', node_size=15, node_alpha=1,
             node_edgecolor='none', node_zorder=3, nodes_only=False):
    """
    plot urbanaccess network nodes and edges

    Parameters
    ----------
    nodes : pandas.DataFrame
    edges : pandas.DataFrame
    x_col : str, optional
        x coordinate column in nodes dataframe
    y_col : str, optional
        y coordinate column in nodes dataframe
    from_col : str, optional
        name of column to use for 'from' node id
    to_col : str, optional
        name of column to use for 'to' node id
    bbox : tuple, optional
        Bounding box formatted as a 4 element tuple:
        (lng_max, lat_min, lng_min, lat_max)
        example: (-122.304611,37.798933,-122.263412,37.822802)
        a bbox can be extracted for an area using: the CSV format
        bbox from http://boundingbox.klokantech.com/
        if None bbox will be calculated from spatial extents of data
    fig_height : int
        matplotlib figure height in inches
    margin : float
        margin around the figure
    edge_color : string
        color of the edge lines
    edge_linewidth : float
        width of the edge lines
    edge_alpha : float
        opacity of the edge lines
    node_color : string
        node color
    node_size : int
        node size
    node_alpha : float
        node opacity
    node_edgecolor : string
        the color of the node border
    node_zorder : int
        zorder to plot nodes, edges are zorder 2. A node_zorder 1 will plot
        nodes under the edges, 3 will plot nodes on top
    nodes_only : bool
        if true only the nodes will plot

    Returns
    -------
    fig, ax
    """

    start_time = time.time()

    # set default x and y cols if none specified
    if 'x' in nodes.columns or 'y' in nodes.columns or x_col is \
            None or y_col is None:
        x_col = 'x'
        y_col = 'y'

    # set default from_col and to_col cols if none specified
    if ('from_int' in edges.columns or 'to_int' in edges.columns) and \
            (from_col is None or to_col is None):
        from_col = 'from_int'
        to_col = 'to_int'

    # set default from_col and to_col cols if none specified
    if ('node_id_from' in edges.columns or 'node_id_to' in edges.columns) and \
            (from_col is None or to_col is None):
        from_col = 'node_id_from'
        to_col = 'node_id_to'

    # if edge df is subset make sure nodes are also subset to match
    from_ids = nodes[nodes.index.isin(list(edges[from_col]))]
    to_id = nodes[nodes.index.isin(list(edges[to_col]))]
    node_ids = pd.concat([from_ids, to_id], ignore_index=False)
    nodes = nodes[nodes.index.isin(list(node_ids.index))]

    node_Xs = nodes[x_col].tolist()
    node_Ys = nodes[y_col].tolist()

    if nodes_only is False:
        if 'from_lon' not in edges.columns \
                or 'to_lon' not in edges.columns \
                or 'from_lat' not in edges.columns \
                or 'from_lon' not in edges.columns:

            edges = _prep_edges(edges=edges, nodes=nodes,
                                from_col=from_col, to_col=to_col,
                                x_col=x_col, y_col=y_col)

    if bbox is None:
        y_max = max(node_Ys)
        y_min = min(node_Ys)
        x_max = max(node_Xs)
        x_min = min(node_Xs)
    else:
        x_min, y_min, x_max, y_max = bbox

    if y_max - y_min <= 0 or x_max - x_min <= 0:
        raise ValueError(('difference between min and max x and or y resulted '
                          'in a negative value or 0'))
    bbox_aspect_ratio = (y_max - y_min) / (x_max - x_min)

    fig, ax = plt.subplots(figsize=(fig_height / bbox_aspect_ratio,
                                    fig_height))

    if nodes_only is False:
        # TODO: optimize for speed by calculating only for edges that are
        # within the the bbox + buffer distance to speed up
        lines = []
        for index, node in edges.iterrows():
            x1, y1, x2, y2 = node['from_lon'], node['from_lat'], \
                             node['to_lon'], node['to_lat']
            line = [(x1, y1), (x2, y2)]
            lines.append(line)

        lc = mc.LineCollection(lines, colors=edge_color,
                               linewidths=edge_linewidth,
                               alpha=edge_alpha, zorder=2)
        ax.add_collection(lc)

    ax.scatter(node_Xs, node_Ys, s=node_size, c=node_color,
               alpha=node_alpha, edgecolor=node_edgecolor,
               zorder=node_zorder)

    # set fig extent
    margin_ns = (y_min - y_max) * margin
    margin_ew = (x_min - x_max) * margin
    ax.set_ylim((y_min - margin_ns, y_max + margin_ns))
    ax.set_xlim((x_min - margin_ew, x_max + margin_ew))

    # configure axis
    ax.get_xaxis().get_major_formatter().set_useOffset(False)
    ax.get_yaxis().get_major_formatter().set_useOffset(False)
    ax.axis('off')

    log('Figure created. Took {:,.2f} seconds'.format(
        time.time() - start_time))

    plt.show()

    return fig, ax


def col_colors(df, col, num_bins=5, cmap='spectral',
               start=0.1, stop=0.9):
    """
    Get a list of colors by binning a continuous variable column
    into quantiles

    Parameters
    ----------
    df : pandas.DataFrame
    col : string
        the name of the column in the dataframe with the continuous variable
    num_bins : int
        how many quantiles
    cmap : string
        name of a colormap
    start : float
        where to start in the colorspace
    stop : float
        where to end in the colorspace

    Returns
    -------
    colors : list
    """
    col = df[df[col].notnull()][col]
    bins_used, categories = _recursive_category_gen(col, num_bins)

    if not bins_used == num_bins:
        log('Too many bins requested, using max bins possible. '
            'To avoid duplicate edges, ' + str(bins_used) + ' bins used.')

    color_list = [cm.get_cmap(cmap)(x) for x in np.linspace(start,
                                                            stop,
                                                            bins_used)]
    cleaned_categories = [int(cat) for cat in categories]
    colors = [color_list[cat] for cat in cleaned_categories]
    return colors


def _recursive_category_gen(col, num_bins):
    """
    Generate number of bins recursively

    Parameters
    ----------
    col : string
        the name of the column in the dataframe with the continuous variable
    num_bins : int
        how many quantiles

    Returns
    -------
    num_bins : int
    categories : list
    """

    bin_labels = range(num_bins)

    # base case catch
    if num_bins == 0:
        raise ValueError('Unable to perform qcut to 0 bins.')

    # we assume the num_bins count will work
    try:
        categories = pd.qcut(x=col, q=num_bins, labels=bin_labels)
        return num_bins, categories

    # if it does not, then we need to go down 1 number of bins
    except ValueError:
        new_bin_count = num_bins - 1
        return _recursive_category_gen(col, new_bin_count)


def _prep_edges(edges, nodes, from_col, to_col,
                x_col, y_col):
    """
    Prepare edges to display edges as lines on the plot

    Parameters
    ----------
    nodes : pandas.DataFrame
    edges : pandas.DataFrame
    from_col : string
        name of column to use for 'from' node id
    to_col : string
        name of column to use for 'to' node id
    x_col : string
        name of column to use for 'x' node coordinates
    y_col : string
        name of column to use for 'y' node coordinates

    Returns
    -------
    edges_wline : pandas.DataFrame
        the edge dataframe with from and to x y coordinates and
        ids to build lines
    """

    if x_col not in nodes.columns or y_col not in nodes.columns:
        raise ValueError(
            '{} or {} columns were not found in nodes columns'.format(x_col,
                                                                      y_col))

    if from_col not in edges.columns or to_col not in edges.columns:
        raise ValueError(
            '{} or {} columns were not found in edges columns'.format(from_col,
                                                                      to_col))

    edges_wline = edges.merge(nodes[[x_col, y_col]], left_on=from_col,
                              right_index=True)
    edges_wline.rename(columns={x_col: 'from_lon', y_col: 'from_lat'},
                       inplace=True)
    edges_wline = edges_wline.merge(nodes[[x_col, y_col]], left_on=to_col,
                                    right_index=True)
    edges_wline.rename(columns={x_col: 'to_lon', y_col: 'to_lat'},
                       inplace=True)

    return edges_wline
