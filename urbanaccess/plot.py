import pandas as pd
import os
import logging as lg
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib import collections as mc
import numpy as np
import time

from urbanaccess.utils import log
from urbanaccess import config


def plot_net(nodes, edges, x_col=None, y_col=None, from_col=None,
             to_col=None, bbox=None,
             fig_height=6, margin=0.02,
             edge_color='#999999', edge_linewidth=1, edge_alpha=1,
             node_color='black', node_size=15, node_alpha=1,
             node_edgecolor='none', node_zorder=3, nodes_only=False,
             show=True, close=False, save=False, filepath=None, dpi=300,
             ax=None):
    """
    plot urbanaccess network nodes and edges

    Parameters
    ----------
    nodes : pandas.DataFrame
    edges : pandas.DataFrame
    x_col : str, optional
        x coordinate column in nodes DataFrame
    y_col : str, optional
        y coordinate column in nodes DataFrame
    from_col : str, optional
        name of column to use for 'from' node ID
    to_col : str, optional
        name of column to use for 'to' node ID
    bbox : tuple, optional
        Bounding box specifying the spatial extent of data to plot. This does
        not subset the nodes and edges, it only creates a view to plot.
        Specify a bounding box formatted as a 4 element tuple:
        (lng_max, lat_min, lng_min, lat_max) comprised of floats.
        Example: (-122.304611,37.798933,-122.263412,37.822802).
        A bbox can be extracted for an area using: the CSV format
        bbox from http://boundingbox.klokantech.com/,
        if None bbox will be calculated from spatial extents of data
    fig_height : int, optional
        matplotlib figure height in inches
    margin : float, optional
        margin around the figure
    edge_color : string, optional
        color of the edge lines
    edge_linewidth : float, optional
        width of the edge lines
    edge_alpha : float, optional
        opacity of the edge lines
    node_color : string, optional
        node color
    node_size : int, optional
        node size
    node_alpha : float, optional
        node opacity
    node_edgecolor : string, optional
        the color of the node border
    node_zorder : int, optional
        zorder to plot nodes, edges are zorder 2. A node_zorder 1 will plot
        nodes under the edges, 3 will plot nodes on top
    nodes_only : bool, optional
        if true only the nodes will plot
    show : bool, optional
        if true, figure will be shown via pyplot.show()
    close : bool, optional
        if true, figure will be closed via pyplot.close()
    save : bool, optional
        if true, save the figure to disk at location specified in filepath
    filepath : string, optional
        if save is true, the full path to the file with file name and
        extension. If only file name and no path is provided, image will be
        saved with the default config.settings.images_folder directory.
        File format is derived from the file extension.
        If None, will save image in config.settings.images_folder as
        'urbanaccess_plot.png'
    dpi : int, optional
        if save is true, the image resolution of saved file.
        Default is 300 DPI.
    ax :  matplotlib.axes._subplots.AxesSubplot, optional
        matplotlib axes, as given by, for example, plt.subplot.
        Use to specify the projection.

    Returns
    -------
    fig, ax
        pyplot.figure, pyplot.axis
    """

    start_time = time.time()

    edges_cols = edges.columns
    nodes_cols = nodes.columns
    has_xy_names = all(
        col in nodes_cols for col in ['x', 'y'])
    xy_is_none = all(
        col is None for col in [x_col, y_col])
    has_from_to_names_int = all(
        col in edges_cols for col in ['from_int', 'to_int'])
    has_from_to_names_node_id = all(
        col in edges_cols for col in ['node_id_from', 'node_id_to'])
    from_to_is_none = all(
        col is None for col in [from_col, to_col])
    has_lat_lon_names = all(
        col in edges_cols for col in [
            'from_lon', 'to_lon', 'from_lat', 'from_lon'])

    # set default x and y cols if none specified
    if has_xy_names or xy_is_none:
        x_col = 'x'
        y_col = 'y'

    # set default from_col and to_col cols if none specified based on cols
    # that are in edge table
    if from_to_is_none:
        if has_from_to_names_int:
            from_col = 'from_int'
            to_col = 'to_int'
        if has_from_to_names_node_id:
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
        if not has_lat_lon_names:
            edges = _prep_edges(edges=edges, nodes=nodes,
                                from_col=from_col, to_col=to_col,
                                x_col=x_col, y_col=y_col)

    if bbox is None:
        y_max, y_min = max(node_Ys), min(node_Ys)
        x_max, x_min = max(node_Xs), min(node_Xs)
    else:
        x_min, y_min, x_max, y_max = bbox

    if y_max - y_min <= 0 or x_max - x_min <= 0:
        raise ValueError('Difference between min and max x and or y resulted '
                         'in a negative value or 0.')
    bbox_aspect_ratio = (y_max - y_min) / (x_max - x_min)

    if ax is None:
        fig, ax = plt.subplots(
            figsize=(fig_height / bbox_aspect_ratio, fig_height))
    else:
        fig = ax.figure

    if nodes_only is False:
        # TODO: optimize for speed by calculating only for edges that are
        #  within the the bbox + buffer distance to speed up
        lines = []
        for index, node in edges.iterrows():
            x1, y1 = node['from_lon'], node['from_lat']
            x2, y2 = node['to_lon'], node['to_lat']
            line = [(x1, y1), (x2, y2)]
            lines.append(line)

        lc = mc.LineCollection(
            lines, colors=edge_color, linewidths=edge_linewidth,
            alpha=edge_alpha, zorder=2)
        ax.add_collection(lc)

    ax.scatter(
        node_Xs, node_Ys, s=node_size, c=node_color,
        alpha=node_alpha, edgecolor=node_edgecolor, zorder=node_zorder)

    # set fig extent
    margin_ns = (y_min - y_max) * margin
    margin_ew = (x_min - x_max) * margin
    ax.set_ylim((y_min - margin_ns, y_max + margin_ns))
    ax.set_xlim((x_min - margin_ew, x_max + margin_ew))

    # configure axis
    ax.get_xaxis().get_major_formatter().set_useOffset(False)
    ax.get_yaxis().get_major_formatter().set_useOffset(False)
    ax.axis('off')

    if save:
        fig = _save_figure(fig, filepath, dpi)
    if show:
        plt.show()
    if close:
        plt.close()

    log('Figure created. Took {:,.2f} seconds.'.format(
        time.time() - start_time))

    return fig, ax


def _save_figure(fig, filepath, dpi):
    # if filepath not specified then use the default in config and
    # save file using default file name as PNG
    if filepath is None:
        filename = '{}.png'.format(config.settings.image_filename)
        filepath = config.settings.images_folder
    else:
        if not isinstance(filepath, str):
            raise ValueError('Filepath must be a string.')
        filename = os.path.basename(filepath)
        filepath = os.path.dirname(filepath)
        # if only filename was specified in param, default dir to the
        # default images dir
        if filepath == '':
            filepath = config.settings.images_folder
    try:
        if not os.path.exists(filepath):
            os.makedirs(filepath)
    except Exception:
        error_msg = 'Unable to make directory: {}.'
        raise ValueError(error_msg.format(filepath))
    # reconstitute the full filepath
    filepath = os.path.join(filepath, filename)
    # if existing file already exists warn that it will be overwritten
    if os.path.isfile(filepath):
        warn_msg = ('Warning: Existing file {} already exists in '
                    'directory: {} and will be overwritten...')
        warning_msg = (warn_msg.format(filename, filepath))
        log(warning_msg, level=lg.WARNING)

    # get image file extension
    image_ext = os.path.splitext(filepath)[1].replace('.', '')

    fig.set_frameon(True)  # required to save with facecolor
    fig.savefig(filepath, dpi=dpi, bbox_inches='tight',
                format=image_ext, facecolor=fig.get_facecolor(),
                transparent=True)
    fig.set_frameon(False)  # return to original state
    log('Saved {} figure: {}.'.format(image_ext, filepath))
    return fig


def col_colors(df, col, num_bins=5, cmap='Spectral', start=0.1, stop=0.9):
    """
    Get a list of colors by binning a continuous variable column
    into quantiles

    Parameters
    ----------
    df : pandas.DataFrame
        pandas.DataFrame with the continuous variable for which to
        build quantiles from
    col : string
        the name of the column in the DataFrame with the continuous variable
    num_bins : int, optional
        how many quantiles
    cmap : string, optional
        name of a colormap
    start : float, optional
        where to start in the colorspace
    stop : float, optional
        where to end in the colorspace

    Returns
    -------
    colors : list
    """
    col_series = df[df[col].notnull()][col]
    bins_used, categories = _recursive_category_gen(col_series, num_bins)

    if not bins_used == num_bins:
        msg = ('Too many bins requested, using max bins possible. '
               'To avoid duplicate edges, {:,} bins used.')
        log(msg.format(bins_used))

    color_list = [cm.get_cmap(cmap)(x) for x in np.linspace(
        start, stop, bins_used)]
    cleaned_categories = [int(cat) for cat in categories]
    colors = [color_list[cat] for cat in cleaned_categories]
    return colors


def _recursive_category_gen(col, num_bins):
    """
    Generate number of bins recursively

    Parameters
    ----------
    col : pandas.Series
        pandas.Series of the column in the DataFrame with the
        continuous variable without nulls
    num_bins : int
        number of quantiles

    Returns
    -------
    num_bins : int
    categories : list
    """
    bin_labels = range(num_bins)

    # base case catch
    if num_bins == 0:
        raise ValueError('Unable to perform qcut to 0 bins.')

    # assume the num_bins count will work
    try:
        categories = pd.qcut(x=col, q=num_bins, labels=bin_labels)
        return num_bins, categories
    # if it does not, then go down 1 number of bins
    except ValueError:
        new_bin_count = num_bins - 1
        return _recursive_category_gen(col, new_bin_count)


def _prep_edges(edges, nodes, from_col, to_col, x_col, y_col):
    """
    Prepare edges to display edges as lines on the plot

    Parameters
    ----------
    nodes : pandas.DataFrame
    edges : pandas.DataFrame
    from_col : string
        name of column to use for 'from' node ID
    to_col : string
        name of column to use for 'to' node ID
    x_col : string
        name of column to use for 'x' node coordinates
    y_col : string
        name of column to use for 'y' node coordinates

    Returns
    -------
    edges_wline : pandas.DataFrame
        the edge DataFrame with from and to x and y coordinates and
        IDs to build lines
    """
    edges_cols = edges.columns
    nodes_cols = nodes.columns

    error_msg = '{} or {} columns were not found in {} table columns.'
    if x_col not in nodes_cols or y_col not in nodes_cols:
        raise ValueError(error_msg.format(x_col, y_col, 'node'))
    if from_col not in edges_cols or to_col not in edges_cols:
        raise ValueError(error_msg.format(from_col, to_col, 'edge'))

    edges_wline = edges.merge(
        nodes[[x_col, y_col]], left_on=from_col, right_index=True)
    edges_wline.rename(
        columns={x_col: 'from_lon', y_col: 'from_lat'}, inplace=True)
    edges_wline = edges_wline.merge(
        nodes[[x_col, y_col]], left_on=to_col, right_index=True)
    edges_wline.rename(
        columns={x_col: 'to_lon', y_col: 'to_lat'}, inplace=True)
    return edges_wline
