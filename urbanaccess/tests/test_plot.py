import pandas
import pytest
import os
import pandas as pd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

from urbanaccess import plot
from urbanaccess.config import settings


# setup decorator to skip test if run on continuous integration (CI)
# environment to avoid plot tests hanging when showing plots
ci_test = os.environ.get('CI') == 'true'
skipifci = pytest.mark.skipif(ci_test, reason='skip on CI')


@pytest.fixture
def show_plot():
    # setup 'show' param depending on if tests are run in CI
    # environment to avoid plot tests hanging when showing plots
    if ci_test:
        # if CI do not show plots
        show_plot = False
        print('Tests with "show=True" are set to "show=False"'.format(
            show_plot))
    else:
        # if local environment show plots
        show_plot = True
    return show_plot


@pytest.fixture
def transit_nodes_invalid_xy():
    data = {
        'id': ['1_Lake_Stop',
               '2_12th_Stop',
               '3_19th_Stop'],
        'x': [-122.26571, -122.26571, -122.26571],
        'y': [37.79739, 37.79739, 37.79739],
        'unique_agency_id': ['rail_agency'] * 3,
        'route_type': [1] * 3,
        'stop_name': ['Lake Stop', '12th Street Stop',
                      '19th Street Stop'],
        'stop_code': ['E', 'H', 'H'],
        'net_type': ['transit'] * 3,
        'nearest_osm_node': [1, 6, 7]
    }
    index = range(3)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def transit_nodes():
    data = {
        'id': ['1_Lake_Stop',
               '2_12th_Stop',
               '3_19th_Stop'],
        'x': [-122.26571, -122.27085, -122.26812],
        'y': [37.79739, 37.80364, 37.80866],
        'unique_agency_id': ['rail_agency'] * 3,
        'route_type': [1] * 3,
        'stop_name': ['Lake Stop', '12th Street Stop',
                      '19th Street Stop'],
        'stop_code': ['E', 'H', 'H'],
        'net_type': ['transit'] * 3,
        'nearest_osm_node': [1, 6, 7]
    }
    index = range(3)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def transit_edges():
    # multi-trip edges
    data = {
        'from': ['1_Lake_Stop', '2_12th_Stop',
                 '1_Lake_Stop', '2_12th_Stop',
                 '1_Lake_Stop', '2_12th_Stop',
                 '1_Lake_Stop', '2_12th_Stop',
                 '3_12th_Stop', '2_19th_Stop',
                 '3_12th_Stop', '2_19th_Stop',
                 '3_12th_Stop', '2_19th_Stop',
                 '3_12th_Stop', '2_19th_Stop',
                 '1_Lake_Stop', '1_Lake_Stop',
                 '1_Lake_Stop', '1_Lake_Stop'],
        'to': ['2_12th_Stop', '3_19th_Stop',
               '2_12th_Stop', '3_19th_Stop',
               '2_12th_Stop', '3_19th_Stop',
               '2_12th_Stop', '3_19th_Stop',
               '2_12th_Stop', '1_Lake_Stop',
               '2_12th_Stop', '1_Lake_Stop',
               '2_12th_Stop', '1_Lake_Stop',
               '2_12th_Stop', '1_Lake_Stop',
               '3_19th_Stop', '3_19th_Stop',
               '3_19th_Stop', '3_19th_Stop'],
        'id': ['100_rail_agency_1', '100_rail_agency_2',
               '100_rail_agency_1', '100_rail_agency_2',
               '100_rail_agency_1', '100_rail_agency_2',
               '100_rail_agency_1', '100_rail_agency_2',
               '200_rail_agency_1', '200_rail_agency_2',
               '200_rail_agency_1', '200_rail_agency_2',
               '200_rail_agency_1', '200_rail_agency_2',
               '200_rail_agency_1', '200_rail_agency_2',
               '300_rail_agency_1', '300_rail_agency_1',
               '300_rail_agency_1', '300_rail_agency_1'],
        'weight': [15, 15, 15, 15, 15, 15, 15, 15,
                   15, 15, 15, 15, 15, 15, 15, 15,
                   5, 5, 5, 5],
        'unique_agency_id': ['rail_agency'] * 20,
        'unique_trip_id': ['100_rail_agency', '100_rail_agency',
                           '100_rail_agency', '100_rail_agency',
                           '100_rail_agency', '100_rail_agency',
                           '100_rail_agency', '100_rail_agency',
                           '200_rail_agency', '200_rail_agency',
                           '200_rail_agency', '200_rail_agency',
                           '200_rail_agency', '200_rail_agency',
                           '200_rail_agency', '200_rail_agency',
                           '300_rail_agency', '300_rail_agency',
                           '300_rail_agency', '300_rail_agency'],
        'sequence': [1, 2, 1, 2, 1, 2, 1, 2,
                     1, 2, 1, 2, 1, 2, 1, 2,
                     1, 1, 1, 1],
        'unique_route_id': ['A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'B_rail_agency', 'B_rail_agency',
                            'B_rail_agency', 'B_rail_agency'],
        'net_type': ['transit'] * 20,
    }

    index = range(20)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def drive_nodes():
    data = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9],
        'x': [
            -122.265474, -122.272543, -122.273680, -122.262834, -122.269889,
            -122.271170, -122.268333, -122.266974, -122.264433],
        'y': [
            37.796897, 37.799683, 37.800206, 37.800964, 37.803884,
            37.804270, 37.809158, 37.808645, 37.807921],
        # name is not expected in OSM nodes but is used here as placeholder
        # for custom columns and as a reference for tests
        'name': [
            '1 8th & Oak', '2 8th & Franklin', '3 8th & Broadway',
            '4 14th & Oak', '5 14th & Franklin', '6 14th & Broadway',
            '7 Berkley & Broadway', '8 Berkley & Franklin',
            '9 Berkley & Harrison'],
        'net_type': ['drive'] * 9}
    index = range(9)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def drive_edges():
    data = {
        'from': [1, 2, 3, 6, 7, 8, 9, 4, 4, 5, 2, 5],
        'to': [2, 3, 6, 7, 8, 9, 4, 1, 5, 6, 5, 8],
        'name': ['8th', '8th', 'Broadway', 'Broadway', 'Berkley', 'Berkley',
                 'Lakeside', 'Oak', '14th', '14th', 'Franklin', 'Franklin'],
        'highway': ['residential', 'residential', 'primary', 'primary',
                    'primary', 'primary', 'residential', 'residential',
                    'primary', 'primary', 'residential', 'residential'],
        'weight': [0.3, 0.3, 0.5, 0.5, 0.6, 0.6, 1, 0.8, 0.8, 0.8, 0.4, 0.4],
        'oneway': ['yes', 'yes', 'no', 'no', 'no', 'no', 'yes', 'yes', 'no',
                   'no', 'yes', 'yes'],
        'net_type': ['drive'] * 12,
    }
    index = range(12)
    df = pd.DataFrame(data, index)

    twoway_df = df.loc[df['oneway'] == 'no']
    twoway_df.rename(columns={'from': 'to', 'to': 'from'}, inplace=True)

    edge_df = pd.concat([df, twoway_df], axis=0, ignore_index=True)
    return edge_df


@pytest.fixture
def connector_edges():
    data = {
        'from': ['1_Lake_Stop', 1, '2_12th_Stop', 6, '3_19th_Stop', 7],
        'to': [1, '1_Lake_Stop', 6, '2_12th_Stop', 7, '3_19th_Stop'],
        'weight': [0.1, 0.1, 0.2, 0.2, 0.15, 0.15],
        'net_type': ['transit to osm', 'osm to transit',
                     'transit to osm', 'osm to transit',
                     'transit to osm', 'osm to transit']}
    index = range(6)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def small_net(transit_edges, drive_edges, connector_edges,
              transit_nodes, drive_nodes):
    edge_df = pd.concat(
        [transit_edges, drive_edges, connector_edges], axis=0)
    node_df = pd.concat(
        [transit_nodes, drive_nodes], axis=0, ignore_index=True)
    node_df.set_index('id', inplace=True, drop=False)
    return edge_df, node_df


def test_plot_w_col_names_case_1(small_net, show_plot):
    edge_df, node_df = small_net
    # edge: has from and to col names specified via params
    # node: has expected default x and y col names
    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=show_plot, close=False, save=False, filepath=None, dpi=300,
        ax=None)


def test_plot_w_col_names_case_2(small_net, show_plot):
    edge_df, node_df = small_net
    node_df.rename(columns={'x': 'long', 'y': 'lat'},
                   inplace=True)
    # edge: has from and to col names specified via params
    # node: has x and y col names specified via params
    plot.plot_net(
        node_df, edge_df,
        x_col='long', y_col='lat',
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=show_plot, close=False, save=False, filepath=None, dpi=300,
        ax=None)


def test_plot_w_col_names_case_3(small_net, show_plot):
    edge_df, node_df = small_net
    edge_df.rename(columns={'from': 'from_int', 'to': 'to_int'},
                   inplace=True)
    # edge: has expected default from_int and to_int col names
    # node: has expected default x and y col names but specified from params
    plot.plot_net(
        node_df, edge_df,
        x_col='x', y_col='y',
        from_col=None, to_col=None,
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=show_plot, close=False, save=False, filepath=None, dpi=300,
        ax=None)


def test_plot_w_col_names_case_4(small_net, show_plot):
    edge_df, node_df = small_net
    edge_df.rename(columns={'from': 'node_id_from', 'to': 'node_id_to'},
                   inplace=True)
    # edge: has expected default node_id_from and node_id_to col names
    # node: has expected default x and y col names
    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col=None, to_col=None,
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=show_plot, close=False, save=False, filepath=None, dpi=300,
        ax=None)


def test_plot_w_bbox_param(small_net, show_plot):
    edge_df, node_df = small_net
    # use bbox param to create a zoomed in plot
    bbox = (-122.274201, 37.800592, -122.259953, 37.809815)
    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=bbox,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=show_plot, close=False, save=False, filepath=None, dpi=300,
        ax=None)


def test_plot_nodes_only(small_net, show_plot):
    edge_df, node_df = small_net
    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=show_plot, close=False, save=False, filepath=None, dpi=300,
        ax=None)


def test_plot_pass_ax(small_net, show_plot):
    edge_df, node_df = small_net
    # test adding ax var to plot: project plot coords

    # This is the standard map projection for California
    teale_albers = ccrs.AlbersEqualArea(
        false_northing=-4000000.0, false_easting=0,
        central_longitude=-120.0, central_latitude=0,
        standard_parallels=(34.0, 40.5))
    teale_albers_ax = plt.axes(projection=teale_albers)

    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=show_plot, close=False, save=False, filepath=None, dpi=300,
        ax=teale_albers_ax)


def test_plot_append_ax(small_net, show_plot):
    edge_df, node_df = small_net

    fig, ax = plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=show_plot, close=False, save=False, filepath=None, dpi=300,
        ax=None)
    # test adding additional ax var to plot: annotate node names
    col = 'id'
    for i, values in node_df.iterrows():
        txt = '{}'.format(values[col])
        ax.annotate(text=txt, xy=(values['x'], values['y']), xytext=(10, -5),
                    textcoords='offset points', size=10, rotation=0,
                    color='red')
    fig.show()


@skipifci  # dont run on CI to avoid stalling Travis when plots are shown
def test_plot_show_close(small_net):
    edge_df, node_df = small_net

    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=True, close=True, save=False, filepath=None, dpi=300,
        ax=None)


def test_plot_save_w_path_and_filename(small_net, tmpdir, show_plot):
    edge_df, node_df = small_net
    save_path = os.path.join(tmpdir.strpath, 'test_save_plot')
    os.makedirs(save_path)
    print('writing test data to dir: {}'.format(save_path))

    file_name = os.path.join(save_path, 'test_plot.png')

    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=False, close=False, save=True, filepath=file_name, dpi=300,
        ax=None)
    # check that file was created
    assert os.path.isfile(file_name)
    # read and show image
    img = mpimg.imread(file_name)
    # if test is not run in CI env show plot
    if show_plot:
        imgplot = plt.imshow(img)
        plt.show()
    # clean up test data
    os.remove(file_name)


def test_plot_save_w_default_name(small_net, show_plot):
    edge_df, node_df = small_net

    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=False, close=False, save=True, filepath=None, dpi=300,
        ax=None)
    # check that file was created
    file_name = os.path.join(
        settings.images_folder, '{}.png'.format(settings.image_filename))
    print('wrote test data to file: {}'.format(file_name))
    assert os.path.isfile(file_name)
    # read and show image
    img = mpimg.imread(file_name)
    # if test is not run in CI env show plot
    if show_plot:
        imgplot = plt.imshow(img)
        plt.show()
    # clean up test data
    os.remove(file_name)


def test_plot_save_w_filename_only(small_net, show_plot):
    edge_df, node_df = small_net
    filename = 'test_image.png'
    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=False,
        show=False, close=False, save=True, filepath=filename, dpi=300,
        ax=None)
    # check that file was created
    file_name = os.path.join(settings.images_folder, filename)
    print('wrote test data to file: {}'.format(file_name))
    assert os.path.isfile(file_name)
    # read and show image
    img = mpimg.imread(file_name)
    # if test is not run in CI env show plot
    if show_plot:
        imgplot = plt.imshow(img)
        plt.show()
    # clean up test data
    os.remove(file_name)


def test_plot_invalid_params(small_net, transit_nodes_invalid_xy, show_plot):
    edge_df, node_df = small_net
    # test bbox with incorrect coords
    with pytest.raises(ValueError) as excinfo:
        plot.plot_net(
            transit_nodes_invalid_xy, edge_df,
            x_col=None, y_col=None,
            from_col='from', to_col='to',
            bbox=None,
            fig_height=6, margin=0.0,
            edge_color='#999999', edge_linewidth=1, edge_alpha=1,
            node_color='black', node_size=15, node_alpha=1,
            node_edgecolor='none', node_zorder=3, nodes_only=True,
            show=show_plot, close=False, save=False, filepath=None, dpi=300,
            ax=None)
    expected_error = ('Difference between min and max x and '
                      'or y resulted in a negative value or 0.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        plot.plot_net(
            node_df, edge_df,
            x_col=None, y_col=None,
            from_col='from', to_col='to',
            bbox=None,
            fig_height=6, margin=0.0,
            edge_color='#999999', edge_linewidth=1, edge_alpha=1,
            node_color='black', node_size=15, node_alpha=1,
            node_edgecolor='none', node_zorder=3, nodes_only=True,
            show=False, close=False, save=True, filepath=4, dpi=300,
            ax=None)
    expected_error = 'Filepath must be a string.'
    assert expected_error in str(excinfo.value)


def test_plot_print_warn(small_net, capsys):
    edge_df, node_df = small_net
    # create and save plot using default save dir and name
    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=True,
        show=False, close=False, save=True, filepath=None, dpi=300,
        ax=None)
    # create and save the same plot again and test for overwrite warning
    plot.plot_net(
        node_df, edge_df,
        x_col=None, y_col=None,
        from_col='from', to_col='to',
        bbox=None,
        fig_height=6, margin=0.0,
        edge_color='#999999', edge_linewidth=1, edge_alpha=1,
        node_color='black', node_size=15, node_alpha=1,
        node_edgecolor='none', node_zorder=3, nodes_only=True,
        show=False, close=False, save=True, filepath=None, dpi=300,
        ax=None)
    # check that expected print prints
    captured = capsys.readouterr()
    assert 'Warning: Existing file' in captured.out
    default_file = os.path.join(
        settings.images_folder, '{}.png'.format(settings.image_filename))
    # clean up test data
    os.remove(default_file)


def test_col_colors_case_1(small_net, capsys):
    edge_df, node_df = small_net

    colors = plot.col_colors(edge_df, col='weight',
                             num_bins=5, cmap='Spectral', start=0.1, stop=0.9)
    assert isinstance(colors, list)
    assert len(colors) == len(edge_df)  # expect 44 where each edge has a color
    assert isinstance(colors[0], tuple)
    assert colors[0] == (
        0.19946174548250672, 0.5289504036908881, 0.7391003460207612, 1.0)
    assert len(set(colors)) == 2  # 2 bins expected
    # check that expected print prints
    captured = capsys.readouterr()
    assert 'Too many bins requested' in captured.out


def test_col_colors_case_2(small_net):
    edge_df, node_df = small_net

    colors = plot.col_colors(edge_df, col='weight',
                             num_bins=1, cmap='Spectral', start=0.1, stop=0.9)
    assert isinstance(colors, list)
    assert len(colors) == len(edge_df)  # expect 44 where each edge has a color
    assert isinstance(colors[0], tuple)
    assert colors[0] == (
        0.8310649750096117, 0.23844675124951936, 0.30880430603613995, 1.0)
    assert len(set(colors)) == 1  # 1 bin expected


def test_recursive_category_gen(small_net):
    edge_df, node_df = small_net
    expected_cats = pd.Series(
        data=[23, 21], index=[0, 1],
        name='weight')
    num_bins = 2
    col_series = edge_df[edge_df['weight'].notnull()]['weight']
    bins_used, categories = plot._recursive_category_gen(
        col_series, num_bins)

    assert isinstance(bins_used, int)
    assert isinstance(categories, pandas.Series)
    assert bins_used == num_bins
    # expect 44 where each edge has a color
    assert len(categories) == len(edge_df)
    assert categories.dtype == pd.api.types.CategoricalDtype(
        categories=None, ordered=True)
    assert all(categories.value_counts() == expected_cats)


def test_recursive_category_gen_invalid(small_net):
    edge_df, node_df = small_net
    col_series = edge_df[edge_df['weight'].notnull()]['weight']
    with pytest.raises(ValueError) as excinfo:
        bins_used, categories = plot._recursive_category_gen(
            col_series, num_bins=0)
    expected_error = 'Unable to perform qcut to 0 bins.'
    assert expected_error in str(excinfo.value)


def test_prep_edges(small_net):
    edge_df, node_df = small_net
    edges_wlines = plot._prep_edges(edge_df, node_df,
                                    from_col='from', to_col='to',
                                    x_col='x', y_col='y')

    expected_cols = ['to_lon', 'to_lat', 'from_lon', 'from_lat']
    test_df = edges_wlines.drop(expected_cols, axis=1)
    # test a sample of records
    assert test_df.iloc[:1].equals(edge_df.iloc[:1])
    stop_1_x = node_df['x'].loc[node_df['id'] == '1_Lake_Stop'].values[0]
    stop_1_y = node_df['y'].loc[node_df['id'] == '1_Lake_Stop'].values[0]
    stop_2_x = node_df['x'].loc[node_df['id'] == '2_12th_Stop'].values[0]
    stop_2_y = node_df['y'].loc[node_df['id'] == '2_12th_Stop'].values[0]
    assert edges_wlines.iloc[:1]['from_lon'].values[0] == stop_1_x
    assert edges_wlines.iloc[:1]['from_lat'].values[0] == stop_1_y
    assert edges_wlines.iloc[:1]['to_lon'].values[0] == stop_2_x
    assert edges_wlines.iloc[:1]['to_lat'].values[0] == stop_2_y
    for col in expected_cols:
        assert col in edges_wlines.columns
        assert edges_wlines[col].isnull().sum() == 0
    assert len(edges_wlines) == 36


def test_prep_edges_invalid(small_net):
    edge_df, node_df = small_net
    with pytest.raises(ValueError) as excinfo:
        edges_wlines = plot._prep_edges(edge_df, node_df,
                                        from_col='from_int', to_col='to_int',
                                        x_col='x', y_col='y')
    expected_error = ('from_int or to_int columns were not found in edge '
                      'table columns.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        edges_wlines = plot._prep_edges(edge_df, node_df,
                                        from_col='from', to_col='to',
                                        x_col='long', y_col='lat')
    expected_error = ('long or lat columns were not found in node table '
                      'columns.')
    assert expected_error in str(excinfo.value)
