using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using System.Xml;
using BulkQuery.Properties;

namespace BulkQuery
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private readonly List<TreeViewModel<DatabaseTreeNode>> databaseTreeModel = new List<TreeViewModel<DatabaseTreeNode>>();
        private readonly string[] systemDatabases = {"master", "model", "msdb", "tempdb"};
        private readonly UserSettingsManager<BulkQueryUserSettings> settingsManager;

        public MainWindow()
        {
            InitializeComponent();
            settingsManager = new UserSettingsManager<BulkQueryUserSettings>("BulkQuery.settings.json");
            Settings = settingsManager.LoadSettings() ?? new BulkQueryUserSettings
            {
                Servers = new List<ServerDefinition>(),
                HideSystemDatabases = true,
                SqlTimeout = 60,
            };
            Settings.Servers = Settings.Servers ?? new List<ServerDefinition>();
        }

        public BulkQueryUserSettings Settings { get; private set; }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            var serverTasks = new List<Task>();
            Settings.Servers.AsParallel().ForAll(AddNodesForServer);
            databaseTreeModel.Sort(new Comparison<TreeViewModel<DatabaseTreeNode>>((i,j) => i.Value.ServerDefinition.DisplayName.CompareTo(j.Value.ServerDefinition.DisplayName)));

            DatabasesTreeView.ItemsSource = databaseTreeModel;
            DatabasesTreeView.KeyDown += (o, ev) =>
            {
                if (ev.Key == Key.Space)
                {
                    if (o is TreeView treeView && treeView.SelectedItem is TreeViewModel<DatabaseTreeNode> selectedItem)
                    {
                        selectedItem.IsChecked ^= true;
                        ev.Handled = true;
                    }
                }
            };
            DatabasesTreeView.Focus();
            InputManager.Current.ProcessInput(new KeyEventArgs(Keyboard.PrimaryDevice, PresentationSource.FromVisual(DatabasesTreeView), 0, Key.Down) { RoutedEvent = Keyboard.KeyDownEvent });
        }

        private void SaveSettings()
        {
            foreach (var node in databaseTreeModel)
            {
                node.Value.ServerDefinition.SelectedDatabases = node.Children
                    .Where(cn => cn.IsChecked == true)
                    .Select(cn => cn.Value.DatabaseDefinition.DatabaseName)
                    .ToList();
            }
            Settings = Settings ?? new BulkQueryUserSettings();
            settingsManager.SaveSettings(Settings);
        }

        private void AddNodesForServer(ServerDefinition server)
        {
            var serverNode = new DatabaseTreeNode
            {
                ServerDefinition = server,
                IsServerNode = true
            };

            try
            {
                var databases = QueryRunner.GetDatabasesForServer(server);

                var serverNodeViewModel = new TreeViewModel<DatabaseTreeNode>(server.DisplayName, serverNode);
                databaseTreeModel.Add(serverNodeViewModel);

                serverNodeViewModel.IsChecked = true;

                foreach (var db in databases.OrderBy(db => db.DatabaseName))
                {
                    if (Settings.HideSystemDatabases && systemDatabases.Contains(db.DatabaseName))
                    {
                        continue;
                    }

                    var dbNode = new DatabaseTreeNode
                    {
                        IsServerNode = false,
                        DatabaseDefinition = db,
                    };
                    var dbNodeViewModel = new TreeViewModel<DatabaseTreeNode>(db.DatabaseName, dbNode);
                    dbNodeViewModel.IsChecked = server.SelectedDatabases.Contains(db.DatabaseName);
                    serverNodeViewModel.Children.Add(dbNodeViewModel);
                    dbNodeViewModel.InitParent(serverNodeViewModel);
                }
            }
            catch(Exception ex)
            {
                var serverNodeViewModel = new TreeViewModel<DatabaseTreeNode>(server.DisplayName + " (Connection Failed)", serverNode);
                databaseTreeModel.Add(serverNodeViewModel);
            }
        }

        private void MenuItem_AddServer_OnClick(object sender, RoutedEventArgs e)
        {
            AddServerDialog dialog = new AddServerDialog();
            if (dialog.ShowDialog() == true)
            {
                var servers = Settings.Servers;
                if (servers.Any(s => s.DisplayName == dialog.ServerDisplayName))
                {
                    MessageBox.Show("A server already exists with this name.\nChoose a different name and try again.", "", MessageBoxButton.OK, MessageBoxImage.Exclamation);
                    return;
                }

                var server = new ServerDefinition(dialog.ServerDisplayName, dialog.ServerConnectionString);
                AddNodesForServer(server);
                DatabasesTreeView.Items.Refresh();

                servers.Add(server);
                SaveSettings();
            }
        }

        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            SaveSettings();
        }

        private static DependencyObject GetDependencyObjectFromVisualTree(DependencyObject startObject, Type type)
        {
            var parent = startObject;
            while (parent != null)
            {
                if (type.IsInstanceOfType(parent))
                    break;
                parent = VisualTreeHelper.GetParent(parent);
            }
            return parent;
        }

        private void DatabasesTreeView_PreviewMouseButton(object sender, MouseButtonEventArgs e)
        {
            DependencyObject obj = e.OriginalSource as DependencyObject;
            TreeViewItem item = GetDependencyObjectFromVisualTree(obj, typeof(TreeViewItem)) as TreeViewItem;
            var selectedElement = item.Header as TreeViewModel<DatabaseTreeNode>;

            ContextMenu menu = new ContextMenu();
            if (selectedElement.Value.IsServerNode)
            {
                var menuItem = new MenuItem();
                menuItem.Header = "Refresh Databases";
                menuItem.Click += (o, args) =>
                {
                    RemoveServer(selectedElement.Value.ServerDefinition);
                    AddNodesForServer(selectedElement.Value.ServerDefinition);
                    DatabasesTreeView.Items.Refresh();
                };
                menu.Items.Add(menuItem);

                menuItem = new MenuItem();
                menuItem.Header = "Remove Server";
                menuItem.Click += (o, args) =>
                {
                    RemoveServer(selectedElement.Value.ServerDefinition);
                };
                menu.Items.Add(menuItem);
            }
                
            (sender as TreeViewItem).ContextMenu = menu;
        }

        private void RemoveServer(ServerDefinition server)
        {
            var servers = Settings.Servers;
            servers.Remove(server);
            SaveSettings();
            var nodeToRemove = databaseTreeModel.FirstOrDefault(s => s.Value.ServerDefinition == server);
            databaseTreeModel.Remove(nodeToRemove);
            DatabasesTreeView.Items.Refresh();
        }

        private IEnumerable<DatabaseDefinition> GetSelectedDatabases()
        {
            return
                databaseTreeModel
                    .SelectMany(serverNode => serverNode.Children.Where(dbNode => dbNode.IsChecked ?? true))
                    .Select(treeNode => treeNode.Value.DatabaseDefinition);
        }
        
        private void ButtonQuery_OnClick(object sender, RoutedEventArgs e)
        {
            RunQuery();
        }

        private async Task RunQuery()
        {
            var query = QueryTextBox.Text;
            var databases = GetSelectedDatabases().ToList();
            var timer = Stopwatch.StartNew();
            RunQueryButton.IsEnabled = false;
            var result = await QueryRunner.BulkQuery(databases, query, Settings.SqlTimeout);
            RunQueryButton.IsEnabled = true;
            Debug.WriteLine("Total query time: " + timer.ElapsedMilliseconds);
            if (result.Messages.Count > 0)
            {
                TextBoxMessages.Text = string.Join(Environment.NewLine, result.Messages);
                if (TextBoxMessagesRow.ActualHeight == 0)
                    TextBoxMessagesRow.Height = new GridLength(60);
            }
            else
            {
                TextBoxMessages.Text = string.Empty;
                TextBoxMessagesRow.Height = new GridLength(0);
            }

            if (result.ResultTable != null)
                ResultDataGrid.ItemsSource = result.ResultTable.DefaultView;
        }

        private void MainGrid_OnDragDelta(object sender, DragDeltaEventArgs e)
        {
            MainGrid.ColumnDefinitions[0].Width = new GridLength(MainGrid.ColumnDefinitions[0].ActualWidth + e.HorizontalChange);
        }

        private void RightHandGrid_OnDragDelta(object sender, DragDeltaEventArgs e)
        {
            RightHandGrid.RowDefinitions[0].Height = new GridLength(RightHandGrid.RowDefinitions[0].ActualHeight + e.VerticalChange);
        }

        private void MenuItem_Github_Click(object sender, RoutedEventArgs e)
        {
            System.Diagnostics.Process.Start("https://github.com/tloten/bulk-query");
        }

        private void Window_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.F5)
            {
                RunQuery();
            }
        }
        private void ValidateTextboxEntryIsInteger(object sender, TextCompositionEventArgs e)
        {
            if (!int.TryParse(e.Text, out var _))
                e.Handled = true;
        }
    }

    public class DatabaseTreeNode
    {
        public bool IsServerNode { get; set; }
        public ServerDefinition ServerDefinition { get; set; }
        public DatabaseDefinition DatabaseDefinition { get; set; }
    }
}
