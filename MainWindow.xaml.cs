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

        public MainWindow()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            if (Settings.Default.NeedsUpgrade)
            {
                Settings.Default.Upgrade();
                Settings.Default.NeedsUpgrade = false;
                Settings.Default.Save();
            }

            if (Settings.Default.ServersList?.Servers == null)
            {
                Settings.Default.ServersList = new ServersList();
                Settings.Default.Save();
            }

            var servers = GetSavedServers();
            var serverTasks = new List<Task>();
            foreach (var server in servers)
            {
                var task = new Task(() => AddNodesForServer(server));
                serverTasks.Add(task);
                task.Start();
            }
            Task.WaitAll(serverTasks.ToArray());
            databaseTreeModel.Sort(new Comparison<TreeViewModel<DatabaseTreeNode>>((i,j) => i.Value.ServerDefinition.DisplayName.CompareTo(j.Value.ServerDefinition.DisplayName)));

            DatabasesTreeView.ItemsSource = databaseTreeModel;
        }

        private List<ServerDefinition> GetSavedServers()
        {
            return Settings.Default.ServersList?.Servers ?? new List<ServerDefinition>();
        }

        private void SaveServers(List<ServerDefinition> servers)
        {
            Settings.Default.ServersList.Servers = servers;
            Settings.Default.Save();
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

                foreach (var db in databases)
                {
                    if (Settings.Default.HideSystemDatabases && systemDatabases.Contains(db.DatabaseName))
                    {
                        continue;
                    }

                    var dbNode = new DatabaseTreeNode
                    {
                        IsServerNode = false,
                        DatabaseDefinition = db
                    };
                    var dbNodeViewModel = new TreeViewModel<DatabaseTreeNode>(db.DatabaseName, dbNode);
                    serverNodeViewModel.Children.Add(dbNodeViewModel);
                }
            }
            catch(Exception ex)
            {
                var serverNodeViewModel = new TreeViewModel<DatabaseTreeNode>(server.DisplayName + " (Connection Failed)", serverNode);
                databaseTreeModel.Add(serverNodeViewModel);
            }

            DatabasesTreeView.Items.Refresh();
        }

        private void MenuItem_AddServer_OnClick(object sender, RoutedEventArgs e)
        {
            AddServerDialog dialog = new AddServerDialog();
            if (dialog.ShowDialog() == true)
            {
                var servers = GetSavedServers();
                if (servers.Any(s => s.DisplayName == dialog.ServerDisplayName))
                {
                    MessageBox.Show("A server already exists with this name.\nChoose a different name and try again.", "", MessageBoxButton.OK, MessageBoxImage.Exclamation);
                    return;
                }

                var server = new ServerDefinition(dialog.ServerDisplayName, dialog.ServerConnectionString);
                AddNodesForServer(server);

                servers.Add(server);
                SaveServers(servers);
            }
        }

        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            Settings.Default.Save();
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
            var servers = GetSavedServers();
            servers.Remove(server);
            SaveServers(servers);
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

        private void RunQuery()
        {
            var query = QueryTextBox.Text;
            var databases = GetSelectedDatabases().ToList();
            var timer = Stopwatch.StartNew();
            var result = QueryRunner.BulkQuery(databases, query).Result;
            Debug.WriteLine("Total query time: " + timer.ElapsedMilliseconds);
            if (result.Messages.Count > 0)
            {
                TextBoxMessages.Text = string.Join(Environment.NewLine, result.Messages);
                TextBoxMessages.Visibility = Visibility.Visible;
            }
            else
            {
                TextBoxMessages.Text = string.Empty;
                TextBoxMessages.Visibility = Visibility.Collapsed;
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
    }

    public class DatabaseTreeNode
    {
        public bool IsServerNode { get; set; }
        public ServerDefinition ServerDefinition { get; set; }
        public DatabaseDefinition DatabaseDefinition { get; set; }
    }
}
