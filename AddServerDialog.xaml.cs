using System.Windows;

namespace BulkQuery
{
    /// <summary>
    /// Interaction logic for AddServerDialog.xaml
    /// </summary>
    public partial class AddServerDialog : Window
    {
        public AddServerDialog()
        {
            InitializeComponent();
        }

        private void BtnAddServer_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
        }

        public string ServerConnectionString => TextBoxConnectionString.Text.Trim();

        public string ServerDisplayName => TextBoxDisplayName.Text.Trim();
    }
}
