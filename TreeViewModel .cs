using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;

namespace BulkQuery
{
    internal class TreeViewModel<T> : INotifyPropertyChanged
    {
        public string Name { get; private set; }
        public List<TreeViewModel<T>> Children { get; }
        public T Value { get; private set; }

        private TreeViewModel<T> parentNode;
        private bool? isChecked = false;

        public bool? IsChecked
        {
            get { return isChecked; }
            set { SetIsChecked(value, true, true); }
        }

        public TreeViewModel(string name, T value)
        {
            Name = name;
            Children = new List<TreeViewModel<T>>();
            Value = value;
        }

        public void InitParent(TreeViewModel<T> parent)
        {
            parentNode = parent;
            SyncParentState();
        }
        
        private void SetIsChecked(bool? value, bool updateChildren, bool updateParent)
        {
            if (value == isChecked)
                return;

            isChecked = value;

            if (updateChildren && isChecked.HasValue)
                Children.ForEach(c => c.SetIsChecked(isChecked, updateChildren:true, updateParent:false));
            
            NotifyPropertyChanged("IsChecked");

            if(updateParent && parentNode != null)
                SyncParentState();
        }

        private void SyncParentState()
        {
            var parentState = parentNode.Children.All(c => c.IsChecked == true) ? true
                        : parentNode.Children.All(c => c.IsChecked == false) ? false
                        : (bool?)null;
            parentNode.SetIsChecked(parentState, updateChildren: false, updateParent:true);
        }

        private void NotifyPropertyChanged(string info)
        {
            if (PropertyChanged != null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(info));
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;
    }
}
