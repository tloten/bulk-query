using System.Collections.Generic;
using System.ComponentModel;

namespace BulkQuery
{
    internal class TreeViewModel<T> : INotifyPropertyChanged
    {
        public string Name { get; private set; }
        public List<TreeViewModel<T>> Children { get; }
        public T Value { get; private set; }

        private bool? isChecked = false;

        public bool? IsChecked
        {
            get { return isChecked; }
            set { SetIsChecked(value, true); }
        }

        public TreeViewModel(string name, T value)
        {
            Name = name;
            Children = new List<TreeViewModel<T>>();
            Value = value;
        }
        
        private void SetIsChecked(bool? value, bool updateChildren)
        {
            if (value == isChecked)
                return;

            isChecked = value;

            if (updateChildren && isChecked.HasValue)
                Children.ForEach(c => c.SetIsChecked(isChecked, true));
            
            NotifyPropertyChanged("IsChecked");
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
