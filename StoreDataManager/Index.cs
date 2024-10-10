namespace StoreDataManager;

public abstract class Index
        {
            public abstract void Insert(string key, int value);
            public abstract int Search(string key);
            public abstract IEnumerable<int> SearchLessThan(string key);
            public abstract IEnumerable<int> SearchGreaterThan(string key);
            public abstract IEnumerable<int> SearchLessThanOrEqual(string key);
            public abstract IEnumerable<int> SearchGreaterThanOrEqual(string key);
            public virtual IEnumerable<int> SearchLike(string key) => throw new NotImplementedException();
            public virtual IEnumerable<int> SearchNotLike(string key) => throw new NotImplementedException();
        }   