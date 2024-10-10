using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;


namespace StoreDataManager;

public class BTreeIndex : Index
        {
            private const int T = 2; // Minimum degree of B-Tree
            private BTreeNode root;

            private class BTreeNode
            {
            public int[] keys;
            public int[] values;
            public BTreeNode[] children;
            public int n;
            public bool leaf;

            public BTreeNode(bool leaf)
            {
                this.leaf = leaf;
                keys = new int[2 * T - 1];
                values = new int[2 * T - 1];
                children = new BTreeNode[2 * T];
                n = 0;
            }
            }

            public BTreeIndex()
            {
            root = new BTreeNode(true);
            }

            public override void Insert(string key, int value)
            {
            int keyHash = key.GetHashCode();
            if (root.n == 2 * T - 1)
            {
                BTreeNode s = new BTreeNode(false);
                s.children[0] = root;
                SplitChild(s, 0, root);
                int i = 0;
                if (s.keys[0] < keyHash)
                i++;
                InsertNonFull(s.children[i], keyHash, value);
                root = s;
            }
            else
                InsertNonFull(root, keyHash, value);
            }

            private void InsertNonFull(BTreeNode x, int k, int v)
            {
            int i = x.n - 1;
            if (x.leaf)
            {
                while (i >= 0 && x.keys[i] > k)
                {
                x.keys[i + 1] = x.keys[i];
                x.values[i + 1] = x.values[i];
                i--;
                }
                x.keys[i + 1] = k;
                x.values[i + 1] = v;
                x.n = x.n + 1;
            }
            else
            {
                while (i >= 0 && x.keys[i] > k)
                i--;
                i++;
                if (x.children[i].n == 2 * T - 1)
                {
                SplitChild(x, i, x.children[i]);
                if (x.keys[i] < k)
                    i++;
                }
                InsertNonFull(x.children[i], k, v);
            }
            }

            private void SplitChild(BTreeNode x, int i, BTreeNode y)
            {
            BTreeNode z = new BTreeNode(y.leaf);
            z.n = T - 1;
            for (int j = 0; j < T - 1; j++)
            {
                z.keys[j] = y.keys[j + T];
                z.values[j] = y.values[j + T];
            }
            if (!y.leaf)
            {
                for (int j = 0; j < T; j++)
                z.children[j] = y.children[j + T];
            }
            y.n = T - 1;
            for (int j = x.n; j >= i + 1; j--)
                x.children[j + 1] = x.children[j];
            x.children[i + 1] = z;
            for (int j = x.n - 1; j >= i; j--)
            {
                x.keys[j + 1] = x.keys[j];
                x.values[j + 1] = x.values[j];
            }
            x.keys[i] = y.keys[T - 1];
            x.values[i] = y.values[T - 1];
            x.n = x.n + 1;
            }

            public override int Search(string key)
            {
            return Search(root, key.GetHashCode());
            }

            private int Search(BTreeNode x, int k)
            {
            int i = 0;
            while (i < x.n && k > x.keys[i])
                i++;
            if (i < x.n && k == x.keys[i])
                return x.values[i];
            if (x.leaf)
                return -1;
            return Search(x.children[i], k);
            }

            public override IEnumerable<int> SearchLessThan(string key)
            {
            return SearchLessThanRec(root, key.GetHashCode());
            }

            private IEnumerable<int> SearchLessThanRec(BTreeNode x, int k)
            {
            List<int> result = new List<int>();
            for (int i = 0; i < x.n; i++)
            {
                if (x.keys[i] >= k)
                break;
                result.Add(x.values[i]);
                if (!x.leaf)
                result.AddRange(SearchLessThanRec(x.children[i], k));
            }
            if (!x.leaf && x.keys[x.n - 1] < k)
                result.AddRange(SearchLessThanRec(x.children[x.n], k));
            return result;
            }

            public override IEnumerable<int> SearchGreaterThan(string key)
            {
            return SearchGreaterThanRec(root, key.GetHashCode());
            }

            private IEnumerable<int> SearchGreaterThanRec(BTreeNode x, int k)
            {
            List<int> result = new List<int>();
            int i;
            for (i = 0; i < x.n; i++)
            {
                if (x.keys[i] > k)
                {
                result.Add(x.values[i]);
                if (!x.leaf)
                    result.AddRange(SearchGreaterThanRec(x.children[i], k));
                }
                else if (!x.leaf)
                result.AddRange(SearchGreaterThanRec(x.children[i], k));
            }
            if (!x.leaf)
                result.AddRange(SearchGreaterThanRec(x.children[i], k));
            return result;
            }

            public override IEnumerable<int> SearchLessThanOrEqual(string key)
            {
            return SearchLessThanOrEqualRec(root, key.GetHashCode());
            }

            private IEnumerable<int> SearchLessThanOrEqualRec(BTreeNode x, int k)
            {
            List<int> result = new List<int>();
            for (int i = 0; i < x.n; i++)
            {
                if (x.keys[i] > k)
                break;
                result.Add(x.values[i]);
                if (!x.leaf)
                result.AddRange(SearchLessThanOrEqualRec(x.children[i], k));
            }
            if (!x.leaf && x.keys[x.n - 1] <= k)
                result.AddRange(SearchLessThanOrEqualRec(x.children[x.n], k));
            return result;
            }

            public override IEnumerable<int> SearchGreaterThanOrEqual(string key)
            {
            return SearchGreaterThanOrEqualRec(root, key.GetHashCode());
            }

            private IEnumerable<int> SearchGreaterThanOrEqualRec(BTreeNode x, int k)
            {
            List<int> result = new List<int>();
            int i;
            for (i = 0; i < x.n; i++)
            {
                if (x.keys[i] >= k)
                {
                result.Add(x.values[i]);
                if (!x.leaf)
                    result.AddRange(SearchGreaterThanOrEqualRec(x.children[i], k));
                }
                else if (!x.leaf)
                result.AddRange(SearchGreaterThanOrEqualRec(x.children[i], k));
            }
            if (!x.leaf)
                result.AddRange(SearchGreaterThanOrEqualRec(x.children[i], k));
            return result;
            }

        public override IEnumerable<int> SearchLike(string pattern)
        {
            return SearchLikeRec(root, pattern);
        }

        private IEnumerable<int> SearchLikeRec(BTreeNode x, string pattern)
        {
            List<int> result = new List<int>();
            for (int i = 0; i < x.n; i++)
            {
                if (IsLikeMatch(x.keys[i].ToString(), pattern))
                {
                    result.Add(x.values[i]);
                }
                if (!x.leaf)
                {
                    result.AddRange(SearchLikeRec(x.children[i], pattern));
                }
            }
            if (!x.leaf)
            {
                result.AddRange(SearchLikeRec(x.children[x.n], pattern));
            }
            return result;
        }

        public override IEnumerable<int> SearchNotLike(string pattern)
        {
            return SearchNotLikeRec(root, pattern);
        }

        private IEnumerable<int> SearchNotLikeRec(BTreeNode x, string pattern)
        {
            List<int> result = new List<int>();
            for (int i = 0; i < x.n; i++)
            {
                if (!IsLikeMatch(x.keys[i].ToString(), pattern))
                {
                    result.Add(x.values[i]);
                }
                if (!x.leaf)
                {
                    result.AddRange(SearchNotLikeRec(x.children[i], pattern));
                }
            }
            if (!x.leaf)
            {
                result.AddRange(SearchNotLikeRec(x.children[x.n], pattern));
            }
            return result;
        }

        private bool IsLikeMatch(string value, string pattern)
        {
            string regexPattern = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            return Regex.IsMatch(value, regexPattern, RegexOptions.IgnoreCase);
        }

        }