using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace StoreDataManager;

public class BSTIndex : Index
        {
            private BSTNode root;
            private Store store;

            private class BSTNode
            {
            public string key;
            public int value;
            public BSTNode left, right;

            public BSTNode(string key, int value)
            {
                this.key = key;
                this.value = value;
                left = right = null;
            }
            }

            public override void Insert(string key, int value)
            {
            root = InsertRec(root, key, value);
            }

            private BSTNode InsertRec(BSTNode root, string key, int value)
            {
            if (root == null)
            {
                root = new BSTNode(key, value);
                return root;
            }

            if (string.Compare(key, root.key) < 0)
                root.left = InsertRec(root.left, key, value);
            else if (string.Compare(key, root.key) > 0)
                root.right = InsertRec(root.right, key, value);

            return root;
            }

            public override int Search(string key)
            {
            return SearchRec(root, key);
            }

            private int SearchRec(BSTNode root, string key)
            {
            if (root == null || root.key == key)
                return (root != null) ? root.value : -1;

            if (string.Compare(key, root.key) < 0)
                return SearchRec(root.left, key);

            return SearchRec(root.right, key);
            }

            public override IEnumerable<int> SearchLessThan(string key)
            {
            List<int> result = new List<int>();
            SearchLessThanRec(root, key, result);
            return result;
            }

            private void SearchLessThanRec(BSTNode node, string key, List<int> result)
            {
            if (node == null)
                return;

            if (string.Compare(key, node.key) <= 0)
                SearchLessThanRec(node.left, key, result);
            else
            {
                SearchLessThanRec(node.left, key, result);
                result.Add(node.value);
                SearchLessThanRec(node.right, key, result);
            }
            }

            public override IEnumerable<int> SearchGreaterThan(string key)
            {
            List<int> result = new List<int>();
            SearchGreaterThanRec(root, key, result);
            return result;
            }

            private void SearchGreaterThanRec(BSTNode node, string key, List<int> result)
            {
            if (node == null)
                return;

            if (string.Compare(key, node.key) >= 0)
                SearchGreaterThanRec(node.right, key, result);
            else
            {
                SearchGreaterThanRec(node.left, key, result);
                result.Add(node.value);
                SearchGreaterThanRec(node.right, key, result);
            }
            }

            public override IEnumerable<int> SearchLessThanOrEqual(string key)
            {
            List<int> result = new List<int>();
            SearchLessThanOrEqualRec(root, key, result);
            return result;
            }

            private void SearchLessThanOrEqualRec(BSTNode node, string key, List<int> result)
            {
            if (node == null)
                return;

            if (string.Compare(key, node.key) < 0)
                SearchLessThanOrEqualRec(node.left, key, result);
            else
            {
                SearchLessThanOrEqualRec(node.left, key, result);
                result.Add(node.value);
                SearchLessThanOrEqualRec(node.right, key, result);
            }
            }

            public override IEnumerable<int> SearchGreaterThanOrEqual(string key)
            {
            List<int> result = new List<int>();
            SearchGreaterThanOrEqualRec(root, key, result);
            return result;
            }

            private void SearchGreaterThanOrEqualRec(BSTNode node, string key, List<int> result)
            {
            if (node == null)
                return;

            if (string.Compare(key, node.key) > 0)
                SearchGreaterThanOrEqualRec(node.right, key, result);
            else
            {
                SearchGreaterThanOrEqualRec(node.left, key, result);
                result.Add(node.value);
                SearchGreaterThanOrEqualRec(node.right, key, result);
            }
            }

        public override IEnumerable<int> SearchLike(string pattern)
        {
            List<int> result = new List<int>();
            SearchLikeRec(root, pattern, result);
            return result;
        }

        private void SearchLikeRec(BSTNode node, string pattern, List<int> result)
        {
            if (node == null)
                return;

            string regexPattern = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            if (Regex.IsMatch(node.key.ToString(), regexPattern, RegexOptions.IgnoreCase))
            {
                result.Add(node.value);
            }

            SearchLikeRec(node.left, pattern, result);
            SearchLikeRec(node.right, pattern, result);
        }

        public override IEnumerable<int> SearchNotLike(string pattern)
        {
            List<int> result = new List<int>();
            SearchNotLikeRec(root, pattern, result);
            return result;
        }

        private void SearchNotLikeRec(BSTNode node, string pattern, List<int> result)
        {
            if (node == null)
                return;

            string regexPattern = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            if (!Regex.IsMatch(node.key.ToString(), regexPattern, RegexOptions.IgnoreCase))
            {
                result.Add(node.value);
            }

            SearchNotLikeRec(node.left, pattern, result);
            SearchNotLikeRec(node.right, pattern, result);
        }
    }