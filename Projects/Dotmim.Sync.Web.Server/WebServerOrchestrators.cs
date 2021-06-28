using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Web.Server
{
    public class WebServerOrchestrators : ICollection<WebServerOrchestrator>, IList<WebServerOrchestrator>
    {
        /// <summary>
        /// Gets or Sets the InnerCollection (Exposed as Public for serialization purpose)
        /// </summary>
        public Collection<WebServerOrchestrator> InnerCollection { get; set; } = new Collection<WebServerOrchestrator>();

        /// <summary>
        /// Create a default collection for Serializers
        /// </summary>
        public WebServerOrchestrators(IMemoryCache cache)
        {
        }

        /// <summary>
        /// Add a new sync parameter 
        /// </summary>
        //public void Add<T>(string name, T value) => this.Add(new WebServerOrchestrator(name, value));


        /// <summary>
        /// Add a new sync parameter 
        /// </summary>
        public void Add(WebServerOrchestrator orchestrator)
        {
            if (this.Any(p => p.ScopeName.Equals(orchestrator.ScopeName, SyncGlobalization.DataSourceStringComparison)))
                throw new ArgumentException($"Orchestrator with scope name {orchestrator.ScopeName} already exists in the service collection");

            InnerCollection.Add(orchestrator);
        }

        /// <summary>
        /// Add an array of parameters
        /// </summary>
        public void AddRange(IEnumerable<WebServerOrchestrator> orchestrators)
        {
            foreach (var p in orchestrators)
                Add(p);
        }

        /// <summary>
        /// Get a parameters by its name
        /// </summary>
        public WebServerOrchestrator this[string scopeName]
        {
            get
            {
                if (string.IsNullOrEmpty(scopeName))
                    throw new ArgumentNullException("scopeName");

                return InnerCollection.FirstOrDefault(p => string.Equals(p.ScopeName, scopeName, SyncGlobalization.DataSourceStringComparison));
            }
        }


        /// <summary>
        /// Clear
        /// </summary>
        public void Clear()
        {
            InnerCollection.Clear();
        }

        public WebServerOrchestrator this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        WebServerOrchestrator IList<WebServerOrchestrator>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, WebServerOrchestrator item) => InnerCollection.Insert(index, item);
        public bool Remove(WebServerOrchestrator item) => InnerCollection.Remove(item);
        public bool Remove(string name) => InnerCollection.Remove(this[name]);
        public bool Contains(WebServerOrchestrator item) => InnerCollection.Contains(item);
        public bool Contains(string name) => this[name] != null;
        public void CopyTo(WebServerOrchestrator[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(WebServerOrchestrator item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        public IEnumerator<WebServerOrchestrator> GetEnumerator() => InnerCollection.GetEnumerator();
        public override string ToString() => this.InnerCollection.Count.ToString();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
    }
}
