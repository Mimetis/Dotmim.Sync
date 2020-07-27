using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Dotmim.Sync.Tests.Misc
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class TestPriorityAttribute : Attribute
    {
        public TestPriorityAttribute(int priority)
        {
            Priority = priority;
        }

        public int Priority { get; private set; }
    }

    public class PriorityOrderer : ITestCaseOrderer
    {
        public IEnumerable<TTestCase> OrderTestCases<TTestCase>(IEnumerable<TTestCase> testCases) where TTestCase : ITestCase
        {
            var sortedMethods = new SortedDictionary<int, List<TTestCase>>();

            foreach (var testCase in testCases)
            {
                int priority = 0;

                // get the attribute priority
                foreach (var attr in testCase.TestMethod.Method.GetCustomAttributes((typeof(TestPriorityAttribute).AssemblyQualifiedName)))
                    priority = attr.GetNamedArgument<int>("Priority");

                // get the all the tests marked with this priority
                // we could potentially have multiple tests with same priority
                sortedMethods.TryGetValue(priority, out var lstTestsForPriority);

                // if new priority with no test for this priority, just add it to my sorted list
                if (lstTestsForPriority == null)
                {
                    lstTestsForPriority = new List<TTestCase>();
                    sortedMethods.Add(priority, lstTestsForPriority);
                }

                // add the test case to the list, already part of the sortedMethods list
                lstTestsForPriority.Add(testCase);
            }

            foreach (var list in sortedMethods.Keys.Select(priority => sortedMethods[priority]))
            {
                // potentially we could have multiple tests with same priority
                // yield ordererd by name so
                list.Sort((x, y) => StringComparer.OrdinalIgnoreCase.Compare(x.TestMethod.Method.Name, y.TestMethod.Method.Name));
                foreach (var testCase in list)
                    yield return testCase;
            }
        }

    }
}
